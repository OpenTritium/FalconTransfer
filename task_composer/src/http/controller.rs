use crate::{
    http::{
        command::TaskCommand::{self, *},
        file_range::FileMultiRange,
        meta::HttpTaskMeta,
        qos::QosAdvice,
        status::TaskStatus,
        worker::{
            DownloadError::{self, *},
            Worker, WorkerFuture, WorkerResult,
        },
    },
    utils::rate_limiter::SharedRateLimiter,
};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use reqwest::Client;
use std::{sync::Arc, u16};
use tokio::{
    select, spawn,
    sync::{broadcast, mpsc, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub struct Controller {
    meta: Arc<HttpTaskMeta>,
    client: Client,
    commited: FileMultiRange, /* 用于记录任务分配的工作量，如果任务只完成了部分工作量，则归还。
                               * 这是为了解决分配工作量给任务时，无法及时反馈会导致工作量会被重复分配 */
    concurrency: u16,
    retries_left: u8,
    file: (),                               // 并发写入文件实例
    cmd_rx: mpsc::Receiver<TaskCommand>,    // 命令邮箱
    qos_rx: broadcast::Receiver<QosAdvice>, // qos 邮箱
    parent_cancel: CancellationToken,
    workers: FuturesUnordered<WorkerFuture>, // 工作者队列
    status: watch::Sender<TaskStatus>,       // 任务
    rate_limit: SharedRateLimiter,           // 令牌桶
}

impl Controller {
    const BLOCK_SIZE: usize = 0x100_0000;

    pub fn new(
        meta: Arc<HttpTaskMeta>, client: Client, cmd_rx: mpsc::Receiver<TaskCommand>,
        qos_rx: broadcast::Receiver<QosAdvice>, status: watch::Sender<TaskStatus>,
    ) -> Self {
        const DEAFULT_CONCURRENCY: u16 = 8;
        const DEFAULT_RETRIES: u8 = 3;
        let file = ();
        let rate_limit = SharedRateLimiter::new_with_no_limit();
        let workers = FuturesUnordered::new();
        let parent_cancel = CancellationToken::new();
        let commited = FileMultiRange::default();
        Self {
            meta,
            client,
            commited,
            concurrency: DEAFULT_CONCURRENCY,
            retries_left: DEFAULT_RETRIES,
            file,
            cmd_rx,
            qos_rx,
            parent_cancel,
            workers,
            status,
            rate_limit,
        }
    }

    pub fn run(mut self) -> JoinHandle<()> {
        spawn(async move {
            let join_worker = self
                .workers
                .next()
                .map(|o| o.map(|outter| outter.map_err(|err| (None, DownloadError::JoinError(err))).flatten()));
            select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_cmd(cmd);
                }
                Ok(qos) = self.qos_rx.recv() => {
                    unimplemented!()
                }
                work_result  = join_worker => {
                    self.handle_worker_result(work_result);
                }
            }
        })
    }

    /// 根据总量计算没有提交的部分
    fn calculate_not_commited(&self) -> Option<FileMultiRange> {
        self.meta.content_range().as_ref().map(|total| total - &self.commited)
    }

    #[inline]
    fn handle_cmd(&mut self, cmd: TaskCommand) {
        match cmd {
            ChangeRateLimited(limit) => {
                self.rate_limit.change_limit(limit);
            }
            Pause => {
                self.status.send_if_modified(|status| status.state.set_pending());
            }
            Resume => {
                self.status.send_if_modified(|status| status.state.set_running());
            }
            Cancel => {
                // 设置eof让工作者队列停止继续分配，
                self.status.send_if_modified(|status| status.state.set_aborted());
                // 销毁所有工作者
                // 销毁文件
                // 销毁控制器
            }
            ChangeConcurrency(non_zero) => todo!(),
        }
    }

    /// 在当前并发数的限制下，创建若干工作者，按照块大小分配工作量，此函数返回0 则代表工作量不足以分配新工作者
    fn spwan_many_worker(&mut self) -> usize {
        if self.status.borrow().state.was_stopped() {
            return 0;
        }
        let Some(mut not_commited) = self.calculate_not_commited() else {
            return 0;
        };
        let mut window_iter = not_commited.window(Self::BLOCK_SIZE);
        let more_worker_count = self.concurrency.saturating_sub(self.workers.len().try_into().unwrap_or(u16::MAX));
        let mut acc = 0;
        while let Some(rng) = window_iter.next()
            && more_worker_count > acc
        {
            acc += 1;
            self.spawn_and_push_worker(Some(rng));
        }
        acc as usize
    }

    /// 创建一个线程，用于不知道目标大小或只支持单线程的链接
    /// 这个不需要考虑提交，只需要更新下载内容和全部内容即可
    fn spawn_single_worker(&mut self) {
        if self.status.borrow().state.was_stopped() {
            return;
        }
        self.spawn_and_push_worker(None);
    }

    /// 根据 range 自动创建 worker 并 push 进队列
    fn spawn_and_push_worker(&mut self, range: Option<FileMultiRange>) {
        let worker = Worker::new(
            self.meta.url(),
            range,
            self.client.clone(),
            self.file,
            self.parent_cancel.child_token(),
            self.rate_limit.clone(),
        );
        let fut = worker.run();
        self.workers.push(fut);
    }

    fn handle_worker_result(&mut self, result: Option<WorkerResult>) {
        match result {
            Some(Ok(rng)) => {
                // 更新下载进度
                self.status.send_if_modified(|status| {
                    if rng.is_empty() {
                        return false;
                    }
                    status.downloaded |= rng;
                    true
                });
                // 如果是多线程下载的继续添加更多工作者，没有工作量了就标志任务已经完成
                if self.meta.is_support_ranges() && self.spwan_many_worker() == 0 {
                    self.status.send_modify(|status| {
                        status.state.set_finished();
                    });
                }
                // 如果是单线程下载，那么到这里应该结束了，直接返回
            }
            Some(Err((full, ParitalDownloaded(partial)))) => {
                // 更新已下载的进度并归还没下载的进度
                let not_downloaded = &full.unwrap() - &partial; // 遇到 partial 的时候，full 必定不为空
                self.status.send_if_modified(|status| {
                    status.downloaded |= partial;
                    if not_downloaded.is_empty() {
                        return false;
                    }
                    self.commited -= not_downloaded;
                    true
                });
                // 此时应当还有工作量，直接添加更多工作者
                if self.meta.is_support_ranges() {
                    self.spwan_many_worker();
                }
            }
            // 任务刚初始化还没有工作者时，尝试添加更多工作者
            None if self.meta.is_support_ranges() && self.status.borrow().state.is_idle() => {
                // 比较少见的情况是这个任务的文件是0大小
                if self.spwan_many_worker() == 0 {
                    self.status.send_modify(|status| {
                        status.state.set_finished();
                    });
                }
            }
            // 如果不支持多线程下载，且任务刚创建处于空闲，往工作者队列添加一个工作者
            None if self.status.borrow().state.is_idle() => {
                self.spawn_single_worker();
            }

            // todo 新增远程线程限制异常，用于主动控制并发数量
            // 处理其他错误，归还工作量即可
            Some(Err((Some(full), err))) => {
                self.status.send_modify(|status| {
                    status.state.set_failed(err);
                });
                self.commited -= full;
            }
            // 这里处理非空闲但是工作者队列现在为空的情况，比如暂停，取消，之前的错误
            _ => {}
        }
        return;
    }
}
