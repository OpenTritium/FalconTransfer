// 一个异步运行时上可以有多个控制器, 但是控制器所控制的 worker 一定是本地的,这样才可以最高效率利用thread local cache
// actor 模式
// 将控制器设计为可以处理多种 任务的模型
use crate::{
    http::{
        command::TaskCommand::{self, *},
        file_range::{FileMultiRange, FileRange},
        meta::HttpTaskMeta,
        qos::QosAdvice,
        status::TaskStatus,
        worker::{
            self, WorkFailed, WorkSuccess, Worker,
            WorkerError::{self, *},
            WorkerFuture, WorkerResult,
        },
    },
    utils::rate_limiter::SharedRateLimiter,
};
use async_broadcast as broadcast;
use compio::{
    fs::File,
    runtime::{JoinHandle, spawn},
};
use compio_watch as watch;
use cyper::Client;
use flume as mpmc;
use futures_util::{StreamExt, select, stream::FuturesUnordered};
use identity::task::TaskId;
use smol_cancellation_token::CancellationToken;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    mem,
    num::NonZeroU8,
    ops::{Deref, Not},
    os::fd::FromRawFd,
    rc::Rc,
};

// todo 一个worker 阻塞太久,尝试注销 worker

pub type JoinResult<T> = Result<T, Box<dyn Any + Send>>;

/// 用于 worker 和 controller 同步
struct TaskState {
    meta: Rc<HttpTaskMeta>,
    committed: FileMultiRange, // 已经持久化了
    inflight: FileMultiRange,  //正在下载中
    max_concurrency: NonZeroU8,
    current_concurrency: u8,
    retries_left: u8,
    file: File,
    cancel: CancellationToken,
    status: watch::Sender<TaskStatus>, /*  worker 可以用于设置对外展示的进度(主要是从视觉角度考虑),
                                        * 控制器用于更新对外暂停状态 */
    rate_limit: SharedRateLimiter,
}

pub struct Controller {
    cmd: mpmc::Receiver<TaskCommand>, // 接受主线程号令
    tasks: HashMap<TaskId, TaskState>,
    workers: FuturesUnordered<WorkerFuture>,
    client: Client,                      // 跨线程共享http客户端
    qos: broadcast::Receiver<QosAdvice>, // qos 邮箱
    pending: HashSet<TaskId>,
}

impl Controller {
    const BLOCK_SIZE: usize = 0x100_0000;
    const DEAFULT_MAX_CONCURRENCY: NonZeroU8 = unsafe { NonZeroU8::new_unchecked(8) };
    const DEFAULT_MAX_RETRIES: u8 = 3;

    pub fn new(client: Client, cmd_rx: mpmc::Receiver<TaskCommand>, qos_rx: broadcast::Receiver<QosAdvice>) -> Self {
        Self {
            cmd: cmd_rx,
            tasks: Default::default(),
            workers: FuturesUnordered::new(),
            client,
            qos: qos_rx,
            pending: Default::default(),
        }
    }

    pub fn run(mut self) -> JoinHandle<()> {
        spawn(async move {
            loop {
                select! {
                    cmd_res =  self.cmd.recv_async() => {
                        self.handle_cmd_res(cmd_res);
                    }
                    worker_res = self.workers.next() => {
                        self.handle_worker_res(worker_res);
                    }
                }
            }
        })
    }

    #[inline]
    fn calculate_block_chunk(remaining: &FileMultiRange, perfer: Option<NonZeroU8>) -> Box<[FileMultiRange]> {
        let concurrency = perfer.unwrap_or(Self::DEAFULT_MAX_CONCURRENCY);
        remaining.clone().window(Self::BLOCK_SIZE).take(concurrency.get().into()).collect()
    }

    #[inline]
    fn check_or_set_finished(task: Option<&TaskState>) -> bool {
        let Some(task) = task else {
            return true;
        };
        let total = task.meta.content_range();
        let is_finished = total.as_ref() == Some(&task.committed);
        if is_finished {
            let _ = task.status.send_if_modified(|status| status.state.set_finished());
        }
        is_finished
    }

    #[inline]
    fn handle_worker_res(&mut self, worker_res: Option<JoinResult<WorkerResult>>) {
        match worker_res {
            Some(Ok(Ok(WorkSuccess { id, downloaded }))) => {
                let mut task = self.tasks.get_mut(&id);
                if let Some(ref mut task) = task {
                    task.committed |= downloaded; // 提交已经持久化的部分
                    task.current_concurrency -= 1; // 减少并发计数器
                }
                if Self::check_or_set_finished(task.as_deref()) {
                    self.tasks.remove(&id);
                    self.pending.remove(&id);
                    return;
                }
                // 如果暂停列表没有它,继续产生工作者
                if !self.pending.contains(&id) {
                    self.spawn_many_worker(id);
                }
            }
            Some(Ok(Err(WorkFailed { id, revert, mut err }))) => {
                let mut task = self.tasks.get_mut(&id);
                // 工作者请求回滚
                if let Some(ref mut task) = task
                    && let Some(revert) = revert
                {
                    task.inflight -= revert; // 取消正在下载的记录
                    if let WorkerError::ParitalDownloaded(ref mut commited) = err {
                        task.committed |= mem::take(commited); // 部分持久化成功
                    }
                }
                if Self::check_or_set_finished(task.as_deref()) {
                    self.tasks.remove(&id);
                    self.pending.remove(&id);
                    return;
                }
                let mut more_worker = false;
                // 先减少并发计数器,然后可尝试次数减法 1
                if let Some(task) = task {
                    task.current_concurrency -= 1;
                    // 假如队列工作者数量大于最大尝试次数会导致溢出
                    let retries_left = task.retries_left.saturating_sub(1);
                    task.retries_left = retries_left;
                    more_worker = retries_left != 0;
                    // 尝试次数耗尽,设置错误状态
                    if !more_worker {
                        let _ = task.status.send_if_modified(|status| status.state.set_failed(err));
                    }
                }
                if !self.pending.contains(&id) && more_worker {
                    self.spawn_many_worker(id);
                }
            }
            Some(Err(err)) => {
                panic!("{:?}", err)
            }
            None => {}
        }
    }

    #[inline]
    fn handle_cmd_res(&mut self, cmd_res: Result<TaskCommand, mpmc::RecvError>) {
        match cmd_res {
            Ok(ChangeRateLimited { id, limit }) => {
                self.tasks.entry(id).and_modify(|task| task.rate_limit.change_limit(limit));
            }
            Ok(ChangeConcurrency { id, concuerrency }) => {
                self.tasks.entry(id).and_modify(|task| task.max_concurrency = concuerrency);
            }
            Ok(Pause(id)) => {
                self.pending.insert(id); // 不要继续创建新工作者了
                self.tasks.entry(id).and_modify(|task| {
                    task.cancel.cancel(); //取消现有工作者
                    let _ = task.status.send_if_modified(|status| status.state.set_pending()); //修改对外状态
                });
            }
            Ok(Resume(id)) => {
                self.pending.remove(&id); //从黑名单中移除
                self.tasks.entry(id).and_modify(|task| {
                    let _ = task.status.send_if_modified(|status| status.state.set_running());
                    task.retries_left = Self::DEFAULT_MAX_RETRIES; // 可能是因为尝试次数耗尽导致停止的,所以恢复一下
                });
                self.spawn_many_worker(id);
            }
            Ok(Cancel(ref id)) => {
                // 从任务列表中移除
                if let Some(task) = self.tasks.remove(id) {
                    task.cancel.cancel(); //记得停止现有的工作者
                }
                self.pending.remove(id); // 清理黑名单,如果有的话
            }
            Ok(Create { meta, watch }) => {
                let id = self.push_task(meta, watch);
                self.spawn_many_worker(id);
            }
            Err(err) => {
                panic!("{:?}", err)
            }
        }
    }

    fn push_task(&mut self, meta: HttpTaskMeta, tx: watch::Sender<TaskStatus>) -> TaskId {
        // todo 文件分配器,这里仅仅用来占位
        let file = unsafe { File::from_raw_fd(0) };
        let task_state = TaskState {
            meta: meta.into(),
            inflight: Default::default(),
            max_concurrency: Self::DEAFULT_MAX_CONCURRENCY,
            current_concurrency: 0,
            retries_left: Self::DEFAULT_MAX_RETRIES,
            file,
            cancel: CancellationToken::new(),
            status: tx,
            rate_limit: SharedRateLimiter::new_with_no_limit(),
            committed: Default::default(),
        };
        let id = TaskId::new();
        self.tasks.insert(id, task_state);
        id
    }

    // todo 实现一个 per id 的创建批量工作者函数, 它会根据 meta (未来有可能是用户建议)来决定并发工作者数量
    fn spawn_many_worker(&mut self, id: TaskId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };
        let available_slots = task.max_concurrency.get().saturating_sub(task.current_concurrency);
        if available_slots == 0 {
            return;
        }
        let meta = task.meta.as_ref();
        let is_single = meta.is_support_ranges().not();
        let perfer = is_single.then_some(unsafe { NonZeroU8::new_unchecked(1) });
        if is_single {
            let fut = Worker::new(
                id,
                meta.url(),
                None,
                self.client.clone(),
                task.file.clone(),
                task.cancel.child_token(),
                task.rate_limit.clone(),
                task.status.clone(),
            )
            .run();
            task.current_concurrency += 1;
            //不需要标记哪些块在下载
            self.workers.push(fut);
            return;
        }
        let mut remaining = meta.content_range().unwrap_or_default();
        remaining -= task.committed.clone();
        remaining -= task.inflight.clone();
        let blks = Self::calculate_block_chunk(&remaining, perfer).into_iter().take(available_slots.into());
        for blk in blks {
            let fut = Worker::new(
                id,
                meta.url(),
                Some(blk.clone()),
                self.client.clone(),
                task.file.clone(),
                task.cancel.child_token(),
                task.rate_limit.clone(),
                task.status.clone(),
            )
            .run();
            task.current_concurrency += 1; // 增加并发计数器
            task.inflight |= blk; // 这个块正在下载
            self.workers.push(fut);
        }
    }
}
