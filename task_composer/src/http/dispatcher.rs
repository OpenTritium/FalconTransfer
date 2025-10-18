// 当前设计模式下一个异步运行时上只会运行一个不同类型的调度器
// 考虑一种情况,一个任务运行中被取消,工作者返回的是主动取消异常
// 增加一种观测措施,避免 watch 的状态还没被观测到任务就被释放了
use crate::{
    http::{
        command::TaskCommand::{self, *},
        file_range::FileMultiRange,
        meta::HttpTaskMeta,
        qos::QosAdvice,
        status::TaskStatus,
        worker::{WorkFailed, WorkSuccess, Worker, WorkerError, WorkerFuture, WorkerResult},
    },
    utils::rate_limiter::SharedRateLimiter,
};
use async_broadcast as broadcast;
use compio::{
    fs::File,
    runtime::{JoinHandle, spawn},
};
use compio_watch as watch;
use flume as mpmc;
use futures_util::{StreamExt, select, stream::FuturesUnordered};
use identity::task::TaskId;
use smol_cancellation_token::CancellationToken;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    mem,
    num::NonZeroU8,
    ops::Not,
    os::fd::FromRawFd,
    rc::Rc,
};
use typed_builder::TypedBuilder;

// todo 一个worker 阻塞太久,尝试注销 worker

pub type JoinResult<T> = Result<T, Box<dyn Any + Send>>;

/// 用于 worker 和 dispatcher 同步
#[derive(Debug)]
struct TaskState {
    meta: Rc<HttpTaskMeta>,
    committed: FileMultiRange, // 已经持久化了
    inflight: FileMultiRange,  //正在下载中
    max_concurrency: NonZeroU8,
    current_concurrency: u8,
    retries_left: u8,
    file: File,
    cancel: CancellationToken,
    status: watch::Sender<TaskStatus>, // 用于在模型层时刻展示进度与状态,工作者设置进度,调度器设置任务状态
    rate_limit: SharedRateLimiter,
}

#[derive(Debug, TypedBuilder)]
pub struct Dispatcher {
    cmd: mpmc::Receiver<TaskCommand>,    // 接受主线程号令
    qos: broadcast::Receiver<QosAdvice>, // qos 邮箱
    #[builder(default)]
    tasks: HashMap<TaskId, TaskState>,
    #[builder(default)]
    workers: FuturesUnordered<WorkerFuture>,
    #[builder(default)]
    pending_tasks: HashSet<TaskId>,
}

impl Dispatcher {
    const BLOCK_SIZE: usize = 0x100_0000;
    const DEAFULT_MAX_CONCURRENCY: NonZeroU8 = unsafe { NonZeroU8::new_unchecked(8) };
    const DEFAULT_MAX_RETRIES: u8 = 3;

    pub fn spawn(mut self) -> JoinHandle<()> {
        spawn(async move {
            loop {
                select! {
                    cmd_res = self.cmd.recv_async() => {
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
    fn calculate_block_chunk(
        remaining: &FileMultiRange, perfer: Option<NonZeroU8>,
    ) -> impl Iterator<Item = FileMultiRange> {
        let concurrency = perfer.unwrap_or(Self::DEAFULT_MAX_CONCURRENCY);
        remaining.clone().into_chunks(Self::BLOCK_SIZE).take(concurrency.get().into())
    }

    /// 如果当前任务为空返回已完成
    /// 如果未指定 content_rng 也返回立马完成
    /// 如果已提交的范围等于元数据里的范围则立马设置已完成
    #[inline]
    fn check_or_set_finished(task: Option<&TaskState>) -> bool {
        let Some(task) = task else {
            return true;
        };
        let Some(total) = task.meta.content_range() else {
            return true;
        };
        let is_finished = total == task.committed;
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
                    self.tasks.remove(&id); // 完成了就移除任务,这会导致 watch 也被销毁
                    self.pending_tasks.remove(&id); // 这一步是为了防呆
                    return;
                }
                // 如果暂停列表没有它,继续产生工作者
                if !self.pending_tasks.contains(&id) {
                    self.spawn_many_worker(id);
                }
            }
            Some(Ok(Err(WorkFailed { id, revert, mut err }))) => {
                let mut task = self.tasks.get_mut(&id);
                let mut need_continue = false;
                if let Some(ref mut task) = task {
                    task.current_concurrency -= 1;
                    let retries_left = task.retries_left.saturating_sub(1); // 假如队列工作者数量大于最大尝试次数会导致溢出
                    task.retries_left = retries_left;
                    need_continue = retries_left != 0;
                    // 如果有回滚的必要
                    if let Some(revert) = revert {
                        task.inflight -= revert; // 取消正在下载的记录
                        if let WorkerError::ParitalDownloaded(ref mut commited) = err {
                            task.committed |= mem::take(commited); // 部分持久化成功
                        }
                    }
                    match &err {
                        WorkerError::InitiateCancel => (), //  如果是主动取消就不要设置状态了,停止你的那个人帮你设置了
                        _ => {
                            let _ = task.status.send_if_modified(|status| status.state.set_failed(err));
                        }
                    }
                }
                if Self::check_or_set_finished(task.as_deref()) {
                    self.tasks.remove(&id); // 完成了就移除任务,这会导致 watch 也被销毁
                    self.pending_tasks.remove(&id); // 这一步是为了防呆
                    return;
                }
                if self.pending_tasks.contains(&id).not() && need_continue {
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
                self.pending_tasks.insert(id); // 不要继续创建新工作者了
                self.tasks.entry(id).and_modify(|task| {
                    task.cancel.cancel(); //取消现有工作者
                    let _ = task.status.send_if_modified(|status| status.state.set_pending()); //修改对外状态
                });
            }
            Ok(Resume(id)) => {
                self.pending_tasks.remove(&id); //从黑名单中移除
                self.tasks.entry(id).and_modify(|task| {
                    let _ = task.status.send_if_modified(|status| status.state.set_running());
                    task.retries_left = Self::DEFAULT_MAX_RETRIES; // 可能是因为尝试次数耗尽导致停止的,所以恢复一下
                });
                self.spawn_many_worker(id);
            }
            Ok(Cancel(ref id)) => {
                if let Some(task) = self.tasks.remove(id) {
                    task.cancel.cancel(); //记得停止现有的工作者
                }
                self.pending_tasks.remove(id); // 清理黑名单,如果有的话
            }
            Ok(Create { meta, watch }) => {
                let id = self.push_task(*meta, *watch);
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
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(task.file.clone())
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
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
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(task.file.clone())
                .range(blk.clone())
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
            task.current_concurrency += 1; // 增加并发计数器
            task.inflight |= blk; // 这个块正在下载
            self.workers.push(fut);
        }
    }
}
