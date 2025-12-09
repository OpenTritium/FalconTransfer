//！每个任务调度器被设计为 ThreadLocal 的，通过 channel 传递命令，通过 watcher 观察当前调度器的所有任务状态
use crate::{
    TaskStateDesc,
    http::{
        command::TaskCommand::{self, *},
        meta::HttpTaskMeta,
        qos::QosAdvice,
        status::TaskStatus,
        worker::{WorkFailed, WorkSuccess, Worker, WorkerError, WorkerFuture, WorkerResult},
    },
    utils::rate_limiter::SharedRateLimiter,
};
use async_broadcast as broadcast;
use compio::{
    fs::{File, OpenOptions},
    runtime::{JoinHandle, spawn},
};
use falcon_config::config;
use falcon_filesystem::assign_path;
use falcon_identity::task::TaskId;
use flume as mpmc;
use futures_util::{StreamExt, select, stream::FuturesUnordered};
use see::sync as watch;
use smol_cancellation_token::CancellationToken;
use sparse_ranges::RangeSet;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    num::NonZeroU8,
    ops::Not,
    rc::Rc,
};
use tracing::{error, info, instrument, warn};
use typed_builder::TypedBuilder;

pub type JoinResult<T> = Result<T, Box<dyn Any + Send>>;

#[derive(Debug)]
pub struct TaskState {
    meta: Rc<HttpTaskMeta>,
    remaining: Option<RangeSet>,
    committed: RangeSet,
    inflight: RangeSet,
    max_concurrency: NonZeroU8,
    current_concurrency: u8,
    retries_left: u8,
    file: Option<File>,
    cancel: CancellationToken,
    status: watch::Sender<TaskStatus>,
    rate_limit: SharedRateLimiter,
}

#[derive(Debug, TypedBuilder)]
pub struct TaskDispatcher {
    cmd: mpmc::Receiver<TaskCommand>,
    qos: broadcast::Receiver<QosAdvice>,
    #[builder(default)]
    tasks: HashMap<TaskId, TaskState>,
    #[builder(default)]
    workers: FuturesUnordered<WorkerFuture>,
    #[builder(default)]
    pendings: HashSet<TaskId>, // 用于阻止继续产生工作者，只需要在移除任务或恢复任务的时候删除就可以了
}

impl TaskDispatcher {
    #[instrument(skip_all)]
    #[inline]
    pub fn spawn(mut self) -> JoinHandle<()> {
        let fut = async move {
            loop {
                // 当没有worker时，只等待命令
                if self.workers.is_empty() {
                    self.handle_cmd_res(self.cmd.recv_async().await).await;
                    continue;
                }
                select! {
                    cmd_res = self.cmd.recv_async() => {
                        self.handle_cmd_res(cmd_res).await;
                    }
                    worker_res = self.workers.next() => {
                        self.handle_worker_res(worker_res).await;
                    }
                }
            }
        };
        spawn(fut)
    }

    #[inline]
    fn check_or_set_finished(task: &TaskState) -> bool {
        // 对于没有指定 content_length 的任务，第一次尝试足矣，因为你无法判断部分下载还是完全下载
        let Some(full_content_range) = task.meta.full_content_range() else {
            return true;
        };
        let was_finished = full_content_range == task.committed;
        if was_finished {
            let _ = task.status.send_if_modified(|s| s.state.set_completed());
        }
        was_finished
    }

    /// 拿走并关闭文件句柄，尝试恢复任务时再根据路径重新打开句柄
    async fn dispose_file(file: &mut Option<File>) {
        if let Some(file) = file.take() {
            file.sync_all().await.expect("Failed to sync all to disk");
            file.close().await.expect("Failed to close file handle");
        }
    }

    /// 只负责更新失败和完成状态
    #[instrument(skip_all)]
    async fn handle_worker_res(&mut self, worker_res: Option<JoinResult<WorkerResult>>) {
        match worker_res {
            Some(Ok(Ok(WorkSuccess { id, downloaded }))) => {
                info!("Task {id:?} downloaded {downloaded:?}");
                let task = self.tasks.get_mut(&id);
                if let Some(task) = task {
                    task.inflight -= &downloaded; // 取消正在下载的记录
                    task.committed |= &downloaded; // 提交已经持久化的部分
                    task.current_concurrency -= 1; // 减少并发计数器
                    if Self::check_or_set_finished(task) {
                        // 落盘并关闭文件
                        Self::dispose_file(&mut task.file).await;
                        info!("Task {id:?} was finished");
                        self.pendings.insert(id); // 放置路障
                        return;
                    }
                }
                // 检查路障列表
                if !self.pendings.contains(&id) {
                    self.spawn_many_worker(id);
                }
            }
            Some(Ok(Err(box WorkFailed { id, revert, err }))) => {
                use TaskStateDesc::*;
                use WorkerError::*;
                warn!("Task {id:?} failed: {err:?}");
                let task = self.tasks.get_mut(&id);
                if let Some(task) = task {
                    task.current_concurrency -= 1;
                    task.retries_left = task.retries_left.saturating_sub(1); // 假如同时有一大批工作者失败且工作者数量大于最大尝试次数会导致负溢出
                    // 如果有回滚的必要
                    if let Some(revert) = revert {
                        task.inflight -= &revert; // 取消正在下载的记录
                        if let ParitalDownloaded(ref commited) = err {
                            // 部分下载成功就回滚不成功的部分
                            task.committed |= commited; // 部分持久化成功
                            let revert = &revert - commited;
                            *task.remaining.as_mut().unwrap() |= &revert;
                        } else {
                            // 其他错误就回滚全部吧
                            *task.remaining.as_mut().unwrap() |= &revert;
                        }
                    }
                    // 先更新错误
                    task.status.send_modify(|status| {
                        status.set_err(err);
                    });
                    // 虽然遇到了错误，不过接下来还是要检查下下载完没有的
                    if Self::check_or_set_finished(task) {
                        Self::dispose_file(&mut task.file).await;
                        self.pendings.insert(id);
                        info!("Task {id:?} finished, file handle has been dropped");
                        return;
                    }
                    // 我想我们不应该继续包容错误了
                    if task.retries_left == 0 {
                        Self::dispose_file(&mut task.file).await;
                        // 如果任务已经处于停止状态，就不要去用错误覆盖这种状态
                        let _ = task.status.send_if_modified(|status| {
                            let state = &mut status.state;
                            if matches!(state, Cancelled | Completed | Paused) {
                                return false;
                            }
                            state.set_failed()
                        });
                        self.pendings.insert(id);
                        warn!("Task {id:?} closed because of err count exceeds max retries");
                        return;
                    }
                }
                // 不继续产生工作者的唯一来源就是路障
                if self.pendings.contains(&id).not() {
                    self.spawn_many_worker(id);
                }
            }
            Some(Err(err)) => {
                panic!("Runtime error: {err:?}")
            }
            None => {
                unreachable!("Dispather doesn't handle worker result when worker is empty")
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle_cmd_res(&mut self, cmd_res: Result<TaskCommand, mpmc::RecvError>) {
        match cmd_res {
            Ok(ChangeRateLimited { ref id, limit }) => {
                if let Some(task) = self.tasks.get_mut(id) {
                    task.rate_limit.change_limit(limit);
                    info!("Task {id:?} change rate limite: {limit:?}");
                    return;
                }
                warn!("Attempt to change rate limite for a non-existent task {id:?}");
            }
            Ok(ChangeConcurrency { ref id, concurrency: concuerrency }) => {
                if let Some(task) = self.tasks.get_mut(id) {
                    task.max_concurrency = concuerrency;
                    info!("Task {id:?} change concurrency: {concuerrency}");
                    return;
                }
                warn!("Attempt to change concurrency for a non-existent task {id:?}");
            }
            Ok(Pause(ref id)) => {
                self.pendings.insert(*id);
                if let Some(task) = self.tasks.get_mut(id) {
                    task.cancel.cancel(); // 取消现有工作者
                    info!("Task {id:?} was paused");
                    let _ = task.status.send_if_modified(|status| status.state.set_paused()); // 即使有现存的工作者刚好失败也不会覆盖暂停状态的
                    return;
                }
                warn!("Attempt to change concurrency for a non-existent task {id:?}");
            }
            Ok(Resume(ref id)) => {
                use TaskStateDesc::*;
                self.pendings.remove(id); // 清理路障
                if let Some(task) = self.tasks.get_mut(id) {
                    task.retries_left = config!(worker_max_retries); // 可能是因为尝试次数耗尽导致停止的，所以恢复一下
                    info!("Resume Task {task:?} retry count");
                    // 你不能 resume 一个已经完成的任务和空闲的任务
                    let state = task.status.borrow().state;
                    if matches!(state, Completed | Idle) {
                        warn!("Cannot resume a completed or idle task");
                        return;
                    }
                    let file = try {
                        match (task.file.take(), task.status.borrow().path.as_ref()) {
                            (None, None) => {
                                info!("Try reassign path and recreate file handle");
                                let path = assign_path(task.meta.as_path_name(), task.meta.mime()).await?;
                                let file = OpenOptions::new().create_new(true).write(true).open(&path).await?;
                                task.status.send_modify(|s| s.path = path.into());
                                file
                            }
                            (None, Some(path)) => {
                                info!("Try recreate file handle");
                                OpenOptions::new().write(true).open(path).await?
                            }
                            //  有文件句柄你还说啥呢
                            (Some(file), _) => file,
                        }
                    };
                    match file {
                        Ok(file) => {
                            task.file = file.into();
                            self.spawn_many_worker(*id);
                            info!("Resume task {id:?}");
                        }
                        Err(err) => {
                            warn!("Failed to recreate file handle :{err}");
                            task.status.send_modify(|s| {
                                s.set_err(err.into());
                                s.state.set_failed();
                            });
                            self.pendings.insert(*id);
                        }
                    }
                }
                warn!("Attempt to resume a non-existent task {id:?}");
            }
            Ok(Cancel(ref id)) => {
                if let Some(task) = self.tasks.get_mut(id) {
                    info!("Cancel Task {id:?} running workers");
                    if task.status.borrow().state.is_running() {
                        task.cancel.cancel()
                    }
                    // 你无法取消一个已经完成或已经取消的任务
                    let cancelable = task.status.send_if_modified(|s| {
                        if s.state.was_completed() {
                            return false;
                        }
                        s.state.set_cancelled()
                    });
                    if cancelable {
                        Self::dispose_file(&mut task.file).await;
                        self.pendings.insert(*id);
                        info!("Cancel task {id:?}");
                        return;
                    }
                    warn!(
                        "Task {id:?} cannot be cancel, because current task status is {state:?}",
                        state = task.status.borrow().state
                    );
                }
                warn!("Attempt to cancel a non-existent task {id:?}");
                // 切忌不要立刻移除任务，我们要确保外界观测到任务被取消状态
            }
            Ok(Remove(ref id)) => {
                use TaskStateDesc::*;
                let couldbe_remove = self
                    .tasks
                    .get(id)
                    .map(|task| {
                        debug_assert!(task.file.is_none(), "the file should be droppped already");
                        let state = task.status.borrow().state;
                        !matches!(state, Running | Paused)
                    })
                    .unwrap_or(false);
                // 你无法去取消一个正在下载或暂停的任务，你需要先把他们取消或者等他们完成或遭遇错误
                if couldbe_remove {
                    self.tasks.remove(id);
                    self.pendings.remove(id); // 防止阻拦未来的新任务
                    info!("Task {id:?} has been removed");
                    return;
                }
                warn!("Task {id:?} is runnning or paused, cannnot be removed");
            }
            // 总是从新元数据创建新文件，后面设计一个指定文件并恢复进度的
            // 这里需要更新对外状态的路径
            Ok(Create { box meta, box watch }) => {
                info!("Create task with meta: {meta:?}");
                let file = try {
                    let path = assign_path(meta.name(), meta.mime()).await?;
                    info!("Assign path: {path}");
                    let file = OpenOptions::new().create_new(true).write(true).open(&path).await?;
                    info!("Create file success");
                    watch.send_modify(|s| s.path = path.into());
                    file
                };
                let id = watch.borrow().id;
                match file {
                    Ok(file) => {
                        self.push_task(meta, watch, file.into());
                        self.spawn_many_worker(id);
                    }
                    Err(err) => {
                        error!("Failed to create file for task {id:?}: {err:?}");
                        watch.send_modify(|status| {
                            status.set_err(err.into());
                            status.state.set_failed(); // 确保状态更新为Failed
                        });
                        self.push_task(meta, watch, None);
                    }
                }
            }
            Err(err) => {
                panic!("Command channel error: {err}");
            }
        }
    }

    #[inline]
    fn push_task(&mut self, meta: HttpTaskMeta, tx: watch::Sender<TaskStatus>, file: Option<File>) {
        let full_content_range = meta.full_content_range();
        let id = tx.borrow().id;
        info!("Push task {id:?} into task quene");
        let task_state = TaskState {
            meta: meta.into(),
            inflight: Default::default(),
            max_concurrency: config!().worker_max_concurrency,
            current_concurrency: 0,
            retries_left: config!(worker_max_retries),
            file,
            cancel: CancellationToken::new(),
            status: tx,
            rate_limit: SharedRateLimiter::new_without_limit(),
            committed: Default::default(),
            remaining: full_content_range,
        };
        self.tasks.insert(id, task_state);
    }

    // 对于支持多线程下载的最大并发数量才被设置，单线程始终是1
    #[instrument(skip(self))]
    fn spawn_many_worker(&mut self, id: TaskId) {
        info!("Attempting to spawn worker for task {id:?}");
        let Some(task) = self.tasks.get_mut(&id) else {
            warn!("Task {id:?} not found when trying to spawn more worker");
            return;
        };
        if task.file.is_none() {
            // 此时有 worker 关闭了文件句柄，就不要继续催动工作者了
            warn!("Task {id:?} has no file handle, not spawning worker");
            return;
        }
        let meta = task.meta.as_ref();
        let fire_once = meta.is_support_ranges().not(); // 该任务只发射一次
        info!("Task {id:?} is single threaded: {fire_once}");
        // 如果只支持单线程且当前有一个工作者，则直接返回
        if fire_once && task.current_concurrency != 0 {
            warn!("Task {id:?} is single-threaded and already has a worker, not spawning more");
            return;
        }
        // 如果仅支持单线程且当前没有工作者，则创建一个工作者
        if fire_once {
            let _ = task.status.send_if_modified(|status| status.state.set_running());
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(task.file.clone().unwrap())
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
            // 不需要 commit 和 inflight
            self.workers.push(fut);
            task.current_concurrency += 1;
            info!("Spawn single worker for task {id:?}");
            return;
        }
        let more_concurrency = task.max_concurrency.get().saturating_sub(task.current_concurrency);
        info!("Task {id:?} can spawn {more_concurrency} more workers");
        // 不需要更多并发
        if more_concurrency == 0 {
            info!("Task {id:?} has reached max concurrency, not spawning more workers");
            return;
        }
        let _ = task.status.send_if_modified(|status| status.state.set_running());
        // 如果你支持 range 那我就假定你有 content-range
        let remaining = task.remaining.as_mut().expect("Task supports ranges but have no content range");
        let blocks = remaining.into_chunks(config!(http_block_size)).take(more_concurrency as usize);
        for blk in blocks {
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(task.file.clone().unwrap())
                .assign(blk.clone())
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
            task.current_concurrency += 1; // 增加并发计数器
            task.inflight |= &blk; // 这个块正在下载
            self.workers.push(fut);
            info!("spawn multi worker for task {id:?} with range {blk:?}");
        }
    }
}
