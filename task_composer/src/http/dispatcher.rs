/// 当前设计模式下一个异步运行时上只会运行一个不同类型的调度器
use crate::{
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
    fs::File,
    runtime::{JoinHandle, spawn},
};
use flume as mpmc;
use futures_util::{StreamExt, select, stream::FuturesUnordered};
use identity::task::TaskId;
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
pub struct Dispatcher {
    cmd: mpmc::Receiver<TaskCommand>,
    qos: broadcast::Receiver<QosAdvice>,
    #[builder(default)]
    tasks: HashMap<TaskId, TaskState>,
    #[builder(default)]
    workers: FuturesUnordered<WorkerFuture>,
    #[builder(default)]
    pendings: HashSet<TaskId>,
}

impl Dispatcher {
    const BLOCK_SIZE: usize = 0x100_0000;
    const DEAFULT_MAX_CONCURRENCY: NonZeroU8 = NonZeroU8::new(8).unwrap();
    const DEFAULT_MAX_RETRIES: u8 = 5;

    #[instrument(skip_all)]
    pub fn spawn(mut self) -> JoinHandle<()> {
        spawn(async move {
            loop {
                // 当没有worker时，只等待命令
                if self.workers.is_empty() {
                    self.handle_cmd_res(self.cmd.recv_async().await).await;
                } else {
                    // 当有worker时，使用select等待命令或worker完成
                    select! {
                        cmd_res = self.cmd.recv_async() => {
                            self.handle_cmd_res(cmd_res).await;
                        }
                        worker_res = self.workers.next() => {
                            self.handle_worker_res(worker_res).await;
                        }
                    }
                }
            }
        })
    }

    #[inline]
    #[instrument(skip_all)]
    fn check_or_set_finished(task: &TaskState) -> bool {
        // 由于已经有worker执行成功，而且未指定范围，所以默认下载完成
        let Some(total) = task.meta.content_range() else {
            return true;
        };
        let is_finished = total == task.committed;
        if is_finished {
            let _ = task.status.send_if_modified(|status| status.state.set_finished()); // 完成了就设置状态，等外界观察到并明确删除才删除
        }
        is_finished
    }

    #[instrument(skip_all)]
    async fn handle_worker_res(&mut self, worker_res: Option<JoinResult<WorkerResult>>) {
        async fn dispose(file: &mut Option<File>) {
            if let Some(file) = file.take() {
                let _ = file.sync_all().await.inspect_err(|err| error!("failed sync file {err:?}"));
                let _ = file.close().await.inspect_err(|err| error!("failed close file {err:?}"));
            }
        }

        match worker_res {
            Some(Ok(Ok(WorkSuccess { id, downloaded }))) => {
                info!("task {id:?} finished {downloaded:?}");
                let task = self.tasks.get_mut(&id);
                if let Some(task) = task {
                    task.inflight -= &downloaded; // 取消正在下载的记录
                    task.committed |= &downloaded; // 提交已经持久化的部分
                    task.current_concurrency -= 1; // 减少并发计数器
                    if Self::check_or_set_finished(task) {
                        dispose(&mut task.file).await;
                        info!("task {id:?} finished");
                        // 记得关闭文件句柄
                        self.pendings.remove(&id); // 这一步是为了防呆
                        return;
                    }
                }
                // 如果暂停列表没有它,继续产生工作者
                if !self.pendings.contains(&id) {
                    self.spawn_many_worker(id);
                }
            }
            Some(Ok(Err(box WorkFailed { id, revert, err }))) => {
                warn!("task {id:?} failed {err:?}");
                let task = self.tasks.get_mut(&id);
                let mut need_continue = false;
                if let Some(task) = task {
                    task.current_concurrency -= 1;
                    let retries_left = task.retries_left.saturating_sub(1); // 假如队列工作者数量大于最大尝试次数会导致溢出
                    task.retries_left = retries_left;
                    need_continue = retries_left != 0;
                    // 如果有回滚的必要
                    if let Some(revert) = revert {
                        task.inflight -= &revert; // 取消正在下载的记录
                        if let WorkerError::ParitalDownloaded(ref commited) = err {
                            // 部分下载成功就回滚不成功的部分
                            task.committed |= commited; // 部分持久化成功
                            let revert = &revert - commited;
                            *task.remaining.as_mut().unwrap() |= &revert;
                        } else {
                            // 其他错误就回滚全部吧
                            *task.remaining.as_mut().unwrap() |= &revert;
                        }
                    }
                    // 如果遇到主动取消则设置主动取消flag，其他错误则对外呈现
                    if matches!(&err, WorkerError::InitiateCancel) {
                        let _ = task.status.send_if_modified(|status| status.state.set_cancelled());
                    } else {
                        task.status.send_modify(|status| {
                            status.set_err(err);
                            status.state.set_failed();
                        });
                    }
                    if Self::check_or_set_finished(task) {
                        dispose(&mut task.file).await;
                        info!("task {id:?} finished");
                        self.pendings.remove(&id); // 这一步是为了防呆
                        return;
                    }
                    if !need_continue {
                        dispose(&mut task.file).await;
                        info!("task {id:?} closed because of err count exceeds max retries");
                    }
                }

                if self.pendings.contains(&id).not() && need_continue {
                    self.spawn_many_worker(id);
                }
            }
            Some(Err(err)) => {
                error!("worker join error: {err:?}");
            }
            None => {
                error!("worker join handle closed unexpectedly");
            }
        }
    }

    #[inline]
    async fn handle_cmd_res(&mut self, cmd_res: Result<TaskCommand, mpmc::RecvError>) {
        match cmd_res {
            Ok(ChangeRateLimited { ref id, limit }) => {
                info!("task {id:?} change rate limited: {limit:?}");
                if let Some(task) = self.tasks.get_mut(id) {
                    task.rate_limit.change_limit(limit);
                }
            }
            Ok(ChangeConcurrency { ref id, concuerrency }) => {
                info!("task {id:?} change concurrency: {concuerrency}");
                if let Some(task) = self.tasks.get_mut(id) {
                    task.max_concurrency = concuerrency;
                }
            }
            Ok(Pause(ref id)) => {
                info!("pause task {id:?}");
                self.pendings.insert(*id); // 不要继续创建新工作者了
                if let Some(task) = self.tasks.get_mut(id) {
                    task.cancel.cancel(); // 取消现有工作者
                    let _ = task.status.send_if_modified(|status| status.state.set_pending()); // 修改对外状态
                }
            }
            Ok(Resume(ref id)) => {
                info!("resume task {id:?}");
                self.pendings.remove(id); // 如果是被暂停的，那就从暂停名单中移除一下
                if let Some(task) = self.tasks.get_mut(id) {
                    task.retries_left = Self::DEFAULT_MAX_RETRIES; // 可能是因为尝试次数耗尽导致停止的,所以恢复一下
                    let _ = task.status.send_if_modified(|status| status.state.set_running()); // 恢复一下对外界的状态
                }
                self.spawn_many_worker(*id); // 催动更多工作者
            }
            Ok(Cancel(ref id)) => {
                info!("cancel task {id:?}");
                // 取消所有工作者，第一个被取消的工作者会主动设置任务被取消的状态
                if let Some(task) = self.tasks.get_mut(id) {
                    // 如果任务正在运行就通知工作者取消
                    if task.status.borrow().state.is_running() {
                        task.cancel.cancel()
                    } else {
                        // 更新对外界状态为取消
                        let _ = task.status.send_if_modified(|status| status.state.set_cancelled());
                    }
                }
                // 切忌不要立刻移除任务，我们要确保外界观测到任务被取消状态
                self.pendings.remove(id); // 保护性清理，防止后续同 ID 任务一创建就被暂停
            }
            Ok(Remove(ref id)) => {
                info!("remove task {id:?}");
                let need_remove =
                    self.tasks.get(id).map(|task| task.status.borrow().state.is_terminal()).unwrap_or(false);
                if need_remove {
                    self.tasks.remove(id); // 外界已经观察到任务被取消或完成了，批准我们删除任务了，外界观察该任务的唯一通道也会被移除
                    self.pendings.remove(id); // 保护性清理
                }
            }
            // 总是从新元数据创建新文件，后面设计一个指定文件并恢复进度的
            Ok(Create { box meta, box watch }) => {
                info!("create task with meta: {:?}", meta);
                // 添加任务后立刻催动工作者
                let file = filesystem::assigned_writable_file(meta.name(), meta.mime()).await.unwrap(); // todo 错误处理，计划是推入任务，但是不spawn
                let id = self.push_task(meta, watch, file);
                self.spawn_many_worker(id);
            }
            Err(err) => {
                error!("cmd receiver error: {:?}", err);
            }
        }
    }

    #[instrument(skip_all)]
    fn push_task(&mut self, meta: HttpTaskMeta, tx: watch::Sender<TaskStatus>, file: File) -> TaskId {
        let total = meta.content_range();
        if let Some(ref total) = total {
            tx.send_modify(|status| status.total = total.last());
        }
        let task_state = TaskState {
            meta: meta.into(),
            inflight: Default::default(),
            max_concurrency: Self::DEAFULT_MAX_CONCURRENCY,
            current_concurrency: 0,
            retries_left: Self::DEFAULT_MAX_RETRIES,
            file: file.into(),
            cancel: CancellationToken::new(),
            status: tx,
            rate_limit: SharedRateLimiter::new_with_no_limit(),
            committed: Default::default(),
            remaining: total,
        };
        let id = TaskId::new();
        self.tasks.insert(id, task_state);
        id
    }

    // 对于支持多线程下载的最大并发数量才被设置，单线程始终是1
    #[instrument(skip(self))]
    fn spawn_many_worker(&mut self, id: TaskId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            return;
        };
        if task.file.is_none() {
            // 此时有 worker 关闭了文件句柄，就不要继续催动工作者了
            return;
        }
        let meta = task.meta.as_ref();
        let is_single = meta.is_support_ranges().not();
        // 如果只支持单线程且当前有一个工作者，则直接返回
        if is_single && task.current_concurrency != 0 {
            return;
        }
        // 如果仅支持单线程且当前没有工作者，则创建一个工作者
        if is_single {
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
            //不需要标记哪些块在下载
            self.workers.push(fut);
            task.current_concurrency += 1;
            info!("spawn single worker for task {id:?}");
            return;
        }
        let more_concurrency = task.max_concurrency.get().saturating_sub(task.current_concurrency);
        // 不需要更多并发
        if more_concurrency == 0 {
            return;
        }
        let _ = task.status.send_if_modified(|status| status.state.set_running());
        // 如果你支持 range 那我就假定你有 content-range
        let remaining = task.remaining.as_mut().unwrap();
        let blocks = remaining.into_chunks(Self::BLOCK_SIZE).take(more_concurrency as usize);
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
