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
use compio_watch as watch;
use flume as mpmc;
use futures_util::{FutureExt, StreamExt, select, stream::FuturesUnordered};
use identity::task::TaskId;
use smol_cancellation_token::CancellationToken;
use sparse_ranges::RangeSet;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    future::pending,
    mem,
    num::NonZeroU8,
    ops::Not,
    rc::Rc,
};
use tracing::{info, instrument, warn};
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
    file: File,
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
    pending_tasks: HashSet<TaskId>,
}

impl Dispatcher {
    const BLOCK_SIZE: usize = 0x100_0000;
    const DEAFULT_MAX_CONCURRENCY: NonZeroU8 = unsafe { NonZeroU8::new_unchecked(8) };
    const DEFAULT_MAX_RETRIES: u8 = 3;

    #[instrument(skip_all)]
    pub fn spawn(mut self) -> JoinHandle<()> {
        spawn(async move {
            loop {
                let worker_fut = if self.workers.is_empty() {
                    pending().boxed_local()
                } else {
                    self.workers.next().boxed_local()
                };
                select! {
                    cmd_res = self.cmd.recv_async() => {
                        info!("cmd_res: {:?}", cmd_res);
                        self.handle_cmd_res(cmd_res).await;
                    }
                    worker_res = worker_fut.fuse() => {
                       info!("worker_res: {:?}", worker_res);
                        if worker_res.is_some() {
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
        let Some(total) = task.meta.content_range() else {
            return true;
        };
        let is_finished = total == task.committed;
        if is_finished {
            let _ = task.status.send_if_modified(|status| status.state.set_finished()); // 完成了就设置状态，等外界观察到并明确删除才删除
        }
        is_finished
    }

    #[inline]
    #[instrument(skip_all)]
    async fn handle_worker_res(&mut self, worker_res: Option<JoinResult<WorkerResult>>) {
        match worker_res {
            Some(Ok(Ok(WorkSuccess { id, downloaded }))) => {
                let mut task = self.tasks.get_mut(&id);
                if let Some(ref mut task) = task {
                    task.committed |= &downloaded; // 提交已经持久化的部分
                    task.current_concurrency -= 1; // 减少并发计数器
                    if Self::check_or_set_finished(task) {
                        self.pending_tasks.remove(&id); // 这一步是为了防呆
                        return;
                    }
                }
                // 如果暂停列表没有它,继续产生工作者
                if !self.pending_tasks.contains(&id) {
                    self.spawn_many_worker(id);
                }
            }
            Some(Ok(Err(box WorkFailed { id, revert, err }))) => {
                let mut task = self.tasks.get_mut(&id);
                let mut need_continue = false;
                if let Some(ref mut task) = task {
                    task.current_concurrency -= 1;
                    let retries_left = task.retries_left.saturating_sub(1); // 假如队列工作者数量大于最大尝试次数会导致溢出
                    task.retries_left = retries_left;
                    need_continue = retries_left != 0;
                    // 如果有回滚的必要
                    if let Some(revert) = revert {
                        task.inflight.difference_assign(&revert); // 取消正在下载的记录
                        if let WorkerError::ParitalDownloaded(ref commited) = err {
                            task.committed |= commited; // 部分持久化成功
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
                        self.pending_tasks.remove(&id); // 这一步是为了防呆
                        return;
                    }
                }
                if self.pending_tasks.contains(&id).not() && need_continue {
                    self.spawn_many_worker(id);
                }
            }
            Some(Err(err)) => {
                panic!("{:?}", err)
            }
            None => {
                info!("dispatcher exit");
            }
        }
    }

    #[inline]
    #[instrument(skip_all)]
    async fn handle_cmd_res(&mut self, cmd_res: Result<TaskCommand, mpmc::RecvError>) {
        match cmd_res {
            Ok(ChangeRateLimited { ref id, limit }) => {
                info!("change rate limited");
                if let Some(task) = self.tasks.get_mut(id) {
                    task.rate_limit.change_limit(limit);
                }
            }
            Ok(ChangeConcurrency { ref id, concuerrency }) => {
                info!("change concurrency");
                if let Some(task) = self.tasks.get_mut(id) {
                    task.max_concurrency = concuerrency;
                }
            }
            Ok(Pause(ref id)) => {
                info!("pause task");
                self.pending_tasks.insert(*id); // 不要继续创建新工作者了
                if let Some(task) = self.tasks.get_mut(id) {
                    task.cancel.cancel(); // 取消现有工作者
                    let _ = task.status.send_if_modified(|status| status.state.set_pending()); // 修改对外状态
                }
            }
            Ok(Resume(ref id)) => {
                info!("resume task {:?}", id);
                self.pending_tasks.remove(id); // 如果是被暂停的，那就从暂停名单中移除一下
                if let Some(task) = self.tasks.get_mut(id) {
                    let _ = task.status.send_if_modified(|status| status.state.set_running()); // 恢复一下对外界的状态
                    task.retries_left = Self::DEFAULT_MAX_RETRIES; // 可能是因为尝试次数耗尽导致停止的,所以恢复一下
                }
                self.spawn_many_worker(*id); // 催动更多工作者
            }
            Ok(Cancel(ref id)) => {
                info!("cancel task: {:?}", id);
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
                self.pending_tasks.remove(id); // 保护性清理，防止后续同 ID 任务一创建就被暂停
            }
            Ok(Remove(ref id)) => {
                info!("remove task: {:?}", id);
                let removable = if let Some(task) = self.tasks.get(id) {
                    task.status.borrow().state.is_terminal()
                } else {
                    false
                };
                if removable {
                    self.tasks.remove(id); // 外界已经观察到任务被取消或完成了，批准我们删除任务了，外界观察该任务的唯一通道也会被移除
                    self.pending_tasks.remove(id); // 保护性清理
                }
            }
            // 总是从新元数据创建新文件，后面设计一个指定文件并恢复进度的
            Ok(Create { meta, watch }) => {
                info!("create task: {}", meta.name());
                // 添加任务后立刻催动工作者
                let file = filesystem::assigned_writable_file(meta.name(), meta.mime()).await.unwrap(); // todo 错误处理，计划是推入任务，但是不spawn
                let id = self.push_task(*meta, *watch, file);
                info!("task {:?} created", id);
                self.spawn_many_worker(id);
            }
            Err(err) => {
                warn!("dispatcher: watch error: {:?}", err);
            }
        }
    }

    #[instrument(skip(self))]
    fn push_task(&mut self, meta: HttpTaskMeta, tx: watch::Sender<TaskStatus>, file: File) -> TaskId {
        let total = meta.content_range();
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
        let meta = task.meta.as_ref();
        let is_single = meta.is_support_ranges().not();
        // 如果只支持单线程且当前有一个工作者，则直接返回
        if is_single && task.current_concurrency == 1 {
            return;
        }
        // 如果仅支持单线程且当前没有工作者，则创建一个工作者
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
            //不需要标记哪些块在下载
            self.workers.push(fut);
            task.current_concurrency += 1;
            return;
        }
        let more_concurrency = task.max_concurrency.get().saturating_sub(task.current_concurrency);
        // 不需要更多并发
        if more_concurrency == 0 {
            return;
        }
        // 如果你支持 range 那我就假定你有 content-range
        let remaining = task.remaining.as_mut().unwrap();
        let blocks = remaining.into_chunks(Self::BLOCK_SIZE).take(more_concurrency as usize);
        for blk in blocks {
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(task.file.clone())
                .assign(blk.clone())
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
            task.current_concurrency += 1; // 增加并发计数器
            task.inflight |= &blk; // 这个块正在下载
            self.workers.push(fut);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::meta::fetch_meta;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;
    use url::Url;

    #[compio::test]
    async fn test_dispatcher() {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG) // 捕获 DEBUG 及更高级别的日志
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
        let (cmd_tx, cmd_rx) = mpmc::unbounded();
        let (_qos_tx, qos_rx) = broadcast::broadcast(1);
        let dispatcher = Dispatcher::builder().cmd(cmd_rx).qos(qos_rx).build();
        spawn(async move { dispatcher.spawn().await }).detach();
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let meta = Box::new(fetch_meta(&url).await.unwrap());
        let (status_tx, status_rx) = watch::channel(TaskStatus::default());
        let cmd = TaskCommand::Create { meta, watch: status_tx.into() };
        cmd_tx.send_async(cmd).await.unwrap();
        // 先打印一次初始状态
        println!("Initial status: {:?}", status_rx.borrow());

        loop {
            // 使用 changed() 等待后续的状态更新
            if status_rx.changed().await.is_err() {
                // 如果发送端被丢弃（例如任务被移除），通道会关闭，这里会收到错误
                println!("Channel closed, exiting loop.");
                break;
            }
            println!("Updated status: {:?}", status_rx.borrow());
        }
    }
}
