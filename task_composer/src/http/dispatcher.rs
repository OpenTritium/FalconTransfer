// Each task dispatcher is designed as ThreadLocal, receiving commands via channel and observing task states via watcher
use crate::{
    TaskStateDesc,
    http::{
        command::TaskCommand::{self, *},
        meta::HttpTaskMeta,
        qos::QosAdvice,
        status::TaskStatus,
        worker::{WorkFailed, WorkSuccess, Worker, WorkerError, WorkerFuture, WorkerResult},
    },
    persist::{PersistTaskPendings, PersistTaskStates},
    utils::rate_limiter::SharedRateLimiter,
};
use async_broadcast as broadcast;
use coarsetime::{Duration, Instant};
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
use tracing::{debug, error, info, instrument, warn};
use typed_builder::TypedBuilder;

pub type JoinResult<T> = Result<T, Box<dyn Any + Send>>;

/// Task state maintaining download task internal state.
#[derive(Debug, TypedBuilder)]
pub struct TaskState {
    #[builder(setter(into))]
    pub meta: Rc<HttpTaskMeta>,
    #[builder(default)]
    pub remaining: Option<RangeSet>,
    pub committed: RangeSet,
    #[builder(default)]
    pub inflight: RangeSet,
    #[builder(default = config!().worker_max_concurrency)]
    pub max_concurrency: NonZeroU8,
    #[builder(default)]
    pub current_concurrency: u8,
    #[builder(default = config!(worker_max_retries))]
    pub retries_left: u8,
    #[builder(default)]
    pub file: Option<File>,
    #[builder(default = CancellationToken::new())]
    pub cancel: CancellationToken,
    pub status: watch::Sender<TaskStatus>,
    #[builder(default = SharedRateLimiter::without_limit())]
    pub rate_limit: SharedRateLimiter,
}

impl TaskState {
    /// Converts task state to persistent format.
    fn to_persist(&self) -> crate::persist::PersistTaskState {
        use crate::persist::PersistTaskState;
        let status = self.status.borrow();
        PersistTaskState::builder()
            .meta(self.meta.as_ref().clone())
            .remaining(self.remaining.clone())
            .committed(self.committed.clone())
            .state(status.state)
            .path(status.path.clone())
            .build()
    }
}

/// Task dispatcher managing download task lifecycles.
///
/// Core responsibilities:
/// 1. Receive and handle external commands (create, pause, resume, cancel, delete)
/// 2. Manage concurrent download workers
/// 3. Handle worker results and update task states
/// 4. Provide persistent state broadcasting
#[derive(Debug, TypedBuilder)]
pub struct TaskDispatcher {
    #[builder(default = broadcast::broadcast(1).0)]
    persist_tasks: broadcast::Sender<PersistTaskStates>,
    #[builder(default = broadcast::broadcast(1).0)]
    persist_pendings: broadcast::Sender<PersistTaskPendings>,
    cmd: mpmc::Receiver<TaskCommand>,
    #[allow(unused)]
    #[builder(default = broadcast::broadcast(1).1)]
    qos: broadcast::Receiver<QosAdvice>,
    #[builder(default)]
    tasks: HashMap<TaskId, TaskState>,
    #[builder(default)]
    workers: FuturesUnordered<WorkerFuture>,
    #[builder(default)]
    pendings: HashSet<TaskId>,
    #[builder(default = Instant::recent())]
    last_broadcast: Instant,
    #[builder(default)]
    broadcast_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to load persisted states")]
    PersistLoad(#[from] falcon_persist::Error),
    #[error("Persisted states is empty")]
    PersistEmpty,
    #[error("No path when restore a running task")]
    NoPathWhenRestoreRunnigTask,
    #[error("Failed to reopen file")]
    ReopenFile(#[from] std::io::Error),
}

impl TaskDispatcher {
    /// Broadcasts all task states and pendings immediately to persistence subscribers.
    ///
    /// Uses `try_broadcast` for non-blocking operation. Receivers use overflow mode,
    /// so channel should never be full - old messages are dropped instead.
    #[inline]
    fn broadcast_now(&mut self) {
        let states = PersistTaskStates(self.tasks.iter().map(|(&id, state)| (id, state.to_persist())).collect());
        let pendings = PersistTaskPendings(self.pendings.clone().into_iter().collect());
        // Broadcast both first to minimize inconsistent state window
        let states_res = self.persist_tasks.try_broadcast(states);
        let pendings_res = self.persist_pendings.try_broadcast(pendings);
        // Handle errors after both broadcasts attempted
        if let Err(err) = states_res {
            error!(error = %err, "Failed to broadcast task states");
        }
        if let Err(err) = pendings_res {
            error!(error = %err, "Failed to broadcast pending tasks");
        }
        self.last_broadcast = Instant::now();
    }

    /// Broadcasts if configured time interval has elapsed since last broadcast.
    #[inline]
    fn broadcast_if_needed(&mut self) {
        if self.last_broadcast.elapsed() >= self.broadcast_interval {
            self.broadcast_now();
        }
    }

    /// Subscribes to persisted tasks broadcast before dispatcher starts.
    pub fn subscribe_persisted_tasks(&self) -> broadcast::Receiver<PersistTaskStates> {
        let mut rx = self.persist_tasks.new_receiver();
        // Overflow mode: drop old messages when full, don't block dispatcher
        rx.set_overflow(true);
        // Don't keep sender active just because receiver exists
        rx.set_await_active(false);
        rx
    }

    /// Subscribes to pending tasks broadcast after dispatcher starts.
    pub fn subscribe_pendings(&self) -> broadcast::Receiver<PersistTaskPendings> {
        let mut rx = self.persist_pendings.new_receiver();
        // Overflow mode: drop old messages when full, don't block dispatcher
        rx.set_overflow(true);
        // Don't keep sender active just because receiver exists
        rx.set_await_active(false);
        rx
    }

    /// Acquires watchers for all current tasks before dispatcher starts.
    #[inline]
    pub fn acquire_watchers(&self) -> impl Iterator<Item = (TaskId, watch::Receiver<TaskStatus>)> {
        self.tasks.iter().map(|(&id, state)| (id, state.status.subscribe()))
    }

    /// Starts dispatcher main event loop.
    ///
    /// Loop logic:
    /// 1. When no workers, block waiting for commands
    /// 2. When workers exist, use select! to wait for both commands and worker completion
    /// 3. Returns false on command channel close to exit loop
    #[instrument(skip_all)]
    pub fn spawn(mut self) -> JoinHandle<()> {
        // Initialize broadcast interval and last_broadcast time
        self.broadcast_interval = Duration::from_secs(config!().persist_broadcast_interval_secs() as u64);
        self.last_broadcast = Instant::now();

        let fut = async move {
            'main: loop {
                // Optimization: only wait for commands when no workers, avoiding unnecessary polling
                if self.workers.is_empty() {
                    if !self.handle_cmd_res(self.cmd.recv_async().await).await {
                        break 'main;
                    }
                    continue;
                }
                select! {
                    cmd_res = self.cmd.recv_async() => {
                        if !self.handle_cmd_res(cmd_res).await {
                            break 'main;
                        }
                    }
                    worker_res = self.workers.next() => {
                        self.handle_worker_res(worker_res).await;
                    }
                }
                // Check if we need to broadcast based on time interval
                self.broadcast_if_needed();
            }
        };
        spawn(fut)
    }

    #[inline]
    fn check_or_set_finished(task: &TaskState) -> bool {
        // For tasks without content_length, first attempt suffices (cannot judge partial vs complete)
        let Some(full_content_range) = task.meta.full_content_range() else {
            return true;
        };
        let was_finished = full_content_range == task.committed;
        if was_finished {
            let _ = task.status.send_if_modified(|s| s.state.set_completed());
        }
        was_finished
    }

    /// Takes and closes file handle, reopens when resuming.
    async fn dispose_file(file: &mut Option<File>) {
        if let Some(file) = file.take() {
            if let Err(e) = file.sync_all().await {
                error!(error = %e, "Failed to sync file data to disk");
            }
            if let Err(e) = file.close().await {
                error!(error = %e, "Failed to close file handle");
            }
        }
    }

    /// Handles worker completion event (success or failure).
    ///
    /// Logic:
    /// 1. Success: update committed ranges, check completion, spawn more workers if incomplete
    /// 2. Failure: rollback uncommitted data, decrement retries, handle based on error type
    /// 3. Mark task as failed if retries exhausted
    #[instrument(skip_all)]
    async fn handle_worker_res(&mut self, worker_res: Option<JoinResult<WorkerResult>>) {
        match worker_res {
            // Worker completed successfully
            Some(Ok(Ok(WorkSuccess { id, flushed }))) => {
                info!(task_id = %id, flushed_range = %flushed, "Worker completed download successfully");
                let task = self.tasks.get_mut(&id);
                if let Some(task) = task {
                    // Update task state: remove from in-flight, add to committed
                    task.inflight -= &flushed;
                    task.committed |= &flushed;
                    task.current_concurrency -= 1;
                    // Check if task fully completed
                    if Self::check_or_set_finished(task) {
                        Self::dispose_file(&mut task.file).await;
                        info!(task_id = %id, "Task download completed");
                        self.pendings.insert(id);
                        self.broadcast_now();
                        return;
                    }
                }
                // Task incomplete and not paused, continue spawning workers
                if !self.pendings.contains(&id) {
                    self.spawn_many_worker(id);
                }
            }
            // Worker download failed
            Some(Ok(Err(WorkFailed { id, assigned, err }))) => {
                use TaskStateDesc::*;
                use WorkerError::*;
                warn!(task_id = %id, error = %err, "Worker failed");
                let task = self.tasks.get_mut(&id);
                if let Some(task) = task {
                    task.current_concurrency -= 1;
                    task.retries_left = task.retries_left.saturating_sub(1);
                    // Handle failed data rollback
                    if let Some(assigned_range) = assigned {
                        task.inflight -= &assigned_range;
                        // Special handling: preserve committed part if partially downloaded
                        if let ParitalDownloaded(ref commited) = err {
                            task.committed |= commited;
                            let revert = &assigned_range - commited;
                            // remaining must exist for multipart downloads
                            *task
                                .remaining
                                .as_mut()
                                .expect("Task remaining range must exist when handling worker failure") |= &revert;
                        } else {
                            // Other errors: rollback full range for redownload
                            // remaining must exist for multipart downloads
                            *task
                                .remaining
                                .as_mut()
                                .expect("Task remaining range must exist when handling worker failure") |=
                                &assigned_range;
                        }
                    }
                    // Send error status notification
                    task.status.send_modify(|status| {
                        status.set_err(err);
                    });
                    // Check if completed after failure (e.g., unknown size tasks)
                    if Self::check_or_set_finished(task) {
                        Self::dispose_file(&mut task.file).await;
                        self.pendings.insert(id);
                        info!(task_id = %id, "Task finished after worker failure, file handle closed");
                        self.broadcast_now();
                        return;
                    }
                    // Retries exhausted, mark task as failed
                    if task.retries_left == 0 {
                        Self::dispose_file(&mut task.file).await;
                        // Don't override stop states with error
                        let _ = task.status.send_if_modified(|status| {
                            let state = &mut status.state;
                            if matches!(state, Cancelled | Completed | Paused) {
                                return false;
                            }
                            state.set_failed()
                        });
                        self.pendings.insert(id);
                        warn!(task_id = %id, "Task closed due to exceeding maximum retry count");
                        self.broadcast_now();
                        return;
                    }
                }
                // Has retries left and not paused, continue trying
                if self.pendings.contains(&id).not() {
                    self.spawn_many_worker(id);
                }
            }
            // Worker thread panic or abnormal termination
            Some(Err(err)) => {
                error!(error = ?err, "Worker thread panic or unexpected termination");
            }
            // Should not happen: method should not be called when workers is empty
            None => {
                unreachable!("Dispatcher attempted to handle worker result when workers list is empty")
            }
        }
    }

    /// Handles external commands (create, pause, resume, cancel, delete, config change).
    ///
    /// Returns:
    /// - true: continue running
    /// - false: command channel closed, dispatcher should stop
    #[instrument(skip_all)]
    async fn handle_cmd_res(&mut self, cmd_res: Result<TaskCommand, mpmc::RecvError>) -> bool {
        match cmd_res {
            // Change task rate limit
            Ok(ChangeRateLimited { ref id, limit }) => {
                if let Some(task) = self.tasks.get_mut(id) {
                    task.rate_limit.change_limit(limit);
                    info!(task_id = %id, rate_limit = ?limit, "Task rate limit changed");
                    return true;
                }
                warn!(task_id = %id, "Attempted to change rate limit for non-existent task");
            }
            // Change task max concurrency
            Ok(ChangeConcurrency { ref id, concurrency }) => {
                if let Some(task) = self.tasks.get_mut(id) {
                    task.max_concurrency = concurrency;
                    info!(task_id = %id, concurrency = concurrency, "Task concurrency changed");
                    return true;
                }
                warn!(task_id = %id, "Attempted to change concurrency for non-existent task");
            }
            // Pause task: cancel all workers, release file handle
            Ok(Pause(ref id)) => {
                self.pendings.insert(*id);
                if let Some(task) = self.tasks.get_mut(id) {
                    task.cancel.cancel();
                    Self::dispose_file(&mut task.file).await;
                    info!(task_id = %id, "Task paused, file handle released");
                    let _ = task.status.send_if_modified(|status| status.state.set_paused());
                    self.broadcast_now();
                    return true;
                }
                warn!(task_id = %id, "Attempted to pause non-existent task");
            }
            // Resume task: reopen file handle, spawn workers
            Ok(Resume(ref id)) => {
                use TaskStateDesc::*;
                self.pendings.remove(id);
                if let Some(task) = self.tasks.get_mut(id) {
                    task.retries_left = config!(worker_max_retries);
                    info!(task_id = %id, "Task resumed, retry count reset");
                    let state = task.status.borrow().state;
                    if matches!(state, Completed | Idle) {
                        warn!(task_id = %id, "Cannot resume a completed or idle task");
                        return true;
                    }
                    // Reopen file handle (decide how to open based on task state)
                    let file = try {
                        match (task.file.take(), task.status.borrow().path.as_ref()) {
                            // First resume, no file handle or path: create new file
                            (None, None) => {
                                let path = assign_path(task.meta.as_path_name(), task.meta.mime()).await?;
                                let file = OpenOptions::new().create_new(true).write(true).open(&path).await?;
                                task.status.send_modify(|s| s.path = path.into());
                                file
                            }
                            // Path exists, reopen existing file
                            (None, Some(path)) => OpenOptions::new().write(true).open(path).await?,
                            // File handle exists, use directly
                            (Some(file), _) => file,
                        }
                    };
                    match file {
                        Ok(file) => {
                            task.file = file.into();
                            self.spawn_many_worker(*id);
                            info!(task_id = %id, "Task resumed successfully");
                            self.broadcast_now();
                        }
                        Err(err) => {
                            warn!(task_id = %id, error = %err, "Failed to recreate file handle");
                            task.status.send_modify(|s| {
                                s.set_err(err.into());
                                s.state.set_failed();
                            });
                            self.pendings.insert(*id);
                            self.broadcast_now();
                        }
                    }
                }
                warn!(task_id = %id, "Attempted to resume non-existent task");
            }
            // Cancel task: cancel all workers, release file handle, update state
            Ok(Cancel(ref id)) => {
                if let Some(task) = self.tasks.get_mut(id) {
                    self.pendings.insert(*id);
                    info!(task_id = %id, "Cancelling running workers for task");
                    // If task is running, cancel all workers
                    if task.status.borrow().state.is_running() {
                        task.cancel.cancel()
                    }
                    // Update task state to cancelled
                    let cancelable = task.status.send_if_modified(|s| {
                        if s.state.was_completed() {
                            return false;
                        }
                        s.state.set_cancelled()
                    });
                    if cancelable {
                        Self::dispose_file(&mut task.file).await;
                        info!(task_id = %id, "Task cancelled and file handle released");
                        self.broadcast_now();
                        return true;
                    }
                    warn!(
                        task_id = %id, current_state = %task.status.borrow().state,
                        "Task cannot be cancelled in current state"
                    );
                }
                warn!(task_id = %id, "Attempted to cancel non-existent task");
                // Don't remove task immediately, ensure external observers see cancelled state
            }
            // Remove task: only completed or failed tasks, running or paused tasks must be cancelled first
            Ok(Remove(ref id)) => {
                use TaskStateDesc::*;
                let couldbe_remove = self
                    .tasks
                    .get(id)
                    .map(|task| {
                        debug_assert!(task.file.is_none(), "File handle should be released before removal");
                        let state = task.status.borrow().state;
                        !matches!(state, Running | Paused)
                    })
                    .unwrap_or(false);
                // Cannot remove running or paused tasks, must cancel or wait for completion
                if couldbe_remove {
                    self.tasks.remove(id);
                    self.pendings.remove(id);
                    info!(task_id = %id, "Task removed from dispatcher");
                    self.broadcast_now();
                    return true;
                }
                warn!(task_id = %id, "Cannot remove task that is running or paused");
            }
            // Create new task: assign file path, open file, spawn workers
            Ok(Create { box meta, box watch }) => {
                info!(task_name = %meta.name(), "Creating new download task");
                let path = match assign_path(meta.name(), meta.mime()).await {
                    Ok(path) => path,
                    Err(err) => {
                        error!(error = %err, "Failed to assign file path for new task");
                        watch.send_modify(|status| {
                            status.set_err(err.into());
                            status.state.set_failed();
                        });
                        self.push_task(meta, watch, None);
                        return true;
                    }
                };
                let file = OpenOptions::new().create_new(true).write(true).open(&path).await;
                let id = watch.borrow().id;
                match file {
                    Ok(file) => {
                        watch.send_modify(|s| s.path = path.into());
                        self.push_task(meta, watch, file.into());
                        self.spawn_many_worker(id);
                    }
                    Err(err) => {
                        error!(task_id = %id, error = %err, "Failed to create file for new task");
                        watch.send_modify(|status| {
                            status.set_err(err.into());
                            status.state.set_failed();
                        });
                        self.push_task(meta, watch, None);
                    }
                }
            }
            // Command channel closed, dispatcher should stop running
            Err(err) => {
                error!(error = %err, "Command channel closed unexpectedly, dispatcher shutting down");
                return false;
            }
        }
        true
    }

    /// Adds new task to dispatcher task list.
    ///
    /// Initializes all task state fields but does not spawn workers immediately.
    /// Must call spawn_many_worker subsequently to start downloads.
    #[inline]
    fn push_task(&mut self, meta: HttpTaskMeta, tx: watch::Sender<TaskStatus>, file: Option<File>) {
        let full_content_range = meta.full_content_range();
        let id = tx.borrow().id;
        info!(task_id = %id, task_name = %meta.name(), "Task registered in dispatcher");
        let task_state = TaskState {
            meta: meta.into(),
            inflight: Default::default(),
            max_concurrency: config!().worker_max_concurrency,
            current_concurrency: 0,
            retries_left: config!(worker_max_retries),
            file,
            cancel: CancellationToken::new(),
            status: tx,
            rate_limit: SharedRateLimiter::without_limit(),
            committed: Default::default(),
            remaining: full_content_range,
        };

        // Broadcast task creation for persistence
        if let Err(err) = self
            .persist_tasks
            .try_broadcast(PersistTaskStates(std::iter::once((id, task_state.to_persist())).collect()))
        {
            warn!(task_id = %id, error = %err, "Failed to broadcast new task for persistence");
        }

        self.tasks.insert(id, task_state);
    }

    /// Spawns multiple workers for task based on concurrency config and remaining ranges.
    ///
    /// Working modes:
    /// 1. Single-worker mode: server doesn't support range requests, only one worker created
    /// 2. Multi-worker mode: split remaining ranges into chunks, create worker for each chunk
    ///
    /// Note: Updates task state to Running before spawning workers.
    #[instrument(skip(self))]
    fn spawn_many_worker(&mut self, id: TaskId) {
        let Some(task) = self.tasks.get_mut(&id) else {
            warn!(task_id = %id, "Task not found when attempting to spawn worker");
            return;
        };
        // File handle must exist to spawn workers (released when paused)
        if task.file.is_none() {
            warn!(task_id = %id, "Skipping worker spawn: file handle not available");
            return;
        }
        let meta = task.meta.as_ref();
        let fire_once = meta.is_support_ranges().not();
        // Single-worker mode: ensure only one worker
        if fire_once && task.current_concurrency != 0 {
            debug!(task_id = %id, "Task has single worker and already has an active worker");
            return;
        }
        // Single-worker mode: create one worker for entire file
        if fire_once {
            let _ = task.status.send_if_modified(|status| status.state.set_running());
            let file = task.file.clone().expect("File handle must exist when spawning worker");
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(file)
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
            self.workers.push(fut);
            task.current_concurrency += 1;
            debug!(task_id = %id, "Single worker spawned");
            return;
        }
        // Multi-worker mode: calculate additional workers that can be spawned
        let more_concurrency = task.max_concurrency.get().saturating_sub(task.current_concurrency);
        if more_concurrency == 0 {
            debug!(task_id = %id, current_concurrency = %task.current_concurrency, max_concurrency = %task.max_concurrency.get(), "Task reached maximum concurrency");
            return;
        }
        // Update task state to Running
        let _ = task.status.send_if_modified(|status| status.state.set_running());
        // Split remaining ranges into chunks, each handled by one worker
        let remaining = task.remaining.as_mut().expect("Task supports ranges but have no content range");
        let blocks = remaining.into_chunks(config!(http_block_size)).take(more_concurrency as usize);
        for blk in blocks {
            let file = task.file.clone().expect("File handle must exist when spawning worker");
            let fut = Worker::builder()
                .id(id)
                .url(meta.url().clone())
                .file(file)
                .assign(blk.clone())
                .child_cancel(task.cancel.child_token())
                .rate_limit(task.rate_limit.clone())
                .status(task.status.clone())
                .build()
                .spawn();
            task.current_concurrency += 1;
            task.inflight |= &blk;
            self.workers.push(fut);
            debug!(task_id = %id, assigned_range = ?blk, "Multi-part worker spawned for download range");
        }
    }
}
