use crate::{
    HttpTaskMeta, TaskCommand, TaskDispatcher, TaskState, TaskStateDesc, TaskStatus, WorkerError, http::dispatcher,
};
use camino::Utf8PathBuf;
use compio::fs::OpenOptions;
use falcon_identity::task::{TaskId, reset_task_id_generator};
use falcon_persist::{impl_persist, store::PersistStore};
use flume as mpmc;
use see::sync as watch;
use serde::{Deserialize, Serialize};
use sparse_ranges::RangeSet;
use std::collections::{HashMap, HashSet, hash_map};
use tracing::error;
use typed_builder::TypedBuilder;

/// Persisted task state for recovery across restarts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, TypedBuilder)]
pub struct PersistTaskState {
    /// Task metadata containing URL and address
    pub meta: HttpTaskMeta,
    /// Remaining undownloaded ranges
    pub remaining: Option<RangeSet>,
    /// Committed ranges to disk
    pub committed: RangeSet,
    /// Task state indicating error before shutdown (errors not serializable)
    pub state: TaskStateDesc,
    /// File write path
    pub path: Option<Utf8PathBuf>,
}

/// Collection of persisted task states indexed by task ID.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistTaskStates(pub HashMap<TaskId, PersistTaskState>);

impl IntoIterator for PersistTaskStates {
    type IntoIter = hash_map::IntoIter<TaskId, PersistTaskState>;
    type Item = (TaskId, PersistTaskState);

    #[inline]
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

impl FromIterator<(TaskId, PersistTaskState)> for PersistTaskStates {
    #[inline]
    fn from_iter<T: IntoIterator<Item = (TaskId, PersistTaskState)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl TaskState {
    /// Creates task state from persisted state.
    async fn from_persisted(id: TaskId, persisted_task: PersistTaskState) -> Result<Self, dispatcher::Error> {
        let PersistTaskState { meta, remaining, committed, state, path } = persisted_task;
        // Only restore file handle for running tasks
        let file = if state.is_running() {
            let path = path.as_ref().ok_or(dispatcher::Error::NoPathWhenRestoreRunnigTask)?;
            Some(OpenOptions::new().read(true).write(true).open(path).await?)
        } else {
            None
        };
        let err = state.was_failed().then_some(WorkerError::PreviousUnlogged);
        let total_size = meta.full_content_range().as_ref().and_then(|s| s.last()).map(|n| n + 1);
        // Display status starting from committed ranges
        let status = TaskStatus::builder()
            .id(id)
            .buffered(committed.clone())
            .flushed(committed.clone())
            .name(meta.name())
            .path(path)
            .state(state)
            .url(meta.url().clone())
            .total(total_size)
            .err(err)
            .build();
        let status = watch::Sender::new(status);
        let task =
            Self::builder().meta(meta).remaining(remaining).committed(committed).file(file).status(status).build();
        Ok(task)
    }
}

impl TaskDispatcher {
    /// Restores dispatcher from persistent store.
    pub async fn from_store_cmd(
        store: &PersistStore, cmd: &mpmc::Receiver<TaskCommand>,
    ) -> Result<Self, dispatcher::Error> {
        let tasks: PersistTaskStates = store.load()?.ok_or(dispatcher::Error::PersistEmpty)?;
        let pendings: PersistTaskPendings = store.load()?.ok_or(dispatcher::Error::PersistEmpty)?;
        let dispatcher = Self::from_persisted(tasks, pendings, cmd.clone()).await?;
        Ok(dispatcher)
    }

    /// Constructs dispatcher from persisted state and command channel.
    ///
    /// Restores all tasks and resets ID generator to avoid conflicts.
    async fn from_persisted(
        persisted_tasks: PersistTaskStates, persisted_pendings: PersistTaskPendings, cmd: mpmc::Receiver<TaskCommand>,
    ) -> Result<Self, dispatcher::Error> {
        let mut tasks = HashMap::<TaskId, TaskState>::new();
        for (id, ptask) in persisted_tasks.into_iter() {
            match TaskState::from_persisted(id, ptask).await {
                Ok(task) => {
                    tasks.insert(id, task);
                }
                Err(err) => {
                    error!(task_id = %id, error = %err, "Failed to restore task from persisted state");
                }
            }
        }
        // Reset ID generator to avoid conflicts with existing tasks.
        let max_id = tasks.keys().copied().max();
        if let Some(max_id) = max_id {
            reset_task_id_generator(max_id.inner() + 1);
        }
        let mut dispatcher = Self::builder().cmd(cmd).pendings(persisted_pendings.0).tasks(tasks).build();
        // Auto-resume running tasks that are not in pendings
        dispatcher.restore_running_tasks();
        Ok(dispatcher)
    }
}

/// Set of pending task IDs awaiting processing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistTaskPendings(pub HashSet<TaskId>);

impl IntoIterator for PersistTaskPendings {
    type Item = (TaskId, ());

    type IntoIter = impl Iterator<Item = Self::Item>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter().map(|id| (id, ())) }
}

impl FromIterator<(TaskId, ())> for PersistTaskPendings {
    #[inline]
    fn from_iter<T: IntoIterator<Item = (TaskId, ())>>(iter: T) -> Self {
        Self(iter.into_iter().map(|(id, _)| id).collect())
    }
}

impl_persist!(PersistTaskStates, "task_states");
impl_persist!(PersistTaskPendings, "task_pendings");
