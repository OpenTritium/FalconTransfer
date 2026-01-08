use crate::task_info::{TaskInfo, TaskState};
use falcon_identity::task::TaskId;
use falcon_task_composer::TaskStatus;
use futures_util::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use see::sync::Receiver;
use std::{collections::HashMap, future::pending};
use tracing::debug;

#[derive(Debug, Clone)]
pub enum WatcherEvent {
    Updated(TaskInfo),
    Removed(TaskId),
}

#[derive(Debug)]
pub struct WatchGroup {
    watchers: HashMap<TaskId, Receiver<TaskStatus>>,
    pendings: FuturesUnordered<LocalBoxFuture<'static, WatcherEvent>>,
}

impl FromIterator<(TaskId, Receiver<TaskStatus>)> for WatchGroup {
    fn from_iter<T: IntoIterator<Item = (TaskId, Receiver<TaskStatus>)>>(iter: T) -> Self {
        let watchers = iter.into_iter().collect();
        Self { watchers, pendings: FuturesUnordered::new() }
    }
}

impl WatchGroup {
    pub fn new() -> Self { Self { watchers: HashMap::new(), pendings: FuturesUnordered::new() } }

    pub async fn snapshot_all(&mut self) -> Box<[TaskInfo]> {
        self.watchers.iter_mut().map(|(_, rx)| map_status_to_info(rx.borrow_and_update().as_ref())).collect()
    }

    #[inline]
    pub fn mark_unchanged(&mut self, id: TaskId) {
        if let Some(rx) = self.watchers.get_mut(&id) {
            rx.mark_unchanged();
        } else {
            debug!(%id, "Attempted to mark non-existent watcher as unchanged");
        }
    }

    #[inline]
    pub fn watch(&mut self, id: TaskId) {
        let Some(rx) = self.watchers.get(&id) else {
            tracing::warn!(%id, "Attempted to watch non-existent task");
            return;
        };
        let mut rx = rx.clone();
        tracing::info!(%id, pendings_count = self.pendings.len(), "Creating new watcher future");
        let fut = async move {
            tracing::info!(%id, "Waiting for rx.changed()...");
            match rx.changed().await {
                Ok(_) => {
                    let status = rx.borrow_and_update();
                    tracing::info!(%id, "rx.changed() completed, status updated");
                    WatcherEvent::Updated(map_status_to_info(status.as_ref()))
                }
                Err(_) => {
                    tracing::error!(%id, "rx.changed() FAILED - channel closed unexpectedly!");
                    WatcherEvent::Removed(id)
                }
            }
        };
        tracing::info!(%id, "About to call boxed_local()");
        let boxed = fut.boxed_local();
        tracing::info!(%id, "boxed_local() completed, about to push");
        self.pendings.push(boxed);
        tracing::info!(pendings_count = self.pendings.len(), "Future pushed to pendings");
    }

    #[inline]
    pub fn acknowledge_and_rewatch(&mut self, id: TaskId) {
        self.mark_unchanged(id);
        self.watch(id);
    }

    pub async fn next(&mut self) -> WatcherEvent {
        tracing::info!(pendings_count = self.pendings.len(), "Waiting for next watcher event");
        let event = if let Some(status) = self.pendings.next().await {
            tracing::info!(pendings_count = self.pendings.len(), "Got watcher event");
            status
        } else {
            tracing::error!("No pending futures, will wait forever (pending())");
            pending().await
        };
        tracing::info!("next() returning event");
        event
    }

    #[inline]
    pub fn push(&mut self, rx: Receiver<TaskStatus>) {
        let id = rx.borrow().id;
        tracing::info!(%id, "Adding new watcher to group");
        self.watchers.insert(id, rx);
        self.watch(id);
    }

    #[inline]
    pub fn remove(&mut self, id: TaskId) -> Option<Receiver<TaskStatus>> { self.watchers.remove(&id) }
}

#[inline]
fn map_status_to_info(status: &TaskStatus) -> TaskInfo {
    use falcon_task_composer::TaskStateDesc::*;
    let TaskStatus { id, total, buffered, state, url, path, err, name, .. } = status;
    let downloaded = buffered.len();
    let state = match state {
        Idle => TaskState::Idle,
        Running => TaskState::Running { downloaded },
        Paused => TaskState::Paused { downloaded },
        Completed => TaskState::Completed,
        Cancelled => TaskState::Cancelled,
        Failed => TaskState::Failed {
            last_error: err.as_ref().map(|err| err.to_string()).unwrap_or_else(|| "Unknown error".to_string()),
            downloaded,
        },
    };
    TaskInfo {
        id: *id,
        name: name.clone(),
        size: *total,
        state,
        url: url.clone(),
        path: path.as_ref().map(|p| p.to_string()),
    }
}
