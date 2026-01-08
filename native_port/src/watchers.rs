use crate::{
    port::NativePortWriter,
    task_info::{NativePayload, TaskInfo, TaskState},
};
use falcon_identity::task::TaskId;
use falcon_task_composer::TaskStatus;
use futures_util::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use see::sync::Receiver;
use std::{collections::HashMap, future::pending};
use tracing::{debug, error, info, instrument};

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
    pub fn watch(&self, id: TaskId) {
        let Some(mut rx) = self.watchers.get(&id).cloned() else {
            debug!(%id, "Attempted to watch non-existent task");
            return;
        };
        let fut = async move {
            if rx.changed().await.is_ok() {
                let status = rx.borrow_and_update();
                WatcherEvent::Updated(map_status_to_info(status.as_ref()))
            } else {
                WatcherEvent::Removed(id)
            }
        };
        self.pendings.push(fut.boxed_local());
    }

    #[inline]
    pub fn remove(&mut self, id: TaskId) { self.watchers.remove(&id); }

    pub async fn next(&mut self) -> WatcherEvent {
        if let Some(status) = self.pendings.next().await {
            status
        } else {
            pending().await
        }
    }

    #[inline]
    pub fn push(&mut self, rx: Receiver<TaskStatus>) {
        let id = rx.borrow().id;
        self.watchers.insert(id, rx);
        self.watch(id);
    }
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

#[instrument(skip(port))]
#[must_use]
pub async fn handle_watch_event(status: WatcherEvent, watchers: &mut WatchGroup, port: &mut NativePortWriter) -> bool {
    match status {
        WatcherEvent::Updated(task) => {
            let id = task.id;
            let res = port
                .send(NativePayload(Box::new([task])))
                .await
                .inspect_err(|err| error!(error = %err, %id, "Failed to send task update to native port"));
            if res.is_ok() {
                debug!(%id, "Task info update sent successfully");
                watchers.mark_unchanged(id);
                watchers.watch(id);
                true
            } else {
                false
            }
        }
        WatcherEvent::Removed(task_id) => {
            info!(%task_id, "Task removed, cleaning up watcher");
            watchers.remove(task_id);
            true
        }
    }
}
