use crate::{
    port::NativePort,
    task_info::{NativePayload, TaskInfo, TaskState},
};
use falcon_identity::task::TaskId;
use falcon_task_composer::TaskStatus;
use futures_util::{FutureExt, StreamExt, future::LocalBoxFuture, stream::FuturesUnordered};
use see::sync::Receiver;
use std::{collections::HashMap, future::pending};
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
pub enum WatcherStatus {
    Updated(TaskInfo),
    Removed(TaskId),
}

pub struct WatchGroup {
    watchers: HashMap<TaskId, Receiver<TaskStatus>>,
    pendings: FuturesUnordered<LocalBoxFuture<'static, WatcherStatus>>,
}

impl WatchGroup {
    #[inline]
    pub fn new() -> Self { Self { watchers: HashMap::new(), pendings: FuturesUnordered::new() } }

    pub async fn snapshot_all(&mut self) -> Box<[TaskInfo]> {
        self.watchers.iter_mut().map(|(_, rx)| map_status_to_info(rx.borrow_and_update().as_ref())).collect()
    }

    #[inline]
    pub fn mark_unchanged(&mut self, id: TaskId) {
        if let Some(rx) = self.watchers.get_mut(&id) {
            rx.mark_unchanged();
        } else {
            warn!("Attempt to mark unchanged a non-existent watcher");
        }
    }

    #[inline]
    pub fn watch(&self, id: TaskId) {
        let Some(mut rx) = self.watchers.get(&id).cloned() else {
            warn!("Attempt to watch a task that does not exist");
            return;
        };
        let fut = async move {
            if rx.changed().await.is_ok() {
                let status = rx.borrow_and_update();
                WatcherStatus::Updated(map_status_to_info(status.as_ref()))
            } else {
                WatcherStatus::Removed(id)
            }
        };
        self.pendings.push(fut.boxed_local());
    }

    #[inline]
    pub fn remove(&mut self, id: TaskId) { self.watchers.remove(&id); }

    pub async fn next(&mut self) -> WatcherStatus {
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
    let TaskStatus { id, total, buffered: downloaded, state, url, path, err, name, .. } = status;
    let downloaded = downloaded.len();
    let state = match state {
        Idle => TaskState::Idle,
        Running => TaskState::Running { downloaded },
        Paused => TaskState::Paused { downloaded },
        Completed => TaskState::Completed,
        Cancelled => TaskState::Cancelled,
        Failed => TaskState::Failed {
            last_error: err
                .as_ref()
                .map(|err| err.to_string())
                .unwrap_or_else(|| "an error should be palced here".to_string()),
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

pub async fn handle_watch_status(status: WatcherStatus, watchers: &mut WatchGroup, port: &mut NativePort) {
    match status {
        WatcherStatus::Updated(task) => {
            let id = task.id;
            let res = port
                .send(NativePayload(Box::new([task])))
                .await
                .inspect_err(|err| error!("Failed to send updated task info to native port: {err:?}"));
            if res.is_ok() {
                info!("Task info has been updated, continue watching");
                watchers.mark_unchanged(id);
                watchers.watch(id);
            }
        }
        WatcherStatus::Removed(task_id) => {
            watchers.remove(task_id);
            info!("Task {task_id:?} removed, it's watchers has been removed too.");
        }
    }
}
