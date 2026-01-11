use falcon_identity::task::TaskId;
use falcon_task_composer::TaskStatus;
use serde::Serialize;
use url::Url;

#[derive(Serialize, Debug, Clone)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum TaskState {
    Idle,
    Running { downloaded: usize },
    Paused { downloaded: usize },
    Completed,
    Cancelled,
    Failed { last_error: String, downloaded: usize },
}

#[derive(Serialize, Debug, Clone)]
pub struct TaskInfo {
    pub id: TaskId,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,
    #[serde(flatten)]
    pub state: TaskState,
    pub url: Url,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl From<TaskStatus> for TaskInfo {
    #[inline]
    fn from(status: TaskStatus) -> Self {
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
        Self { id, name, size: total, state, url, path: path.as_ref().map(|p| p.to_string()) }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct NativePayload(pub Box<[TaskInfo]>);
