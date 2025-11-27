use falcon_identity::task::TaskId;
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

#[derive(Debug, Serialize, Clone)]
pub struct NativePayload(pub Box<[TaskInfo]>);
