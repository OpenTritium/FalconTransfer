use crate::http::{meta::HttpTaskMeta, status::TaskStatus};
use falcon_identity::task::TaskId;
use see::sync as watch;
use std::{
    fmt::Debug,
    num::{NonZeroU8, NonZeroU32},
};

#[derive(Debug, Clone)]
pub enum TaskCommand {
    ChangeConcurrency { id: TaskId, concurrency: NonZeroU8 },
    ChangeRateLimited { id: TaskId, limit: Option<NonZeroU32> },
    Pause(TaskId),
    Resume(TaskId),
    Cancel(TaskId), // 取消但不移除任务，这是为了确保外部观测到了任务真的被取消了
    Create { meta: Box<HttpTaskMeta>, watch: Box<watch::Sender<TaskStatus>> },
    Remove(TaskId), // 语义是外部已经观测到变更，请移除该任务
}
