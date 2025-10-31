use crate::http::{meta::HttpTaskMeta, status::TaskStatus};
use identity::task::TaskId;
use see::sync as watch;
use std::{
    fmt::Debug,
    num::{NonZeroU8, NonZeroU32},
};

#[derive(Debug, Clone)]
pub enum TaskCommand {
    ChangeConcurrency { id: TaskId, concuerrency: NonZeroU8 },
    ChangeRateLimited { id: TaskId, limit: Option<NonZeroU32> },
    Pause(TaskId),
    Resume(TaskId),
    Cancel(TaskId), //取消但暂时不移除任务,因为它会drop 观测器,导致外部无法获悉状态
    Create { meta: Box<HttpTaskMeta>, watch: Box<watch::Sender<TaskStatus>> },
    Remove(TaskId), // 我已经观测到取消,准许移除任务
}
