use crate::http::{meta::HttpTaskMeta, status::TaskStatus};
use compio_watch as watch;
use identity::task::TaskId;
use std::num::{NonZeroU8, NonZeroU32};

#[derive(Debug, Clone)]
pub enum TaskCommand {
    ChangeConcurrency { id: TaskId, concuerrency: NonZeroU8 },
    ChangeRateLimited { id: TaskId, limit: Option<NonZeroU32> },
    Pause(TaskId),
    Resume(TaskId),
    Cancel(TaskId),
    Create { meta: HttpTaskMeta, watch: watch::Sender<TaskStatus> },
}
