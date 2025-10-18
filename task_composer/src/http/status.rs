use crate::http::{file_range::FileMultiRange, worker::WorkerError};

#[derive(Debug, Default)]
pub struct TaskStatus {
    pub total: FileMultiRange, // 用于展示下载总量，当目标大小未知时，与已下载量同步
    pub downloaded: FileMultiRange,
    pub state: TaskState,
}

impl TaskStatus {
    pub fn new(total: FileMultiRange, downloaded: FileMultiRange, state: TaskState) -> Self {
        Self { total, downloaded, state }
    }
}

#[derive(Debug, Default)]
pub enum TaskState {
    #[default]
    Idle, // 空闲，刚创建好但是没有worker 的状态
    Running,             // 运行中
    Pending,             // 已暂停/等待中
    Finished,            // 正常完成 (对应 eof)
    Failed(WorkerError), // 出错了 (对应 last_err 和 eof)
}

impl TaskState {
    pub fn is_idle(&self) -> bool { matches!(self, TaskState::Idle) }

    pub fn is_running(&self) -> bool { matches!(self, TaskState::Running) }

    pub fn is_pending(&self) -> bool { matches!(self, TaskState::Pending) }

    pub fn is_finished(&self) -> bool { matches!(self, TaskState::Finished) }

    pub fn is_failed(&self) -> bool { matches!(self, TaskState::Failed(_)) }

    /// 状态是否停止（中断，完成，失败）
    pub fn is_stopped(&self) -> bool { self.is_finished() || self.is_failed() }

    pub fn set_idle(&mut self) -> bool {
        if self.is_idle() {
            false
        } else {
            *self = TaskState::Idle;
            true
        }
    }

    pub fn set_running(&mut self) -> bool {
        if self.is_running() {
            false
        } else {
            *self = TaskState::Running;
            true
        }
    }

    pub fn set_pending(&mut self) -> bool {
        if self.is_pending() {
            false
        } else {
            *self = TaskState::Pending;
            true
        }
    }

    pub fn set_finished(&mut self) -> bool {
        if self.is_finished() {
            false
        } else {
            *self = TaskState::Finished;
            true
        }
    }

    pub fn set_failed(&mut self, err: WorkerError) -> bool {
        *self = TaskState::Failed(err);
        true
    }
}
