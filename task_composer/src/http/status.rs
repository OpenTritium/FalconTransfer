use crate::http::worker::WorkerError;
use TaskStateDesc::*;
use camino::Utf8PathBuf;
use sparse_ranges::RangeSet;
use url::Url;

#[derive(Debug)]
pub struct TaskStatus {
    pub total: Option<usize>,
    pub downloaded: RangeSet,
    pub state: TaskStateDesc,
    pub err: Option<WorkerError>,
    pub url: Url,
    pub path: Utf8PathBuf,
}

impl TaskStatus {
    pub fn default_with(url: Url, path: Utf8PathBuf, total_size: Option<usize>) -> Self {
        Self { total: total_size, downloaded: RangeSet::new(), state: TaskStateDesc::default(), err: None, url, path }
    }

    pub fn has_err(&self) -> bool { self.err.is_some() }

    pub fn set_err(&mut self, err: WorkerError) { self.err = Some(err); }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum TaskStateDesc {
    #[default]
    Idle, // 空闲，刚创建好但是没有worker 的状态
    Running,   // 运行中
    Paused,    // 已暂停
    Completed, // 正常完成
    Cancelled, // 被取消
    Failed,    //超过错误计数
}

impl TaskStateDesc {
    pub fn is_idle(&self) -> bool { matches!(self, Idle) }

    pub fn is_running(&self) -> bool { matches!(self, Running) }

    pub fn was_paused(&self) -> bool { matches!(self, Paused) }

    pub fn was_completed(&self) -> bool { matches!(self, Completed) }

    pub fn was_cancelled(&self) -> bool { matches!(self, Cancelled) }

    pub fn was_failed(&self) -> bool { matches!(self, Failed) }

    /// 任务完成，取消或失败
    pub fn is_terminal(&self) -> bool { matches!(self, Completed | Cancelled | Failed) }

    pub fn set_idle(&mut self) -> bool {
        if self.is_idle() {
            false
        } else {
            *self = Idle;
            true
        }
    }

    pub fn set_running(&mut self) -> bool {
        if self.is_running() {
            false
        } else {
            *self = Running;
            true
        }
    }

    pub fn set_pending(&mut self) -> bool {
        if self.was_paused() {
            false
        } else {
            *self = Paused;
            true
        }
    }

    pub fn set_finished(&mut self) -> bool {
        if self.was_completed() {
            false
        } else {
            *self = Completed;
            true
        }
    }

    pub fn set_cancelled(&mut self) -> bool {
        if self.was_cancelled() {
            false
        } else {
            *self = Cancelled;
            true
        }
    }

    pub fn set_failed(&mut self) -> bool {
        if self.was_failed() {
            false
        } else {
            *self = Failed;
            true
        }
    }
}
