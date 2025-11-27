use crate::http::worker::WorkerError;
use TaskStateDesc::*;
use camino::Utf8PathBuf;
use falcon_identity::task::TaskId;
use sparse_ranges::RangeSet;
use url::Url;

#[derive(Debug)]
pub struct TaskStatus {
    pub id: TaskId,
    pub name: String,
    pub total: Option<usize>,
    pub buffered: RangeSet, // 缓冲写入量
    pub flushed: RangeSet,  // 刷入量
    pub state: TaskStateDesc,
    pub err: Option<WorkerError>,
    pub url: Url,                  // 重定向成功后记得更新
    pub path: Option<Utf8PathBuf>, // 可能创建文件失败
}

impl TaskStatus {
    pub fn default_with(
        id: TaskId, name: &str, url: Url, path: Option<Utf8PathBuf>, total_size: Option<usize>,
    ) -> Self {
        Self {
            total: total_size,
            buffered: RangeSet::new(),
            flushed: RangeSet::new(),
            state: TaskStateDesc::default(),
            err: None,
            url,
            path,
            id,
            name: name.to_string(),
        }
    }

    pub fn has_err(&self) -> bool { self.err.is_some() }

    pub fn set_err(&mut self, err: WorkerError) { self.err = Some(err); }
}

#[derive(Debug, Default, Clone, Copy)]
pub enum TaskStateDesc {
    #[default]
    Idle, // 空闲，刚创建好但是没有
    Running,   // 运行中
    Paused,    // 已暂停
    Completed, // 正常完成
    Cancelled, // 被取消
    Failed,    // 超过错误计数
}

impl TaskStateDesc {
    #[inline]
    pub fn is_idle(&self) -> bool { matches!(self, Idle) }

    #[inline]
    pub fn is_running(&self) -> bool { matches!(self, Running) }

    #[inline]
    pub fn was_paused(&self) -> bool { matches!(self, Paused) }

    #[inline]
    pub fn was_completed(&self) -> bool { matches!(self, Completed) }

    #[inline]
    pub fn was_cancelled(&self) -> bool { matches!(self, Cancelled) }

    #[inline]
    pub fn was_failed(&self) -> bool { matches!(self, Failed) }

    #[inline]
    pub fn set_idle(&mut self) -> bool {
        if self.is_idle() {
            false
        } else {
            *self = Idle;
            true
        }
    }

    #[inline]
    pub fn set_running(&mut self) -> bool {
        if self.is_running() {
            false
        } else {
            *self = Running;
            true
        }
    }

    #[inline]
    pub fn set_paused(&mut self) -> bool {
        if self.was_paused() {
            false
        } else {
            *self = Paused;
            true
        }
    }

    #[inline]
    pub fn set_completed(&mut self) -> bool {
        if self.was_completed() {
            false
        } else {
            *self = Completed;
            true
        }
    }

    #[inline]
    pub fn set_cancelled(&mut self) -> bool {
        if self.was_cancelled() {
            false
        } else {
            *self = Cancelled;
            true
        }
    }

    #[inline]
    pub fn set_failed(&mut self) -> bool {
        if self.was_failed() {
            false
        } else {
            *self = Failed;
            true
        }
    }
}
