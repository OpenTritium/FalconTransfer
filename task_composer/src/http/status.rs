use crate::http::worker::WorkerError;
use TaskStateDesc::*;
use camino::Utf8PathBuf;
use falcon_identity::task::TaskId;
use sparse_ranges::RangeSet;
use std::fmt;
use ubyte::ByteUnit;
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

impl fmt::Display for TaskStateDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Idle => write!(f, "🔵 Idle"),
            Running => write!(f, "🟢 Running"),
            Paused => write!(f, "🟡 Paused"),
            Completed => write!(f, "✅ Completed"),
            Cancelled => write!(f, "⚪ Cancelled"),
            Failed => write!(f, "❌ Failed"),
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let flushed_bytes = self.flushed.len();
        let buffered_bytes = self.buffered.len();
        write!(f, "Task[{}] '{}' - {}", self.id, self.name, self.state)?;
        if let Some(total) = self.total {
            let percentage = if total > 0 {
                (flushed_bytes as f64 / total as f64 * 100.0).min(100.0)
            } else {
                100.0
            };
            write!(
                f,
                " | {}/{} ({:.1}%)",
                ByteUnit::Byte(flushed_bytes as u64),
                ByteUnit::Byte(total as u64),
                percentage
            )?;
        } else {
            write!(f, " | flushed: {}", ByteUnit::Byte(flushed_bytes as u64))?;
        }
        if buffered_bytes > flushed_bytes {
            write!(f, ", buffered: +{}", ByteUnit::Byte((buffered_bytes - flushed_bytes) as u64))?;
        }
        if let Some(path) = &self.path {
            write!(f, "\n  Path: {}", path)?;
        }
        if let Some(err) = &self.err {
            write!(f, "\n  Error: {}", err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_status_display_running_with_progress() {
        // 测试运行中的任务，带进度和缓冲
        let mut status = TaskStatus::default_with(
            TaskId::new(),
            "ubuntu.iso",
            Url::parse("https://example.com/ubuntu.iso").unwrap(),
            Some("/downloads/ubuntu.iso".into()),
            Some(6_000_000_000), // 6 GB
        );

        status.buffered.insert_n_at(1_500_000_001, 0); // 1.5 GB buffered
        status.flushed.insert_n_at(1_000_000_001, 0); // 1 GB flushed
        status.state.set_running();

        let output = format!("{}", status);

        // 验证输出包含关键信息
        assert!(output.contains("ubuntu.iso"), "应该包含文件名");
        assert!(output.contains("🟢 Running"), "应该显示运行状态");
        assert!(output.contains("1.00GB"), "应该显示已刷新的字节数");
        assert!(output.contains("6GB"), "应该显示总大小");
        assert!(output.contains("16.7%"), "应该显示百分比");
        assert!(output.contains("buffered: +500MB"), "应该显示未刷新的缓冲数据");
        assert!(output.contains("Path: /downloads/ubuntu.iso"), "应该显示路径");
    }

    #[test]
    fn test_task_status_display_completed() {
        // 测试完成状态
        let mut status = TaskStatus::default_with(
            TaskId::new(),
            "file.zip",
            Url::parse("https://example.com/file.zip").unwrap(),
            Some("/tmp/file.zip".into()),
            Some(1_000_000_000), // 1 GB
        );

        status.buffered.insert_n_at(1_000_000_000, 0);
        status.flushed.insert_n_at(1_000_000_000, 0);
        status.state.set_completed();

        let output = format!("{}", status);

        assert!(output.contains("file.zip"));
        assert!(output.contains("✅ Completed"));
        assert!(output.contains("1GB/1GB"));
        assert!(output.contains("100.0%"));
        assert!(!output.contains("buffered:"), "完成时不应该有未刷新的缓冲");
    }

    #[test]
    fn test_task_status_display_failed_with_error() {
        // 测试失败状态带错误信息
        let mut status = TaskStatus::default_with(
            TaskId::new(),
            "failed.dat",
            Url::parse("https://example.com/failed.dat").unwrap(),
            Some("/tmp/failed.dat".into()),
            Some(1_000_000),
        );

        status.state.set_failed();
        status.set_err(WorkerError::ParitalDownloaded(RangeSet::new()));

        let output = format!("{}", status);

        assert!(output.contains("failed.dat"));
        assert!(output.contains("❌ Failed"));
        assert!(output.contains("Error:"), "应该显示错误信息");
    }

    #[test]
    fn test_task_status_display_without_total_size() {
        // 测试没有总大小的流式下载
        let mut status = TaskStatus::default_with(
            TaskId::new(),
            "streaming-data",
            Url::parse("https://example.com/stream").unwrap(),
            None,
            None,
        );

        status.buffered.insert_n_at(10_485_761, 0); // 10 MB
        status.flushed.insert_n_at(8_388_609, 0); // 8 MB
        status.state.set_running();

        let output = format!("{}", status);

        assert!(output.contains("streaming-data"));
        assert!(output.contains("🟢 Running"));
        assert!(output.contains("flushed: 8.00MiB"), "没有总大小时应该显示 flushed");
        assert!(output.contains("buffered: +2MiB"));
        assert!(!output.contains("Path:"), "没有路径不应该显示 Path");
    }

    #[test]
    fn test_task_status_display_paused() {
        // 测试暂停状态
        let mut status = TaskStatus::default_with(
            TaskId::new(),
            "large-file.zip",
            Url::parse("https://example.com/large-file.zip").unwrap(),
            Some("/tmp/large-file.zip".into()),
            Some(500_000_000),
        );

        status.buffered.insert_n_at(100_000_001, 0);
        status.flushed.insert_n_at(100_000_001, 0);
        status.state.set_paused();

        let output = format!("{}", status);

        assert!(output.contains("large-file.zip"));
        assert!(output.contains("🟡 Paused"));
        assert!(output.contains("100.00MB/500MB"));
        assert!(output.contains("20.0%"));
    }

    #[test]
    fn test_task_state_display() {
        assert_eq!(format!("{}", TaskStateDesc::Idle), "🔵 Idle");
        assert_eq!(format!("{}", TaskStateDesc::Running), "🟢 Running");
        assert_eq!(format!("{}", TaskStateDesc::Paused), "🟡 Paused");
        assert_eq!(format!("{}", TaskStateDesc::Completed), "✅ Completed");
        assert_eq!(format!("{}", TaskStateDesc::Cancelled), "⚪ Cancelled");
        assert_eq!(format!("{}", TaskStateDesc::Failed), "❌ Failed");
    }
}
