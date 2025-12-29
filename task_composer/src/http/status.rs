use crate::http::worker::WorkerError;
use TaskStateDesc::*;
use camino::Utf8PathBuf;
use falcon_identity::task::TaskId;
use serde::{Deserialize, Serialize};
use sparse_ranges::RangeSet;
use std::fmt;
use typed_builder::TypedBuilder;
use ubyte::ByteUnit;
use url::Url;

macro_rules! impl_state_check {
    ($($method:ident, $state:ident);+ $(;)?) => {
        $(
            #[inline]
            pub const fn $method(&self) -> bool { matches!(self, $state) }
        )+
    };
}

macro_rules! impl_state_setter {
    ($($method:ident, $state:ident, $check:ident);+ $(;)?) => {
        $(
            #[inline]
            pub const fn $method(&mut self) -> bool {
                if self.$check() {
                    false
                } else {
                    *self = $state;
                    true
                }
            }
        )+
    };
}

/// Task status tracking download progress and state.
#[derive(Debug, TypedBuilder)]
pub struct TaskStatus {
    #[builder(default)]
    pub id: TaskId,
    #[builder(setter(into))]
    pub name: String,
    /// Total file size in bytes
    #[builder(default)]
    pub total: Option<usize>,
    /// Buffered write ranges
    #[builder(default)]
    pub buffered: RangeSet,
    /// Flushed ranges to disk
    #[builder(default)]
    pub flushed: RangeSet,
    #[builder(default)]
    pub state: TaskStateDesc,
    #[builder(default)]
    pub err: Option<WorkerError>,
    /// Download URL (update after redirect)
    pub url: Url,
    /// Local file path (None if file creation failed)
    #[builder(default)]
    pub path: Option<Utf8PathBuf>,
}

impl TaskStatus {
    /// Checks if task has an error.
    #[inline]
    pub const fn has_err(&self) -> bool { self.err.is_some() }

    /// Sets error for this task.
    #[inline]
    pub fn set_err(&mut self, err: WorkerError) { self.err = Some(err); }
}

/// Task state description.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskStateDesc {
    #[default]
    Idle,
    Running,
    Paused,
    Completed,
    Cancelled,
    Failed,
}

impl TaskStateDesc {
    impl_state_check! {
        is_idle, Idle;
        is_running, Running;
        was_paused, Paused;
        was_completed, Completed;
        was_cancelled, Cancelled;
        was_failed, Failed;
    }

    impl_state_setter! {
        set_idle, Idle, is_idle;
        set_running, Running, is_running;
        set_paused, Paused, was_paused;
        set_completed, Completed, was_completed;
        set_cancelled, Cancelled, was_cancelled;
        set_failed, Failed, was_failed;
    }
}

impl fmt::Display for TaskStateDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Idle => write!(f, "Idle"),
            Running => write!(f, "Running"),
            Paused => write!(f, "Paused"),
            Completed => write!(f, "Completed"),
            Cancelled => write!(f, "Cancelled"),
            Failed => write!(f, "Failed"),
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
        let mut status = TaskStatus::builder()
            .id(TaskId::new())
            .name("ubuntu.iso")
            .url(Url::parse("https://example.com/ubuntu.iso").unwrap())
            .path(Some("/downloads/ubuntu.iso".into()))
            .total(Some(6_000_000_000)) // 6 GB
            .build();

        status.buffered.insert_n_at(1_500_000_001, 0); // 1.5 GB buffered
        status.flushed.insert_n_at(1_000_000_001, 0); // 1 GB flushed
        status.state.set_running();

        let output = format!("{}", status);

        // 验证输出包含关键信息
        assert!(output.contains("ubuntu.iso"), "应该包含文件名");
        assert!(output.contains("Running"), "应该显示运行状态");
        assert!(output.contains("1.00GB"), "应该显示已刷新的字节数");
        assert!(output.contains("6GB"), "应该显示总大小");
        assert!(output.contains("16.7%"), "应该显示百分比");
        assert!(output.contains("buffered: +500MB"), "应该显示未刷新的缓冲数据");
        assert!(output.contains("Path: /downloads/ubuntu.iso"), "应该显示路径");
    }

    #[test]
    fn test_task_status_display_completed() {
        let mut status = TaskStatus::builder()
            .id(TaskId::new())
            .name("file.zip")
            .url(Url::parse("https://example.com/file.zip").unwrap())
            .path(Some("/tmp/file.zip".into()))
            .total(Some(1_000_000_000)) // 1 GB
            .build();

        status.buffered.insert_n_at(1_000_000_000, 0);
        status.flushed.insert_n_at(1_000_000_000, 0);
        status.state.set_completed();

        let output = format!("{}", status);

        assert!(output.contains("file.zip"));
        assert!(output.contains("Completed"));
        assert!(output.contains("1GB/1GB"));
        assert!(output.contains("100.0%"));
        assert!(!output.contains("buffered:"), "完成时不应该有未刷新的缓冲");
    }

    #[test]
    fn test_task_status_display_failed_with_error() {
        let mut status = TaskStatus::builder()
            .id(TaskId::new())
            .name("failed.dat")
            .url(Url::parse("https://example.com/failed.dat").unwrap())
            .path(Some("/tmp/failed.dat".into()))
            .total(Some(1_000_000)) // 1 MB
            .build();

        status.state.set_failed();
        status.set_err(WorkerError::ParitalDownloaded(RangeSet::new()));

        let output = format!("{}", status);

        assert!(output.contains("failed.dat"));
        assert!(output.contains("Failed"));
        assert!(output.contains("Error:"), "应该显示错误信息");
    }

    #[test]
    fn test_task_status_display_without_total_size() {
        let mut status = TaskStatus::builder()
            .id(TaskId::new())
            .name("streaming-data")
            .url(Url::parse("https://example.com/stream").unwrap())
            .build();

        status.buffered.insert_n_at(10_485_761, 0); // 10 MB
        status.flushed.insert_n_at(8_388_609, 0); // 8 MB
        status.state.set_running();

        let output = format!("{}", status);

        assert!(output.contains("streaming-data"));
        assert!(output.contains("Running"));
        assert!(output.contains("flushed: 8.00MiB"), "没有总大小时应该显示 flushed");
        assert!(output.contains("buffered: +2MiB"));
        assert!(!output.contains("Path:"), "没有路径不应该显示 Path");
    }

    #[test]
    fn test_task_status_display_paused() {
        let mut status = TaskStatus::builder()
            .id(TaskId::new())
            .name("large-file.zip")
            .url(Url::parse("https://example.com/large-file.zip").unwrap())
            .path(Some("/tmp/large-file.zip".into()))
            .total(Some(500_000_000)) // 500 MB
            .build();

        status.buffered.insert_n_at(100_000_001, 0);
        status.flushed.insert_n_at(100_000_001, 0);
        status.state.set_paused();

        let output = format!("{}", status);

        assert!(output.contains("large-file.zip"));
        assert!(output.contains("Paused"));
        assert!(output.contains("100.00MB/500MB"));
        assert!(output.contains("20.0%"));
    }

    #[test]
    fn test_task_state_display() {
        assert_eq!(format!("{}", TaskStateDesc::Idle), "Idle");
        assert_eq!(format!("{}", TaskStateDesc::Running), "Running");
        assert_eq!(format!("{}", TaskStateDesc::Paused), "Paused");
        assert_eq!(format!("{}", TaskStateDesc::Completed), "Completed");
        assert_eq!(format!("{}", TaskStateDesc::Cancelled), "Cancelled");
        assert_eq!(format!("{}", TaskStateDesc::Failed), "Failed");
    }
}
