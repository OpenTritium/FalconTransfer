use crate::http::{meta::HttpTaskMeta, status::TaskStatus};
use falcon_identity::task::TaskId;
use see::sync as watch;
use std::{
    fmt::{self, Debug, Display},
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

impl Display for TaskCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TaskCommand::*;
        match self {
            ChangeConcurrency { id, concurrency } => {
                write!(f, "ChangeConcurrency[{}] -> {}", id, concurrency)
            }
            ChangeRateLimited { id, limit } => {
                if let Some(limit) = limit {
                    write!(f, "ChangeRateLimited[{}] -> {} bytes/s", id, limit)
                } else {
                    write!(f, "ChangeRateLimited[{}] -> unlimited", id)
                }
            }
            Pause(id) => write!(f, "Pause[{}]", id),
            Resume(id) => write!(f, "Resume[{}]", id),
            Cancel(id) => write!(f, "Cancel[{}]", id),
            Create { meta, .. } => {
                write!(f, "Create['{}']", meta.name())
            }
            Remove(id) => write!(f, "Remove[{}]", id),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_command_display() {
        let id = TaskId::new();

        // Test Pause
        let cmd = TaskCommand::Pause(id);
        println!("Pause: {}", cmd);
        assert!(format!("{}", cmd).contains("Pause"));

        // Test Resume
        let cmd = TaskCommand::Resume(id);
        println!("Resume: {}", cmd);
        assert!(format!("{}", cmd).contains("Resume"));

        // Test Cancel
        let cmd = TaskCommand::Cancel(id);
        println!("Cancel: {}", cmd);
        assert!(format!("{}", cmd).contains("Cancel"));

        // Test Remove
        let cmd = TaskCommand::Remove(id);
        println!("Remove: {}", cmd);
        assert!(format!("{}", cmd).contains("Remove"));

        // Test ChangeConcurrency
        let cmd = TaskCommand::ChangeConcurrency { id, concurrency: NonZeroU8::new(8).unwrap() };
        println!("ChangeConcurrency: {}", cmd);
        assert!(format!("{}", cmd).contains("ChangeConcurrency"));
        assert!(format!("{}", cmd).contains("8"));

        // Test ChangeRateLimited with limit
        let cmd = TaskCommand::ChangeRateLimited { id, limit: Some(NonZeroU32::new(1048576).unwrap()) };
        println!("ChangeRateLimited (limited): {}", cmd);
        assert!(format!("{}", cmd).contains("ChangeRateLimited"));
        assert!(format!("{}", cmd).contains("1048576"));

        // Test ChangeRateLimited without limit
        let cmd = TaskCommand::ChangeRateLimited { id, limit: None };
        println!("ChangeRateLimited (unlimited): {}", cmd);
        assert!(format!("{}", cmd).contains("unlimited"));
    }
}
