use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
static GLOBAL_TASK_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new() -> Self { Self(GLOBAL_TASK_ID.fetch_add(1, Relaxed)) }
}

impl Default for TaskId {
    fn default() -> Self { Self::new() }
}
