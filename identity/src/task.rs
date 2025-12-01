use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

static GLOBAL_TASK_ID: AtomicU64 = AtomicU64::new(0);

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct TaskId(u64);

impl TaskId {
    pub fn new() -> Self { Self(GLOBAL_TASK_ID.fetch_add(1, Relaxed)) }
}

impl Default for TaskId {
    fn default() -> Self { Self::new() }
}

impl Debug for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{}", self.0) }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "#{}", self.0) }
}
