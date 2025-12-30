use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    sync::atomic::{AtomicU64, Ordering::Relaxed},
};

static GLOBAL_TASK_ID: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn reset_task_id_generator(n: u64) { GLOBAL_TASK_ID.store(n, Relaxed); }

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Serialize, Deserialize, Debug)]
pub struct TaskId(u64);

impl TaskId {
    #[inline]
    pub fn new() -> Self { Self(GLOBAL_TASK_ID.fetch_add(1, Relaxed)) }

    #[inline]
    pub fn inner(&self) -> u64 { self.0 }
}

impl Default for TaskId {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl Display for TaskId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "#{}", self.0) }
}
