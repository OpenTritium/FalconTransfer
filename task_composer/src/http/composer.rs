use std::sync::Arc;
use tokio::sync::Semaphore;

struct TaskComposer {
    semaphore: Arc<Semaphore>,
}

impl TaskComposer {}
