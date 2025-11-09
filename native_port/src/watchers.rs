use see::sync::Receiver;
use task_composer::TaskStatus;

use crate::TaskInfo;

struct Watchers(Vec<Receiver<TaskStatus>>);

impl Watchers {
    fn collect_all(&self) -> Vec<TaskStatus> {
        self.0.iter().map(|rx| rx.borrow_and_update().as_ref().clone()).collect()
    }
}

impl From<&TaskStatus> for TaskInfo {
    fn from(status: &TaskStatus) -> Self {
        TaskInfo { id: status.id, name: status.name.clone(), size: status.total, state: status.state }
    }
}
