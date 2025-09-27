use std::num::{NonZeroU16, NonZeroU32};

#[derive(Debug, Clone, Copy)]
pub enum TaskCommand {
    ChangeConcurrency(NonZeroU16),
    ChangeRateLimited(Option<NonZeroU32>),
    Pause,
    Resume,
    Cancel,
}
