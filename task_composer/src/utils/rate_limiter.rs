use crate::WorkerError;
use arc_swap::{ArcSwapOption, access::Access};
use compio::bytes::Bytes;
use governor::{DefaultDirectRateLimiter, InsufficientCapacity, Quota};
use std::{num::NonZeroU32, sync::Arc};

#[must_use]
#[derive(Default, Clone, Debug)]
pub struct SharedRateLimiter(Arc<ArcSwapOption<DefaultDirectRateLimiter>>);

impl SharedRateLimiter {
    #[inline]
    pub fn new_without_limit() -> Self { Self(ArcSwapOption::empty().into()) }

    #[inline]
    pub fn new_with_limit(bytes_per_sencond: NonZeroU32) -> Self {
        let limiter = DefaultDirectRateLimiter::direct(Quota::per_second(bytes_per_sencond));
        Self(ArcSwapOption::from_pointee(limiter).into())
    }

    #[inline]
    /// 会有一次加载原子变量的开销，但其实还好
    pub fn is_no_limit(&self) -> bool { self.0.load().is_none() }

    // 在没有限速器的时候，任意数值都可以
    #[inline]
    pub async fn acquire(&self, n: u32) -> Result<(), InsufficientCapacity> {
        if n == 0 {
            return Ok(());
        }
        let inner = self.0.load();
        let fut = inner.as_ref().map(|l| l.until_n_ready(unsafe { NonZeroU32::new_unchecked(n) }));
        if let Some(fut) = fut {
            return fut.await;
        }
        Ok(())
    }

    #[inline]
    pub fn change_limit(&self, bytes_per_second: Option<NonZeroU32>) {
        let Some(limit) = bytes_per_second else {
            self.0.store(None);
            return;
        };
        let new_limiter = DefaultDirectRateLimiter::direct(Quota::per_second(limit));
        self.0.store(Some(new_limiter.into()));
    }
}

pub trait AcquireToken {
    fn token(&self) -> Result<u32, WorkerError>;
}

impl AcquireToken for Bytes {
    #[inline]
    fn token(&self) -> Result<u32, WorkerError> {
        let len = self.len();
        let token = len.try_into().map_err(|_| WorkerError::ChunkTooLarge(len))?;
        Ok(token)
    }
}
