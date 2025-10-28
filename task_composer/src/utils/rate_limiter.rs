use arc_swap::ArcSwapOption;
use governor::{DefaultDirectRateLimiter, InsufficientCapacity, Quota};
use std::{num::NonZeroU32, sync::Arc};

#[derive(Default, Clone, Debug)]
pub struct SharedRateLimiter(Arc<ArcSwapOption<DefaultDirectRateLimiter>>);

impl SharedRateLimiter {
    pub fn new_with_no_limit() -> Self { Self(ArcSwapOption::empty().into()) }

    pub fn new_with_limit(bytes_per_sencond: NonZeroU32) -> Self {
        let limiter = DefaultDirectRateLimiter::direct(Quota::per_second(bytes_per_sencond));
        Self(ArcSwapOption::from_pointee(limiter).into())
    }

    /// 会有一次加载原子变量的开销
    pub fn is_no_limit(&self) -> bool { self.0.load().is_none() }

    // 在没有限速器的时候，任意数值都可以
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

    pub fn change_limit(&self, bytes_per_second: Option<NonZeroU32>) {
        let Some(limit) = bytes_per_second else {
            self.0.store(None);
            return;
        };
        let new_limiter = DefaultDirectRateLimiter::direct(Quota::per_second(limit));
        self.0.store(Some(new_limiter.into()));
    }
}
