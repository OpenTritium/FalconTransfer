use crate::WorkerError;
use arc_swap::{ArcSwapOption, access::Access};
use compio::bytes::Bytes;
use governor::{DefaultDirectRateLimiter, InsufficientCapacity, Quota};
use std::{num::NonZeroU32, sync::Arc};

/// Shared rate limiter using ArcSwap for lock-free atomic updates.
///
/// # Design Features
///
/// - Zero-overhead unlimited mode via `without_limit()`
/// - Runtime modifiable via `change_limit()` without restart
/// - Lock-free reads using `ArcSwapOption::load()`
/// - Token bucket algorithm with burst support
#[must_use]
#[derive(Default, Clone, Debug)]
pub struct SharedRateLimiter(Arc<ArcSwapOption<DefaultDirectRateLimiter>>);

impl SharedRateLimiter {
    /// Creates a rate limiter without any limits.
    ///
    /// Calls to `acquire()` will return immediately without any overhead.
    #[inline]
    pub fn without_limit() -> Self { Self(ArcSwapOption::empty().into()) }

    /// Creates rate limiter with fixed rate limit.
    #[inline]
    pub fn with_limit(bytes_per_second: NonZeroU32) -> Self { Self::with_limit_and_burst(bytes_per_second, None) }

    /// Creates rate limiter with rate limit and burst capacity.
    ///
    /// Burst capacity allows short-term traffic exceeding configured limit.
    /// Recommended burst is typically 1-2x the average rate.
    #[inline]
    pub fn with_limit_and_burst(bytes_per_second: NonZeroU32, burst_bytes: Option<NonZeroU32>) -> Self {
        let quota = Quota::per_second(bytes_per_second);
        let quota = burst_bytes.map_or(quota, |burst| quota.allow_burst(burst));
        let limiter = DefaultDirectRateLimiter::direct(quota);
        Self(ArcSwapOption::from_pointee(limiter).into())
    }

    /// Checks if limiter is in unlimited mode.
    #[inline]
    pub fn is_no_limit(&self) -> bool { self.0.load().is_none() }

    /// Acquires the specified number of tokens.
    ///
    /// If rate limiting is enabled, waits until sufficient tokens are available.
    /// If disabled, returns immediately.
    ///
    /// Returns `Err(InsufficientCapacity)` only on configuration errors or
    /// abnormal system time (e.g., time moves backwards). Normal rate limiting
    /// waits automatically via `until_n_ready()`.
    #[inline]
    pub async fn acquire(&self, n: u32) -> Result<(), InsufficientCapacity> {
        if n == 0 {
            return Ok(());
        }
        let inner = self.0.load();
        if let Some(limiter) = inner.as_ref() {
            // Safety: n > 0 guaranteed by early return above
            let nz = unsafe { NonZeroU32::new_unchecked(n) };
            limiter.until_n_ready(nz).await?;
        }
        Ok(())
    }

    /// Dynamically changes the rate limit.
    ///
    /// Atomic operation that does not affect ongoing `acquire()` calls.
    /// Pass `None` for unlimited mode.
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

/// Trait for types convertible to rate limiter tokens.
///
/// Allows different data types to specify their size in tokens,
/// typically used for rate limiting based on data size.
pub trait AcquireToken {
    /// Converts implementer into token count.
    ///
    /// # Errors
    ///
    /// Returns `WorkerError::ChunkTooLarge` if size exceeds `u32::MAX`.
    fn tokens(&self) -> Result<u32, WorkerError>;
}

impl AcquireToken for Bytes {
    #[inline]
    fn tokens(&self) -> Result<u32, WorkerError> {
        let len = self.len();
        let token = len.try_into().map_err(|_| WorkerError::ChunkTooLarge(len))?;
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_without_limit() {
        let limiter = SharedRateLimiter::without_limit();
        assert!(limiter.is_no_limit());
    }

    #[test]
    fn test_with_limit() {
        let limit = NonZeroU32::new(1000).unwrap();
        let limiter = SharedRateLimiter::with_limit(limit);
        assert!(!limiter.is_no_limit());
    }

    #[test]
    fn test_with_limit_and_burst() {
        let limit = NonZeroU32::new(1000).unwrap();
        let burst = NonZeroU32::new(2000).unwrap();
        let limiter = SharedRateLimiter::with_limit_and_burst(limit, Some(burst));
        assert!(!limiter.is_no_limit());
    }

    #[compio::test]
    async fn test_acquire_without_limit() {
        let limiter = SharedRateLimiter::without_limit();

        // Should acquire immediately without waiting
        let result = limiter.acquire(1000).await;
        assert!(result.is_ok());

        // Large acquisitions should also succeed immediately
        let result = limiter.acquire(u32::MAX).await;
        assert!(result.is_ok());
    }

    #[compio::test]
    async fn test_acquire_zero_tokens() {
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(100).unwrap());

        // Zero tokens should always succeed immediately
        let result = limiter.acquire(0).await;
        assert!(result.is_ok());
    }

    #[compio::test]
    async fn test_acquire_with_limit() {
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(1000).unwrap());

        // First acquisition should succeed immediately
        let start = std::time::Instant::now();
        let result = limiter.acquire(500).await;
        assert!(result.is_ok());
        assert!(start.elapsed() < Duration::from_millis(100));

        // Second small acquisition should also succeed quickly
        let result = limiter.acquire(200).await;
        assert!(result.is_ok());
    }

    #[compio::test]
    async fn test_acquire_burst_capacity() {
        let limit = NonZeroU32::new(100).unwrap();
        let burst = NonZeroU32::new(500).unwrap();
        let limiter = SharedRateLimiter::with_limit_and_burst(limit, Some(burst));

        // Should be able to acquire burst amount immediately
        let start = std::time::Instant::now();
        let result = limiter.acquire(500).await;
        assert!(result.is_ok());
        assert!(start.elapsed() < Duration::from_millis(100));

        // Further acquisitions will be rate-limited
        let result = limiter.acquire(100).await;
        assert!(result.is_ok());
    }

    #[compio::test]
    async fn test_change_limit_to_unlimited() {
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(100).unwrap());
        assert!(!limiter.is_no_limit());

        // Change to unlimited
        limiter.change_limit(None);
        assert!(limiter.is_no_limit());

        // Should acquire immediately regardless of size
        let result = limiter.acquire(1_000_000).await;
        assert!(result.is_ok());
    }

    #[compio::test]
    async fn test_change_limit_from_unlimited() {
        let limiter = SharedRateLimiter::without_limit();
        assert!(limiter.is_no_limit());

        // Change to limited
        let limit = NonZeroU32::new(1000).unwrap();
        limiter.change_limit(Some(limit));
        assert!(!limiter.is_no_limit());

        // Should now be rate-limited
        let result = limiter.acquire(500).await;
        assert!(result.is_ok());
    }

    #[compio::test]
    async fn test_change_limit_dynamically() {
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(100).unwrap());

        // Acquire with low limit
        let result = limiter.acquire(50).await;
        assert!(result.is_ok());

        // Increase limit
        limiter.change_limit(Some(NonZeroU32::new(10_000).unwrap()));

        // Should acquire quickly with new higher limit
        let start = std::time::Instant::now();
        let result = limiter.acquire(5000).await;
        assert!(result.is_ok());
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn test_shared_rate_limiter_clone() {
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(1000).unwrap());
        let cloned = limiter.clone();

        assert_eq!(limiter.is_no_limit(), cloned.is_no_limit());
    }

    #[test]
    fn test_acquire_token_for_bytes() {
        // Small bytes
        let bytes = Bytes::from_static(&[1, 2, 3, 4, 5]);
        assert_eq!(bytes.tokens().unwrap(), 5);

        // Empty bytes
        let bytes = Bytes::new();
        assert_eq!(bytes.tokens().unwrap(), 0);

        // Larger bytes
        let data = vec![0u8; 1024];
        let bytes = Bytes::from(data);
        assert_eq!(bytes.tokens().unwrap(), 1024);
    }

    #[test]
    fn test_acquire_token_overflow() {
        // Test bytes larger than u32::MAX
        let too_large = (u32::MAX as usize) + 1;
        let data = vec![0u8; too_large];
        let bytes = Bytes::from(data);

        let result = bytes.tokens();
        assert!(matches!(result, Err(WorkerError::ChunkTooLarge(_))));
    }

    #[compio::test]
    async fn test_concurrent_acquires() {
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(10_000).unwrap());

        // Spawn multiple concurrent acquisitions
        let mut tasks = vec![];
        for _ in 0..10 {
            let limiter_clone = limiter.clone();
            tasks.push(async move { limiter_clone.acquire(100).await });
        }

        // All should succeed
        for task in tasks {
            assert!(task.await.is_ok());
        }
    }

    /// Tests the actual rate limiting effect with more robust timing assertions.
    ///
    /// This test verifies that when the token bucket is exhausted, subsequent
    /// acquisitions wait for the appropriate amount of time for new tokens to
    /// become available.
    #[compio::test]
    async fn test_rate_limiting_effect() {
        // Rate limit: 1000 bytes per second
        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(1000).unwrap());

        // Consume the entire initial bucket (1000 bytes)
        let start = std::time::Instant::now();
        let result = limiter.acquire(1000).await;
        assert!(result.is_ok(), "First acquire should succeed immediately");
        let first_duration = start.elapsed();
        assert!(
            first_duration < Duration::from_millis(100),
            "First acquisition should be immediate, got {:?}",
            first_duration
        );

        // Now acquire another 1000 bytes - the bucket is empty, so we must wait
        // approximately 1 second for new tokens to become available
        let start = std::time::Instant::now();
        let result = limiter.acquire(1000).await;
        assert!(result.is_ok(), "Second acquire should succeed after waiting");
        let second_duration = start.elapsed();

        // Should wait approximately 1 second (1000 bytes @ 1000 bytes/s)
        // Use relaxed bounds to account for scheduling overhead and system load
        assert!(
            second_duration >= Duration::from_millis(900),
            "Should wait at least 900ms for token replenishment, got {:?}",
            second_duration
        );
        assert!(
            second_duration < Duration::from_millis(1500),
            "Should wait less than 1500ms, got {:?}",
            second_duration
        );
    }

    /// Tests rate limiting accuracy by verifying the actual throughput matches
    /// the configured rate limit.
    #[compio::test]
    async fn test_rate_accuracy() {
        const TARGET_RATE: u32 = 10_000; // 10 KB/s
        const TOTAL_REQUEST: u64 = 30_000; // Try to acquire 30 KB total (3x the rate)
        const CHUNK_SIZE: u32 = 100; // Acquire in 100-byte chunks

        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(TARGET_RATE).unwrap());

        let start = std::time::Instant::now();
        let mut total_acquired = 0u64;

        // Acquire tokens in chunks
        while total_acquired < TOTAL_REQUEST {
            let to_acquire = CHUNK_SIZE.min((TOTAL_REQUEST - total_acquired) as u32);
            limiter.acquire(to_acquire).await.unwrap();
            total_acquired += to_acquire as u64;
        }

        let elapsed = start.elapsed();

        // We requested 3x the rate.
        // First bucket (10KB) is available immediately.
        // Remaining 20KB requires waiting 2 seconds.
        // Total time: ~2 seconds for 30KB = 15KB/s effective rate
        let expected_min = Duration::from_millis(1900); // At least ~1.9 seconds
        let expected_max = Duration::from_millis(2500); // At most ~2.5 seconds

        assert!(
            elapsed >= expected_min && elapsed <= expected_max,
            "Should take approximately 2 seconds for 3x rate (first bucket full), got {:?}",
            elapsed
        );

        // The effective rate over the entire period includes the initial full bucket
        // 30KB in 2 seconds = 15KB/s, which is higher than the 10KB/s target rate
        // This is correct behavior for token bucket with initial full capacity
        let actual_rate = (total_acquired as f64 / elapsed.as_secs_f64()) as u32;

        // Allow wider range: 12-18 KB/s is acceptable for 30KB in ~2s
        assert!(
            (12_000..=18_000).contains(&actual_rate),
            "Rate should be ~15KB/s (30KB / 2s) with initial full bucket, got {} bytes/s in {:?}",
            actual_rate,
            elapsed
        );
    }

    /// Tests high-frequency small acquisitions to ensure the limiter performs
    /// well under realistic workloads with many small requests.
    #[compio::test]
    async fn test_high_frequency_acquires() {
        const RATE_LIMIT: u32 = 1_000_000; // 1 MB/s
        const NUM_OPERATIONS: usize = 1000;
        const CHUNK_SIZE: u32 = 100; // 100 bytes per operation

        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(RATE_LIMIT).unwrap());

        let start = std::time::Instant::now();

        // Perform many small acquisitions
        for _ in 0..NUM_OPERATIONS {
            limiter.acquire(CHUNK_SIZE).await.unwrap();
        }

        let elapsed = start.elapsed();
        let total_bytes = (NUM_OPERATIONS * CHUNK_SIZE as usize) as u64;
        let expected_duration = Duration::from_secs_f64(total_bytes as f64 / RATE_LIMIT as f64);

        // Should complete in approximately the expected time, plus some overhead
        assert!(
            elapsed < expected_duration * 2,
            "High-frequency operations should complete efficiently, got {:?}, expected ~{:?}",
            elapsed,
            expected_duration
        );
    }

    /// Tests concurrent limit changes while acquisitions are in progress.
    /// This verifies that the RCU-style updates don't cause crashes or data races.
    #[compio::test]
    async fn test_concurrent_limit_changes_and_acquisitions() {
        use compio::{runtime::spawn, time::sleep};

        let limiter = SharedRateLimiter::with_limit(NonZeroU32::new(1000).unwrap());
        let mut tasks = vec![];

        // Spawn tasks that continuously change the limit
        for i in 1..=5 {
            let limiter_clone = limiter.clone();
            tasks.push(spawn(async move {
                for _ in 0..50 {
                    let new_limit = NonZeroU32::new(i * 1000).unwrap();
                    limiter_clone.change_limit(Some(new_limit));
                    sleep(Duration::from_micros(200)).await;
                }
            }));
        }

        // Spawn tasks that continuously acquire tokens
        for _ in 0..10 {
            let limiter_clone = limiter.clone();
            tasks.push(spawn(async move {
                for _ in 0..100 {
                    // Some acquires may need to wait if limit is low
                    let _ = limiter_clone.acquire(100).await;
                }
            }));
        }

        // All operations should complete successfully without panicking
        for task in tasks {
            task.await.unwrap();
        }
    }

    /// Tests that the limiter correctly handles burst capacity.
    #[compio::test]
    async fn test_burst_capacity_exhaustion() {
        const RATE: u32 = 100; // 100 bytes/s
        const BURST: u32 = 500; // 500 bytes burst capacity

        let limiter = SharedRateLimiter::with_limit_and_burst(
            NonZeroU32::new(RATE).unwrap(),
            Some(NonZeroU32::new(BURST).unwrap()),
        );

        // Should be able to acquire the entire burst amount immediately
        let start = std::time::Instant::now();
        limiter.acquire(BURST).await.unwrap();
        let first_duration = start.elapsed();

        assert!(
            first_duration < Duration::from_millis(100),
            "Burst acquisition should be immediate, got {:?}",
            first_duration
        );

        // Next acquisition should be rate-limited since burst is exhausted
        let start = std::time::Instant::now();
        limiter.acquire(RATE).await.unwrap();
        let second_duration = start.elapsed();

        // Should wait approximately 1 second for RATE bytes
        assert!(
            second_duration >= Duration::from_millis(900),
            "After burst exhaustion, should wait for tokens, got {:?}",
            second_duration
        );
        assert!(
            second_duration < Duration::from_millis(1500),
            "Wait time should be reasonable, got {:?}",
            second_duration
        );
    }
}
