//! Workers are designed to be ThreadLocal and only report work to their local dispatcher.
//! A worker may partially complete work, fully complete work, or fail to complete work (due to errors).
//! Rollback typically occurs when work proceeds normally but some slices are missing at the end.
use crate::{
    http::{
        header_map_ext::{HeaderMapExt, HeaderMapExtError},
        status::TaskStatus,
    },
    rate_limiter::AcquireToken,
    utils::rate_limiter::SharedRateLimiter,
};
use WorkerError::*;
use compio::{
    BufResult,
    fs::File,
    io::{AsyncWrite, AsyncWriteAtExt, AsyncWriteExt},
    runtime::{JoinHandle, spawn},
    time::timeout,
};
use cyper::Client;
use falcon_config::global_config;
use falcon_filesystem::{RandBufFile, SeqBufFile};
use falcon_identity::task::TaskId;
use futures_util::{StreamExt, TryStreamExt};
use multipart_async_stream::{LendingIterator, MultipartStream};
use see::sync as watch;
use smol_cancellation_token::CancellationToken;
use sparse_ranges::{FrozenRangeSet, Range, RangeSet};
use std::{num::NonZeroUsize, sync::LazyLock, time::Duration};
use thiserror::Error;
use tracing::{debug, error, instrument, warn};
use typed_builder::TypedBuilder;
use url::Url;

/// Macro to handle errors with cleanup (shutdown + progress update) before returning.
macro_rules! cleanup_then_fail {
    ($file:expr, $status:expr, $id:expr, $err:expr, $assigned:expr) => {{
        let _ = $file.shutdown().await;
        Self::update_progress($status, $file.buffered_range(), $file.flushed_range());
        return Err(Self::make_failed($id, $err, $assigned));
    }};
}

pub static HTTP_CLIENT: LazyLock<Client> = LazyLock::new(Client::new);

/// Worker thread responsible for downloading specific range of a file.
///
/// Thread-local design reporting only to local dispatcher.
/// Handles three download modes:
/// - **Any mode**: Unknown file size, continuous download
/// - **Continuous mode**: Known file size, single range download
/// - **Sparse mode**: Known file size, multi-range (multipart) download
#[derive(Debug, TypedBuilder)]
pub struct Worker {
    /// Unique task identifier
    id: TaskId,
    /// Download URL
    url: Url,
    /// File handle (supports random or sequential access)
    file: File,
    /// Channel to report download progress to dispatcher
    status: watch::Sender<TaskStatus>,
    /// Token to receive cancellation requests
    child_cancel: CancellationToken,
    /// Rate limiter controlling download speed
    rate_limit: SharedRateLimiter,
    /// Optional assigned range(s). `None` means download entire file.
    #[builder(default, setter(strip_option))]
    assign: Option<FrozenRangeSet>,
    /// Buffer base size for SeqBufFile and RandBufFile (cached from config)
    #[builder(default = global_config().file_buffer_base)]
    buffer_base: NonZeroUsize,
    /// Buffer max size for SeqBufFile (cached from config)
    #[builder(default = global_config().file_buffer_max)]
    buffer_max: NonZeroUsize,
    /// Worker timeout in minutes (cached from config)
    #[builder(default = Duration::from_mins(global_config().worker_timeout_mins.get() as u64))]
    timeout: Duration,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("Failed to send HTTP request")]
    CyperInternal(#[from] cyper::Error),
    #[error("Failed to parse HTTP response headers")]
    HeaderExt(#[from] HeaderMapExtError),
    #[error("Downloaded ranges do not match assigned ranges")]
    PartialDownloaded(RangeSet), // Not using FrozenRangeSet to avoid an extra conversion
    #[error("Failed to parse multipart response stream")]
    MultiPartStream(#[from] multipart_async_stream::Error),
    #[error("Rate limit exceeded")]
    RateLimit(#[from] governor::InsufficientCapacity),
    #[error("Failed to write to file")]
    File(#[from] std::io::Error),
    #[error("Worker was cancelled")]
    InitiateCancel,
    #[error("HTTP response chunk exceeds maximum allowed size: {0} bytes")]
    ChunkTooLarge(usize),
    #[error("Operation timed out")]
    Timeout,
    #[error("Worker assigned empty download range")]
    EmptyRange,
    #[error("Previous error was not logged before shutdown")]
    PreviousUnlogged,
    #[error("Downloaded ranges are not continuous")]
    NotContinuous,
}

#[derive(Debug)]
#[must_use]
pub struct WorkSuccess {
    pub id: TaskId,
    /// Ranges that have been flushed to disk
    pub flushed: RangeSet,
}

#[derive(Debug, TypedBuilder)]
#[must_use]
pub struct WorkFailed {
    pub id: TaskId,
    /// Ranges that were assigned to this worker
    #[builder(default, setter(strip_option))]
    pub assigned: Option<RangeSet>,
    pub err: WorkerError,
}

pub type WorkerResult = Result<WorkSuccess, WorkFailed>;
pub type WorkerFuture = JoinHandle<WorkerResult>;

impl Worker {
    /// Creates `WorkFailed` error with optional assigned range.
    #[inline]
    fn make_failed(id: TaskId, err: WorkerError, assigned: Option<RangeSet>) -> WorkFailed {
        let builder = WorkFailed::builder().id(id).err(err);
        match assigned {
            Some(range) => builder.assigned(range).build(),
            None => builder.build(),
        }
    }

    #[instrument(skip(self), fields(id = %self.id, range = ?self.assign))]
    pub fn spawn(self) -> WorkerFuture {
        let dur = self.timeout;
        let timeout_err = Self::make_failed(self.id, Timeout, self.assign.clone().map(Into::into));
        let fut = async move {
            match self.assign {
                Some(ref rng) if rng.is_empty() => Err(Self::make_failed(self.id, EmptyRange, None)),
                Some(ref rng) if rng.ranges_count() == 1 => self.download_continuous().await,
                Some(_) => self.download_spare().await,
                None => self.download_any().await,
            }
        };
        spawn(async move { timeout(dur, fut).await.map_err(|_| timeout_err).flatten() })
    }

    /// Updates task status by merging new buffered and flushed ranges.
    #[inline]
    fn update_progress(status: &watch::Sender<TaskStatus>, new_buffered: &RangeSet, new_flushed: &RangeSet) {
        status.send_modify(|s| {
            let TaskStatus { buffered, flushed, .. } = s;
            *buffered |= new_buffered;
            *flushed |= new_flushed;
        });
    }

    /// Downloads file when target size is unknown.
    ///
    /// Used for single-threaded downloads or when file size cannot be determined.
    #[instrument(name = "worker.any", skip(self), fields(task_id = %self.id, url = %self.url))]
    async fn download_any(self) -> WorkerResult {
        debug!("Starting download with unknown file size");
        let resp = HTTP_CLIENT
            .get(self.url)
            .map_err(|e| Self::make_failed(self.id, e.into(), None))?
            .send()
            .await
            .map_err(|e| Self::make_failed(self.id, e.into(), None))?;
        let mut byte_stream = resp.bytes_stream();
        let mut file = SeqBufFile::with_posistion(self.file, 0, self.buffer_base, self.buffer_max);
        while let Some(body_res) = byte_stream.next().await {
            if self.child_cancel.is_cancelled() {
                cleanup_then_fail!(file, &self.status, self.id, InitiateCancel, None);
            }
            let body = match body_res {
                Ok(b) => b,
                Err(e) => cleanup_then_fail!(file, &self.status, self.id, e.into(), None),
            };
            let tokens = match body.tokens() {
                Ok(t) => t,
                Err(e) => cleanup_then_fail!(file, &self.status, self.id, e, None),
            };
            if let Err(e) = self.rate_limit.acquire(tokens).await {
                cleanup_then_fail!(file, &self.status, self.id, e.into(), None);
            }
            if let BufResult(Err(e), _) = file.write_all(body).await {
                cleanup_then_fail!(file, &self.status, self.id, e.into(), None);
            }
            Self::update_progress(&self.status, file.buffered_range(), file.flushed_range());
        }
        if let Err(e) = file.shutdown().await {
            cleanup_then_fail!(file, &self.status, self.id, e.into(), None);
        }
        let flushed = file.into_flushed_range();
        Self::update_progress(&self.status, &flushed, &flushed);
        // download_any is for unknown file size, success with continuous download (ranges_count() == 1)
        if flushed.ranges_count() > 1 {
            return Err(Self::make_failed(self.id, NotContinuous, flushed.into()));
        }
        if flushed.is_empty() {
            return Err(Self::make_failed(self.id, EmptyRange, None));
        }
        Ok(WorkSuccess { id: self.id, flushed })
    }

    /// Downloads continuous range of the file.
    ///
    /// # Preconditions
    ///
    /// Assigned range must be non-empty and contain exactly one continuous range.
    /// Guaranteed by `spawn()` dispatch logic.
    #[instrument(name = "worker.continuous", skip(self), fields(task_id = %self.id, url = %self.url, range = ?self.assign))]
    async fn download_continuous(self) -> WorkerResult {
        let assigned = self.assign.expect("Assign is Some by spawn() guarantee");
        let start = assigned.start().expect("Range is non-empty by spawn() guarantee") as u64;
        debug!(
            task_id = %self.id,
            assigned = ?assigned,
            start_pos = start,
            "Starting continuous download"
        );
        let range_header =
            assigned.to_http_range_header().expect("Range header valid for non-empty ranges").to_string();
        let resp = HTTP_CLIENT
            .get(self.url)
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?
            .header("Range", range_header)
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?
            .send()
            .await
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?;

        debug!(
            task_id = %self.id,
            status = resp.status().as_u16(),
            content_length = resp.headers().get("content-length").map(|v| v.to_str().ok()),
            content_range = resp.headers().get("content-range").map(|v| v.to_str().ok()),
            "HTTP response received"
        );

        let mut byte_stream = resp.bytes_stream();
        let mut file = SeqBufFile::with_posistion(self.file, start, self.buffer_base, self.buffer_max);
        while let Some(body_res) = byte_stream.next().await {
            if self.child_cancel.is_cancelled() {
                cleanup_then_fail!(file, &self.status, self.id, InitiateCancel, Some(assigned.into()));
            }
            let body = match body_res {
                Ok(b) => b,
                Err(e) => cleanup_then_fail!(file, &self.status, self.id, e.into(), Some(assigned.into())),
            };
            let tokens = match body.tokens() {
                Ok(t) => t,
                Err(e) => cleanup_then_fail!(file, &self.status, self.id, e, Some(assigned.into())),
            };
            if let Err(e) = self.rate_limit.acquire(tokens).await {
                cleanup_then_fail!(file, &self.status, self.id, RateLimit(e), Some(assigned.into()));
            }
            if let BufResult(Err(e), _) = file.write_all(body).await {
                cleanup_then_fail!(file, &self.status, self.id, File(e), Some(assigned.into()));
            }
            debug!(
                task_id = %self.id,
                buffered = ?file.buffered_range(),
                flushed = ?file.flushed_range(),
                "Data written to buffer"
            );
            Self::update_progress(&self.status, file.buffered_range(), file.flushed_range());
        }
        if let Err(e) = file.shutdown().await {
            cleanup_then_fail!(file, &self.status, self.id, File(e), Some(assigned.into()));
        }
        let flushed = file.into_flushed_range();
        debug!(
            task_id = %self.id,
            assigned = ?assigned,
            flushed = ?flushed,
            assigned_len = assigned.len(),
            flushed_len = flushed.len(),
            "Checking if download is complete"
        );
        Self::update_progress(&self.status, &flushed, &flushed);
        if flushed != assigned {
            error!(
                task_id = %self.id,
                assigned = ?assigned,
                flushed = ?flushed,
                assigned_len = assigned.len(),
                flushed_len = flushed.len(),
                "Downloaded ranges do not match assigned ranges"
            );
            return Err(Self::make_failed(self.id, PartialDownloaded(flushed), Some(assigned.into())));
        }
        Ok(WorkSuccess { id: self.id, flushed })
    }

    /// Downloads sparse ranges using multipart requests.
    ///
    /// # Preconditions
    ///
    /// Assigned range must contain at least two separate ranges.
    /// Guaranteed by `spawn()` dispatch logic.
    #[instrument(name = "worker.sparse", skip(self), fields(task_id = %self.id, url = %self.url, range = ?self.assign))]
    async fn download_spare(self) -> WorkerResult {
        let assigned = self.assign.expect("Assign is Some by spawn() guarantee");
        debug!(
            task_id = %self.id,
            assigned = ?assigned,
            "Starting sparse (multipart) download"
        );
        let range_header =
            assigned.to_http_range_header().expect("Range header valid for non-empty ranges").to_string();
        let resp = HTTP_CLIENT
            .get(self.url)
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?
            .header("Range", range_header)
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?
            .send()
            .await
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?;
        let boundary = resp
            .headers()
            .parse_boundary()
            .map_err(|e| Self::make_failed(self.id, e.into(), Some(assigned.clone().into())))?;
        let byte_stream = resp.bytes_stream();
        let mut multipart = MultipartStream::new(byte_stream, &boundary);
        let mut file = RandBufFile::new(self.file, self.buffer_base);
        while let Some(part_res) = multipart.next().await {
            if self.child_cancel.is_cancelled() {
                cleanup_then_fail!(file, &self.status, self.id, InitiateCancel, Some(assigned.into()));
            }
            let part = match part_res {
                Ok(p) => p,
                Err(e) => {
                    cleanup_then_fail!(file, &self.status, self.id, e.into(), Some(assigned.into()))
                }
            };
            let (part_assigned, _) = match part.headers().parse_single_content_range() {
                Ok(r) => r,
                Err(e) => cleanup_then_fail!(file, &self.status, self.id, e.into(), Some(assigned.into())),
            };
            if !assigned.contains(&part_assigned) {
                warn!(
                    part_range = ?part_assigned,
                    assigned_range = ?assigned,
                    "Skipping multipart part outside assigned range"
                );
                continue;
            }
            let mut body_stream = part.body();
            let mut cur = part_assigned.start();
            while let Ok(Some(payload)) = body_stream.try_next().await {
                if self.child_cancel.is_cancelled() {
                    cleanup_then_fail!(file, &self.status, self.id, InitiateCancel, Some(assigned.clone().into()));
                }
                if payload.is_empty() {
                    warn!(
                        part_range = ?part_assigned,
                        offset = cur,
                        "Received empty multipart payload, skipping"
                    );
                    continue;
                }
                // Safety: payload.len() > 0 verified above, recved validated against part_assigned below
                let recved = unsafe { Range::new_unchecked(cur, cur + payload.len() - 1) };
                if !part_assigned.contains(&recved) {
                    warn!(
                        slice_range = ?recved,
                        part_range = ?part_assigned,
                        "Skipping multipart payload slice outside part boundary"
                    );
                    continue;
                }
                let tokens = match payload.tokens() {
                    Ok(t) => t,
                    Err(e) => cleanup_then_fail!(file, &self.status, self.id, e, Some(assigned.clone().into())),
                };
                if let Err(e) = self.rate_limit.acquire(tokens).await {
                    cleanup_then_fail!(file, &self.status, self.id, e.into(), Some(assigned.clone().into()));
                }
                let payload_len = payload.len();
                let BufResult(res, _) = file.write_all_at(payload, cur as u64).await;
                if let Err(e) = res {
                    cleanup_then_fail!(file, &self.status, self.id, e.into(), Some(assigned.clone().into()));
                }
                cur += payload_len;
                debug!(
                    task_id = %self.id,
                    part_range = ?part_assigned,
                    write_offset = cur - payload_len,
                    payload_len = payload_len,
                    buffered = ?file.buffered_range(),
                    flushed = ?file.flushed_range(),
                    "Multipart part data written"
                );
                Self::update_progress(&self.status, file.buffered_range(), file.flushed_range());
            }
        }
        if let Err(e) = file.shutdown().await {
            cleanup_then_fail!(file, &self.status, self.id, e.into(), Some(assigned.clone().into()));
        }
        let flushed = file.into_flushed_range();
        debug!(
            task_id = %self.id,
            assigned = ?assigned,
            flushed = ?flushed,
            assigned_len = assigned.len(),
            flushed_len = flushed.len(),
            "Checking if multipart download is complete"
        );
        Self::update_progress(&self.status, &flushed, &flushed);
        if flushed != assigned {
            error!(
                task_id = %self.id,
                assigned = ?assigned,
                flushed = ?flushed,
                assigned_len = assigned.len(),
                flushed_len = flushed.len(),
                "Downloaded ranges do not match assigned ranges"
            );
            Err(Self::make_failed(self.id, PartialDownloaded(flushed), Some(assigned.clone().into())))
        } else {
            Ok(WorkSuccess { id: self.id, flushed })
        }
    }
}

// TODO: Add unit tests
