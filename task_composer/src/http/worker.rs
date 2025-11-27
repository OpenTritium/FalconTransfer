//! Worker 被设计为 ThreadLocal 的, 只向本地的调度器汇报工作
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
    fs::File,
    io::{AsyncWrite, AsyncWriteAtExt, AsyncWriteExt},
    runtime::{JoinHandle, spawn},
    time::timeout,
};
use cyper::Client;
use falcon_filesystem::{RandBufFile, SeqBufFile};
use falcon_identity::task::TaskId;
use futures_util::{FutureExt, StreamExt, TryStreamExt, select, select_biased};
use multipart_async_stream::{LendingIterator, MultipartStream};
use see::sync as watch;
use smol_cancellation_token::CancellationToken;
use sparse_ranges::{FrozenRangeSet, Range, RangeSet};
use std::{sync::LazyLock, time::Duration};
use thiserror::Error;
use tracing::{Level, instrument, span, warn};
use typed_builder::TypedBuilder;
use url::Url;

pub static GLOBAL_HTTP_CLIENT: LazyLock<Client> = LazyLock::new(Client::new);

#[derive(Debug, TypedBuilder)]
pub struct Worker {
    id: TaskId,
    url: Url,
    file: File,
    status: watch::Sender<TaskStatus>,
    child_cancel: CancellationToken,
    rate_limit: SharedRateLimiter,
    #[builder(default, setter(strip_option))]
    assign: Option<FrozenRangeSet>,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("reqwest internal error: {0}")]
    CyperInternal(#[from] cyper::Error),
    #[error("header map process error: {0}")]
    HeaderExt(#[from] HeaderMapExtError),
    #[error("partial download range: {0:?}")]
    ParitalDownloaded(RangeSet),
    #[error("multipart stream error: {0}")]
    MultiPartStream(#[from] multipart_async_stream::Error),
    #[error("token bucket capacity insufficient: {0}")]
    RateLimit(#[from] governor::InsufficientCapacity),
    #[error("file opt error: {0}")]
    File(#[from] std::io::Error),
    #[error("initiate cancel")]
    InitiateCancel,
    #[error("recved chunk size {0} is greater than u32::MAX")]
    ChunkTooLarge(usize),
    #[error("timeout")]
    Timeout,
    #[error("this worker has a empty assigned range")]
    EmptyRange,
}

#[derive(Debug)]
pub struct WorkSuccess {
    pub id: TaskId,
    pub downloaded: RangeSet,
}

#[derive(Debug, TypedBuilder)]
pub struct WorkFailed {
    pub id: TaskId,
    #[builder(default, setter(strip_option))]
    pub revert: Option<RangeSet>,
    pub err: WorkerError,
}

pub type WorkerResult = Result<WorkSuccess, Box<WorkFailed>>;
pub type WorkerFuture = JoinHandle<WorkerResult>;
type InnerResult = Result<RangeSet, WorkerError>;

impl Worker {
    // todo：修改超时机制
    const WORKER_TIMEOUT: Duration = Duration::from_mins(3);

    #[instrument(skip(self), fields(id = ?self.id, range = ?self.assign))]
    pub fn spawn(self) -> WorkerFuture {
        let mk_err = move |err| Box::new(WorkFailed::builder().id(self.id).err(err).build());
        let mut timeout_err = mk_err(Timeout);
        timeout_err.revert = self.assign.clone().map(Into::into);
        let fut = async move {
            match self.assign {
                // 检查是否被分配分配了空 range，直接返回错误
                Some(ref rng) if rng.is_empty() => Err(mk_err(EmptyRange)),
                // 如果只有一个 range，直接连续下载
                Some(ref rng) if rng.ranges_count() == 1 => self.download_continuous().await,
                // 多个 range 就稀疏下载
                Some(_) => self.download_spare().await,
                // 应对不知道 content-length 的情况
                None => self.download_any().await,
            }
        };
        spawn(async move { timeout(Self::WORKER_TIMEOUT, fut).await.map_err(|_| timeout_err).flatten() })
    }

    #[inline]
    fn update_progress(status: &mut watch::Sender<TaskStatus>, new_buffered: &RangeSet, new_flushed: &RangeSet) {
        status.send_modify(|s| {
            let TaskStatus { buffered, flushed, .. } = s;
            *buffered |= new_buffered;
            *flushed |= new_flushed;
        });
    }

    #[instrument(skip_all)]
    async fn download_any_impl(
        url: Url, file: File, rate_limit: SharedRateLimiter, mut status: watch::Sender<TaskStatus>,
    ) -> InnerResult {
        let span = span!(Level::INFO, "download_any_impl", url = %url);
        let resp = GLOBAL_HTTP_CLIENT.get(url)?.send().await?;
        let mut strm = resp.bytes_stream();
        let mut file = SeqBufFile::from(file);
        let guard = span.entered();
        while let Some(body_res) = strm.next().await {
            let body = body_res?;
            rate_limit.acquire(body.token()?).await?;
            file.write_all(body).await.0?;
            Self::update_progress(&mut status, file.buffered_range(), file.flushed_range());
        }
        file.shutdown().await?;
        guard.exit();
        Ok(file.into_flushed_range())
    }

    /// 用于不知道目标文件大小的下载。
    /// 通常用于需要单线程下载或不知道目标大小的文件，降级到使用单个 worker 并使用 download_any 下载。
    /// 对于此函数，如果成功总是返回成功下载的 range, 如果失败则返回 None 与 错误
    #[instrument(skip_all)]
    async fn download_any(self) -> WorkerResult {
        let mk_err = move |err| Box::new(WorkFailed::builder().id(self.id).err(err).build());
        let handle_res =
            |res: InnerResult| res.map(|downloaded| WorkSuccess { id: self.id, downloaded }).map_err(mk_err);
        select! {
            res = Self::download_any_impl(self.url, self.file, self.rate_limit, self.status).fuse() => {
                handle_res(res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                Err(mk_err(InitiateCancel))
            }
        }
    }

    /// 用于知道目标文件大小的下载实现，返回的时候需要检查范围
    #[instrument(skip_all)]
    async fn download_continuous_impl(
        full: &FrozenRangeSet, url: Url, file: File, rate_limit: SharedRateLimiter,
        mut status: watch::Sender<TaskStatus>,
    ) -> InnerResult {
        // 实际上 spawn 里保证了了 full 至少包含一个 range
        let start = full.start().unwrap() as u64;
        let resp = GLOBAL_HTTP_CLIENT
            .get(url)?
            .header("Range", full.to_http_range_header().unwrap().to_string())? // 所以这里也不会 panic，看上面的注释
            .send()
            .await?;
        let mut strm = resp.bytes_stream();
        let mut file = SeqBufFile::with_position(file, start);
        while let Some(body_res) = strm.next().await {
            let body = body_res?;
            rate_limit.acquire(body.token()?).await?;
            file.write_all(body).await.0?;
            Self::update_progress(&mut status, file.buffered_range(), file.flushed_range());
        }
        Ok(file.into_flushed_range())
    }

    /// 稀疏和连续通用，用于检测出worker正常执行完但没有下载成功的部分
    #[instrument(skip_all)]
    fn handle_ranged_result(id: TaskId, full: FrozenRangeSet, res: InnerResult) -> WorkerResult {
        res.and_then(|flushed| {
            if flushed != full {
                Err(ParitalDownloaded(flushed))
            } else {
                Ok(flushed)
            }
        })
        .map(|downloaded| WorkSuccess { id, downloaded })
        .map_err(|err| Box::new(WorkFailed::builder().id(id).revert(full.into()).err(err).build()))
    }

    /// 用于一个连续的文件切片下载，调用此函数时假设 range 不为空
    /// 此函数成功时返回成功下载的范围，失败时返回取消提交的范围和错误
    #[instrument(skip_all)]
    async fn download_continuous(self) -> WorkerResult {
        // 实际上 spawn 里保证了了 full 至少包含一个 range
        let full = self.assign.unwrap();
        let cancel_err =
            || Err(Box::new(WorkFailed::builder().id(self.id).revert(full.clone().into()).err(InitiateCancel).build()));
        select_biased! {
            res = Self::download_continuous_impl(&full, self.url, self.file, self.rate_limit, self.status).fuse() => {
                Self::handle_ranged_result(self.id, full, res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                cancel_err()
            }
        }
    }

    #[instrument(skip_all)]
    async fn download_spare_impl(
        full: &FrozenRangeSet, url: Url, file: File, rate_limit: SharedRateLimiter,
        mut status: watch::Sender<TaskStatus>,
    ) -> InnerResult {
        let resp = GLOBAL_HTTP_CLIENT
            .get(url)?
            .header("Range", full.to_http_range_header().unwrap().to_string())? // 保证了至少两个 range
            .send()
            .await?;
        let bnd = resp.headers().parse_boundary()?;
        let strm = resp.bytes_stream();
        let mut multipart = MultipartStream::new(strm, &bnd);
        let mut file = RandBufFile::new(file);
        while let Some(part_res) = multipart.next().await {
            let part = part_res?;
            let hdr = part.headers();
            let (part_full, _) = hdr.parse_single_content_range()?;
            if !full.contains(&part_full) {
                warn!("part {part_full:?} is not in the assigned range in {full:?}");
                continue; // 如果这个 part 的范围不在被分配的范围中，则跳过
            }
            let mut body_strm = part.body();
            let cur = part_full.start(); // 每个part 的起始位置，然后随着body 流的写入这个也开始偏移
            while let Ok(Some(payload)) = body_strm.try_next().await {
                if payload.is_empty() {
                    warn!("empty payload"); // 但平时这种情况真的少吧
                    continue; // 这是为了防止 rng 计算错误
                }
                //
                let recved = unsafe { Range::new_unchecked(cur, cur + payload.len() - 1) };
                if !part_full.contains(&recved) {
                    warn!("part slice {recved:?} is not in the part range in {part_full:?}");
                    continue; // 如果这个 part 的某个块不在 这个 part 范围中也跳过
                }
                rate_limit.acquire(payload.token()?).await?;
                file.write_all_at(payload, cur as u64).await.0?;
                Self::update_progress(&mut status, file.buffered_range(), file.flushed_range());
            }
        }
        Ok(file.into_flushed_range())
    }

    /// 用于稀疏文件切片下载
    #[instrument(skip_all)]
    async fn download_spare(self) -> WorkerResult {
        // spawn 里保证至少包含两个 range
        let full = self.assign.unwrap();
        let cancel_err =
            || Err(Box::new(WorkFailed::builder().id(self.id).revert(full.clone().into()).err(InitiateCancel).build()));

        select_biased! {
            res = Self::download_spare_impl(&full, self.url, self.file, self.rate_limit,self.status).fuse() => {
                Self::handle_ranged_result(self.id, full, res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                cancel_err()
            }
        }
    }
}
