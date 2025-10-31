//! Worker 被设计为 ThreadLocal 的, 只向本地的调度器汇报工作
use crate::{
    http::{
        file_cursor::FileCursor,
        header_map_ext::{HeaderMapExt, HeaderMapExtError},
        status::TaskStatus,
    },
    utils::rate_limiter::SharedRateLimiter,
};
use WorkerError::*;
use compio::{
    fs::File,
    io::AsyncWriteAtExt,
    runtime::{JoinHandle, spawn},
    time::timeout,
};
use cyper::Client;
use futures_util::{FutureExt, StreamExt, TryStreamExt, select_biased};
use identity::task::TaskId;
use multipart_async_stream::{LendingIterator, MultipartStream};
use see::sync as watch;
use smol_cancellation_token::CancellationToken;
use sparse_ranges::{FrozenRangeSet, Range, RangeSet};
use std::{sync::LazyLock, time::Duration};
use thiserror::Error;
use tracing::instrument;
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
    #[error("file write error: {0}")]
    FileWrite(#[from] std::io::Error),
    #[error("initiate cancel")]
    InitiateCancel,
    #[error("recved chunk size {0} is greater than u32::MAX")]
    ChunkTooLarge(usize),
    #[error("timeout")]
    Timeout,
    #[error("zero content length")]
    ZeroContentLength,
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
    const WORKER_TIMEOUT: Duration = Duration::from_mins(3);

    #[instrument(skip(self), fields(id = ?self.id, range = ?self.assign))]
    pub fn spawn(self) -> WorkerFuture {
        let assign = self.assign.clone();
        let id = self.id;
        let fut = async move {
            match self.assign {
                Some(ref rng) if rng.is_empty() => {
                    Err(Box::new(WorkFailed::builder().id(id).err(WorkerError::ZeroContentLength).build()))
                }
                Some(ref rng) if rng.ranges_count() == 1 => self.download_continuous().await,
                Some(_) => self.download_spare().await,
                None => self.download_any().await,
            }
        };
        spawn(async move {
            // timeout(Self::WORKER_TIMEOUT, fut).await.unwrap_or_else(|_| {
            //     let mut failed = Box::new(WorkFailed::builder().id(id).err(Timeout).build());
            //     failed.revert = assign.map(Into::into);
            //     Err(failed)
            // })
            fut.await
        })
    }

    #[instrument(skip_all)]
    async fn download_any_impl(
        url: Url, mut file: File, rate_limit: SharedRateLimiter, status: watch::Sender<TaskStatus>,
    ) -> InnerResult {
        let span = tracing::span!(tracing::Level::INFO, "download_any_impl", url = %url);
        let resp = GLOBAL_HTTP_CLIENT.get(url)?.send().await?;
        let mut strm = resp.bytes_stream();
        let mut file = FileCursor::from(&mut file);
        let guard = span.entered();
        while let Some(body_res) = strm.next().await {
            let body = body_res?;
            let n = {
                let len = body.len();
                len.try_into().map_err(|_| ChunkTooLarge(len))?
            };
            rate_limit.acquire(n).await?;
            file.write_all(body).await?;
            status.send_modify(|s| s.downloaded |= file.range());
        }
        guard.exit();
        Ok(file.into_range())
    }

    /// 用于不知道目标文件大小的下载。
    /// 通常用于需要单线程下载或不知道目标大小的文件，降级到使用单个 worker 并使用 download_any 下载。
    /// 对于此函数，如果成功总是返回成功下载的 range, 如果失败则返回 None 与 错误
    #[instrument(skip_all)]
    async fn download_any(self) -> WorkerResult {
        let cancel_err = || Err(Box::new(WorkFailed::builder().id(self.id).err(InitiateCancel).build()));
        let handle_res = |res: InnerResult| {
            res.map(|downloaded| WorkSuccess { id: self.id, downloaded })
                .map_err(|err| Box::new(WorkFailed::builder().id(self.id).err(err).build()))
        };
        select_biased! {
            res = Self::download_any_impl(self.url, self.file, self.rate_limit, self.status).fuse() => {
                handle_res(res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                cancel_err()
            }
        }
    }

    /// 用于知道目标文件大小的下载实现，返回的时候需要检查范围
    #[instrument(skip_all)]
    async fn download_continuous_impl(
        full: &FrozenRangeSet, url: Url, mut file: File, rate_limit: SharedRateLimiter,
        status: watch::Sender<TaskStatus>,
    ) -> InnerResult {
        let start = full.start().expect("null start") as u64;
        let resp = GLOBAL_HTTP_CLIENT
            .get(url)?
            .header("Range", full.to_http_range_header().unwrap().to_string())?
            .send()
            .await?;
        let mut strm = resp.bytes_stream();
        let mut file = FileCursor::with_position(&mut file, start);
        while let Some(body_res) = strm.next().await {
            let body = body_res?;
            let n = {
                let len = body.len();
                len.try_into().map_err(|_| ChunkTooLarge(len))?
            };
            rate_limit.acquire(n).await?;
            file.write_all(body).await?;
            status.send_modify(|s| s.downloaded |= file.range());
        }
        Ok(file.into_range())
    }

    /// 稀疏和连续通用，用于检测出worker正常执行完但没有下载成功的部分
    #[instrument(skip_all)]
    fn handle_ranged_result(id: TaskId, full: FrozenRangeSet, res: InnerResult) -> WorkerResult {
        res.and_then(|downloaded| {
            if downloaded != full {
                Err(ParitalDownloaded(downloaded))
            } else {
                Ok(downloaded)
            }
        })
        .map(|downloaded| WorkSuccess { id, downloaded })
        .map_err(|err| Box::new(WorkFailed::builder().id(id).revert(full.into()).err(err).build()))
    }

    /// 用于一个连续的文件切片下载，调用此函数时假设 range 不为空
    /// 此函数成功时返回成功下载的范围，失败时返回取消提交的范围和错误
    #[instrument(skip_all)]
    async fn download_continuous(self) -> WorkerResult {
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
        full: &FrozenRangeSet, url: Url, mut file: File, rate_limit: SharedRateLimiter,
        status: watch::Sender<TaskStatus>,
    ) -> InnerResult {
        let resp = GLOBAL_HTTP_CLIENT
            .get(url)?
            .header("Range", full.to_http_range_header().unwrap().to_string())?
            .send()
            .await?;
        let bnd = resp.headers().parse_boundary()?;
        let strm = resp.bytes_stream();
        let mut multipart = MultipartStream::new(strm, &bnd);
        let mut total_downloaded = RangeSet::new();
        while let Some(part_res) = multipart.next().await {
            let part = part_res?;
            let hdr = part.headers();
            let (part_full, _) = hdr.parse_content_range()?;
            if !full.contains(&part_full) {
                continue; // 如果这个 part 的范围不在被分配的范围中，则跳过
            }
            let mut body = part.body();
            let mut partial_downloaded = RangeSet::new();
            while let Ok(Some(payload)) = body.try_next().await {
                let len = payload.len();
                let cur = partial_downloaded.last().map_or(part_full.start(), |n| n + 1);
                if 0 == len {
                    continue; // 如果实际收到的文件长度为 0，也跳过
                }
                let recv = Range::new(cur, cur + len - 1);
                if !part_full.contains(&recv) {
                    continue; // 如果实际收到的文件范围不在头的指示中，也跳过
                }
                let n = {
                    let len = payload.len();
                    len.try_into().map_err(|_| ChunkTooLarge(len))?
                };
                rate_limit.acquire(n).await?;
                file.write_all_at(payload, cur as u64).await.0.inspect(|_| {
                    partial_downloaded.insert_range(&recv);
                })?;
                status.send_modify(|s| s.downloaded |= &partial_downloaded);
            }
            total_downloaded |= &partial_downloaded;
        }
        Ok(total_downloaded)
    }

    /// 用于稀疏文件切片下载
    #[instrument(skip_all)]
    async fn download_spare(self) -> WorkerResult {
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

#[cfg(test)]
mod tests {
    use super::*;
    use compio::{BufResult, io::AsyncReadAtExt};
    use see::sync as watch;
    use std::vec;
    use tempfile::NamedTempFile;
    use url::Url;

    fn mock_file() -> File {
        let std_file = NamedTempFile::new().unwrap().into_file();
        unsafe {
            #[cfg(unix)]
            {
                use std::os::fd::{FromRawFd, IntoRawFd};
                compio::fs::File::from_raw_fd(std_file.into_raw_fd())
            }
            #[cfg(windows)]
            {
                // todo 可能要设置非阻塞
                use std::os::windows::io::{FromRawHandle, IntoRawHandle};
                let handle = std_file.into_raw_handle();
                compio::fs::File::from_raw_handle(handle)
            }
        }
    }

    // 记得给 fd 设置非阻塞
    fn mock_worker(rng: RangeSet) -> (File, Worker) {
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let cancel = CancellationToken::new();
        let file = mock_file();
        let (tx, _rx) = watch::channel(TaskStatus::default());
        let worker = Worker::builder()
            .id(TaskId::new())
            .url(url)
            .file(file.clone())
            .assign(rng.into())
            .child_cancel(cancel)
            .rate_limit(rate_limiter)
            .status(tx)
            .build();
        (file.clone(), worker)
    }

    #[compio::test]
    async fn test_spare_worker() {
        let rng: RangeSet = [Range::new(0, 128), Range::new(200, 296)].into_iter().collect();
        let (file, worker) = mock_worker(rng.clone());
        let result = worker.spawn().await.unwrap();
        assert_eq!(result.unwrap().downloaded, rng);
        let buf = vec![];
        let BufResult(res, buf) = file.read_to_end_at(buf, 0).await;
        res.unwrap();
        println!("{:?}", buf);
    }

    #[compio::test]
    async fn test_continuous_worker() {
        let mut set = RangeSet::new();
        set.insert_range(&Range::new(64, 128));
        let (file, worker) = mock_worker(set.clone());
        let result = worker.spawn().await.unwrap();
        println!("{:?}", result);
        assert_eq!(result.unwrap().downloaded, set);
        let buf = vec![];
        let BufResult(res, buf) = file.read_to_end_at(buf, 0).await;
        res.unwrap();
        println!("{:?}", buf);
    }

    fn mock_worker_any() -> (File, Worker) {
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        // 使用一个较小的、已知大小的文件进行测试会更稳定和快速
        let url = Url::parse("https://www.rust-lang.org/static/images/rust-logo-blk.svg").unwrap();
        let cancel = CancellationToken::new();
        let file = mock_file();
        let (tx, _rx) = watch::channel(TaskStatus::default());
        let worker = Worker::builder()
            .id(TaskId::new())
            .url(url)
            .file(file.clone())
            .child_cancel(cancel)
            .rate_limit(rate_limiter)
            .status(tx)
            .build();
        // 注意这里 range 传入 None
        (file.clone(), worker)
    }

    #[compio::test]
    async fn test_any_worker() {
        let (_file, worker) = mock_worker_any();
        let result = worker.spawn().await.unwrap();
        println!("{:?}", result);

        // 对于 download_any，我们不知道确切大小，但可以断言结果是 Ok 且范围不为空
        assert!(result.is_ok());
        assert!(!result.unwrap().downloaded.is_empty());
    }
}
