//! Worker 被设计为 ThreadLocal 的, 只向本地的调度器汇报工作
use crate::{
    http::{
        file_range::{FileCursor, FileMultiRange, FileRange, IntoRangeHeader},
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
use compio_watch as watch;
use cyper::Client;
use futures_util::{FutureExt, StreamExt, TryStreamExt, select_biased};
use identity::task::TaskId;
use multipart_async_stream::{LendingIterator, MultipartStream};
use smol_cancellation_token::CancellationToken;
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
    range: Option<FileMultiRange>,
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("reqwest internal error: {0}")]
    CyperInternal(#[from] cyper::Error),
    #[error("header map process error: {0}")]
    HeaderExt(#[from] HeaderMapExtError),
    #[error("partial download range: {0:?}")]
    ParitalDownloaded(FileMultiRange),
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
}

#[derive(Debug)]
pub struct WorkSuccess {
    pub id: TaskId,
    pub downloaded: FileMultiRange,
}

#[derive(Debug, TypedBuilder)]
pub struct WorkFailed {
    pub id: TaskId,
    #[builder(default, setter(strip_option))]
    pub revert: Option<FileMultiRange>,
    pub err: WorkerError,
}

pub type WorkerResult = Result<WorkSuccess, WorkFailed>;
pub type WorkerFuture = JoinHandle<WorkerResult>;
type InnerResult = Result<FileMultiRange, WorkerError>;

impl Worker {
    const WORKER_TIMEOUT: Duration = Duration::from_secs(300);

    #[instrument(skip(self), fields(id = ?self.id, range = ?self.range))]
    pub fn spawn(self) -> WorkerFuture {
        let range = self.range.clone();
        let id = self.id;
        let fut = async move {
            match self.range {
                Some(ref rng) if rng.ranges_len() == 1 => self.download_continuous().await,
                Some(_) => self.download_spare().await,
                None => self.download_any().await,
            }
        };
        spawn(async move {
            timeout(Self::WORKER_TIMEOUT, fut).await.unwrap_or({
                let mut failed = WorkFailed::builder().id(id).err(Timeout).build();
                failed.revert = range;
                Err(failed)
            })
        })
    }

    #[instrument(skip(file, rate_limit))]
    async fn download_any_impl(url: Url, mut file: File, rate_limit: SharedRateLimiter) -> InnerResult {
        let resp = GLOBAL_HTTP_CLIENT.get(url)?.send().await?;
        let mut body_stream = resp.bytes_stream();
        let mut file = FileCursor::from(&mut file);
        while let Some(body_res) = body_stream.next().await {
            let body = body_res?;
            let n = {
                let len = body.len();
                len.try_into().map_err(|_| ChunkTooLarge(len))?
            };
            rate_limit.acquire(n).await?;
            file.write_all(body).await?;
        }
        Ok(file.into_range())
    }

    /// 用于不知道目标文件大小的下载。
    /// 通常用于需要单线程下载或不知道目标大小的文件，降级到使用单个 worker 并使用 download_any 下载。
    /// 对于此函数，如果成功总是返回成功下载的 range, 如果失败则返回 None 与 错误
    #[instrument(skip(self))]
    async fn download_any(self) -> WorkerResult {
        let cancel_err = Err(WorkFailed::builder().id(self.id).err(InitiateCancel).build());
        let handle_res = |res: InnerResult| {
            res.map(|downloaded| WorkSuccess { id: self.id, downloaded })
                .map_err(|err| WorkFailed::builder().id(self.id).err(err).build())
        };
        select_biased! {
            res = Self::download_any_impl(self.url, self.file, self.rate_limit).fuse() => {
                handle_res(res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                cancel_err
            }
        }
    }

    #[instrument(skip(file, rate_limit), fields(url = %url))]
    async fn download_continuous_impl(
        full: &FileMultiRange, url: Url, mut file: File, rate_limit: SharedRateLimiter,
    ) -> InnerResult {
        let start = *full.ranges().next().expect("continuous download requires a start").start() as u64;
        let resp = GLOBAL_HTTP_CLIENT.get(url)?.header("Range", full.as_header_value().unwrap())?.send().await?;
        let mut body_stream = resp.bytes_stream();
        let mut file = FileCursor::with_position(&mut file, start);
        while let Some(body_res) = body_stream.next().await {
            let body = body_res?;
            let n = {
                let len = body.len();
                len.try_into().map_err(|_| ChunkTooLarge(len))?
            };
            rate_limit.acquire(n).await?;
            file.write_all(body).await?;
        }
        Ok(file.into_range())
    }

    /// 用于一个连续的文件切片下载，调用此函数时假设 range 不为空
    /// 此函数成功时返回成功下载的范围，失败时返回取消提交的范围和错误
    #[instrument(skip(self))]
    async fn download_continuous(self) -> WorkerResult {
        let full = self.range.unwrap();
        let cancel_err = Err(WorkFailed::builder().id(self.id).revert(full.clone()).err(InitiateCancel).build());
        let handle_res = |res: InnerResult| {
            res.and_then(|downloaded| {
                if downloaded != full {
                    Err(ParitalDownloaded(downloaded))
                } else {
                    Ok(downloaded)
                }
            })
            .map(|downloaded| WorkSuccess { id: self.id, downloaded })
            .map_err(|err| WorkFailed::builder().id(self.id).revert(full.clone()).err(err).build())
        };
        select_biased! {
            res = Self::download_continuous_impl(&full, self.url, self.file, self.rate_limit).fuse() => {
                handle_res(res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                cancel_err
            }
        }
    }

    #[instrument(skip(file, rate_limit), fields(url = %url))]
    async fn download_spare_impl(
        full: &FileMultiRange, url: Url, mut file: File, rate_limit: SharedRateLimiter,
    ) -> InnerResult {
        let resp = GLOBAL_HTTP_CLIENT.get(url)?.header("Range", full.as_header_value().unwrap())?.send().await?;
        let boundary = resp.headers().parse_boundary()?;
        let body_stream = resp.bytes_stream();
        let mut multipart = MultipartStream::new(body_stream, &boundary);
        let mut total_downloaded = FileMultiRange::default();
        while let Some(part_res) = multipart.next().await {
            let part = part_res?;
            let hdr = part.headers();
            let (part_full, _) = hdr.parse_content_range()?;
            if !full.is_superset_for(part_full) {
                continue; // 如果这个 part 的范围不在被分配的范围中，则跳过
            }
            let mut body = part.body();
            let mut part_downloaded = FileMultiRange::default();
            while let Ok(Some(payload)) = body.try_next().await {
                let len = payload.len();
                let cur = part_downloaded.last().map_or(part_full.start, |n| n + 1);
                let recv_rng: FileRange = (cur..cur + len).into();
                if !part_full.contains(recv_rng) {
                    continue; // 如果实际收到的文件范围不在头的指示中，也跳过
                }
                let n = {
                    let len = payload.len();
                    len.try_into().map_err(|_| ChunkTooLarge(len))?
                };
                rate_limit.acquire(n).await?;
                file.write_all_at(payload, cur as u64).await.0.inspect(|_| part_downloaded.insert_range(recv_rng))?;
            }
            total_downloaded.union_assign(&part_downloaded);
        }
        Ok(total_downloaded)
    }

    /// 用于稀疏文件切片下载
    #[instrument(skip(self))]
    async fn download_spare(self) -> WorkerResult {
        let full = self.range.unwrap();
        let cancel_err = Err(WorkFailed::builder().id(self.id).revert(full.clone()).err(InitiateCancel).build());
        let handle_res = |res: InnerResult| {
            res.and_then(|downloaded| {
                if downloaded != full {
                    Err(ParitalDownloaded(downloaded))
                } else {
                    Ok(downloaded)
                }
            })
            .map(|downloaded| WorkSuccess { id: self.id, downloaded })
            .map_err(|err| WorkFailed::builder().id(self.id).revert(full.clone()).err(err).build())
        };
        select_biased! {
            res = Self::download_spare_impl(&full, self.url, self.file, self.rate_limit).fuse() => {
                handle_res(res)
            }
            _ = self.child_cancel.cancelled().fuse() => {
                cancel_err
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::{BufResult, io::AsyncReadAtExt};
    use compio_watch as watch;
    use std::{
        os::fd::{FromRawFd, IntoRawFd},
        vec,
    };
    use tempfile::NamedTempFile;
    use url::Url;

    // 记得给 fd 设置非阻塞
    fn mock_worker(rng: FileMultiRange) -> (File, Worker) {
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let cancel = CancellationToken::new();
        let std_file = NamedTempFile::new().unwrap().into_file();
        let file = unsafe { compio::fs::File::from_raw_fd(std_file.into_raw_fd()) };
        let (tx, _rx) = watch::channel(TaskStatus::default());
        let worker = Worker::builder()
            .id(TaskId::new())
            .url(url)
            .file(file.clone())
            .range(rng)
            .child_cancel(cancel)
            .rate_limit(rate_limiter)
            .status(tx)
            .build();
        (file.clone(), worker)
    }

    #[compio::test]
    async fn test_spare_worker() {
        let rng: FileMultiRange = ([0..=128, 200..=296].as_slice()).into();
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
        let rng: FileMultiRange = (64..=128).into();
        let (file, worker) = mock_worker(rng.clone());
        let result = worker.spawn().await.unwrap();
        println!("{:?}", result);
        assert_eq!(result.unwrap().downloaded, rng);
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
        let std_file = NamedTempFile::new().unwrap().into_file();
        let file = unsafe { compio::fs::File::from_raw_fd(std_file.into_raw_fd()) };

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
