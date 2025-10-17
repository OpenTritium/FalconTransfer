//! worker 是 thread local 的,每个 worker 只向 threadlocal 的 controller 汇报
//! 它是任务无关的
use crate::{
    http::{
        file_range::{FileCursor, FileMultiRange, FileRange, IntoRangeHeader},
        header_map_ext::{HeaderMapExt, HeaderMapExtError},
        status::TaskStatus,
    },
    utils::rate_limiter::SharedRateLimiter,
};
use compio::{
    fs::File,
    io::AsyncWriteAtExt,
    runtime::{JoinHandle, spawn},
};
use compio_watch as watch;
use cyper::Client;
use futures_util::{StreamExt, TryStreamExt};
use identity::task::TaskId;
use multipart_async_stream::{LendingIterator, MultipartStream};
use smol_cancellation_token::CancellationToken;
use thiserror::Error;
use url::Url;

// todo id
pub struct Worker {
    id: TaskId,
    url: Url,
    range: Option<FileMultiRange>,
    client: Client,
    file: File,
    status: watch::Sender<TaskStatus>,
    child_cancel: CancellationToken,
    rate_limit: SharedRateLimiter,
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
}

//todo 实现取消
#[derive(Debug)]
pub struct WorkSuccess {
    pub id: TaskId,
    pub downloaded: FileMultiRange,
}

#[derive(Debug)]
pub struct WorkFailed {
    pub id: TaskId,
    pub revert: Option<FileMultiRange>,
    pub err: WorkerError,
}

pub type WorkerResult = Result<WorkSuccess, WorkFailed>;
pub type WorkerFuture = JoinHandle<WorkerResult>;

// todo 文件写入错误处理
impl Worker {
    pub fn new(
        id: TaskId, url: &Url, range: Option<FileMultiRange>, client: Client, file: File,
        child_cancel: CancellationToken, rate_limit: SharedRateLimiter, status: watch::Sender<TaskStatus>,
    ) -> Self {
        Self { url: url.clone(), range, client, file, child_cancel, rate_limit, id, status }
    }

    /// 如果下载完成，返回所有 Range，如果部分成功则返回一个异常（所有和已经下载的 Range）
    pub fn run(self) -> WorkerFuture {
        spawn(async move {
            match self.range {
                Some(ref rng) if rng.ranges_len() == 1 => self.download_continuous().await,
                Some(_) => self.download_spare().await,
                None => self.download_any().await,
            }
        })
    }

    /// 用于不知道目标文件大小的下载。
    /// 通常用于需要单线程下载或不知道目标大小的文件，降级到使用单个 worker 并使用 download_any 下载。
    /// 对于此函数，如果成功总是返回成功下载的 range, 如果失败则返回 None 与 错误
    async fn download_any(mut self) -> WorkerResult {
        let err_without_uncommit = |err: cyper::Error| WorkFailed { id: self.id, revert: None, err: err.into() };
        let resp =
            self.client.get(self.url).map_err(err_without_uncommit)?.send().await.map_err(err_without_uncommit)?;
        let mut body_stream = resp.bytes_stream();
        let mut file = FileCursor::from(&mut self.file);
        while let Some(body_result) = body_stream.next().await {
            let body = body_result.map_err(err_without_uncommit)?;
            self.rate_limit.acquire(body.len() as u32).await.map_err(|err| WorkFailed {
                id: self.id,
                revert: None,
                err: err.into(),
            })?;
            file.write_all(body).await.map_err(|err| WorkFailed { id: self.id, revert: None, err: err.into() })?;
        }
        Ok(WorkSuccess { id: self.id, downloaded: file.into_range() })
    }

    /// 用于一个连续的文件切片下载，调用此函数时假设 range 不为空
    /// 此函数成功时返回成功下载的范围，失败时返回取消提交的范围和错误
    async fn download_continuous(mut self) -> WorkerResult {
        let full = self.range.unwrap();
        let start_offset = *full.ranges().next().expect("Continuous download requires a start").start() as u64;
        let err_with_uncommit =
            |err: cyper::Error| WorkFailed { id: self.id, revert: full.clone().into(), err: err.into() };
        let resp = self
            .client
            .get(self.url)
            .map_err(err_with_uncommit)?
            .header("Range", full.into_header_value().unwrap())
            .map_err(err_with_uncommit)?
            .send()
            .await
            .map_err(err_with_uncommit)?;
        let mut body_stream = resp.bytes_stream();
        let mut file = FileCursor::with_position(&mut self.file, start_offset);
        while let Some(body_result) = body_stream.next().await {
            let body = body_result.map_err(err_with_uncommit)?;
            self.rate_limit.acquire(body.len() as u32).await.map_err(|err| WorkFailed {
                id: self.id,
                revert: full.clone().into(),
                err: err.into(),
            })?;
            // 取消提交下载失败的部分
            file.write_all(body).await.map_err(|err| WorkFailed {
                id: self.id,
                revert: full.clone().into(),
                err: err.into(),
            })?;
        }
        let downloaded = file.into_range();
        // 如果流提前终止，返回部分下载错误
        if full != downloaded {
            return Err(WorkFailed {
                id: self.id,
                revert: full.into(),
                err: WorkerError::ParitalDownloaded(downloaded),
            });
        }
        Ok(WorkSuccess { id: self.id, downloaded })
    }

    /// 用于稀疏文件切片下载
    async fn download_spare(mut self) -> WorkerResult {
        let assigned_rng = self.range.unwrap();
        let clnt_err_with_uncommit =
            |err: cyper::Error| WorkFailed { id: self.id, revert: assigned_rng.clone().into(), err: err.into() };
        let resp = self
            .client
            .get(self.url)
            .map_err(clnt_err_with_uncommit)?
            .header("Range", assigned_rng.into_header_value().unwrap())
            .map_err(clnt_err_with_uncommit)?
            .send()
            .await
            .map_err(clnt_err_with_uncommit)?;
        let hdr_err_with_uncommit =
            |err: HeaderMapExtError| WorkFailed { id: self.id, revert: assigned_rng.clone().into(), err: err.into() };
        let boundary = resp.headers().parse_boundary().map_err(hdr_err_with_uncommit)?;
        let body_stream = resp.bytes_stream();
        let mut multipart = MultipartStream::new(body_stream, &boundary);
        let mut total_downloaded = FileMultiRange::default();
        while let Some(part_result) = multipart.next().await {
            let part = part_result.map_err(|err| WorkFailed {
                id: self.id,
                revert: assigned_rng.clone().into(),
                err: err.into(),
            })?;
            let hdr = part.headers();
            let (spare_full, _) = hdr.parse_content_range().map_err(hdr_err_with_uncommit)?;
            if !assigned_rng.is_superset_for(spare_full) {
                // 如果这个 part 的范围不在被分配的范围中，则跳过
                break;
            }
            let mut body = part.body();
            let mut spare_downloaded = FileMultiRange::default();
            while let Ok(Some(payload)) = body.try_next().await {
                let len = payload.len();
                let cur = spare_downloaded.last().map_or(spare_full.start, |n| n + 1);
                let recv_rng: FileRange = (cur..cur + len).into();
                if !spare_full.contains(recv_rng) {
                    // 如果实际收到的文件范围不在头的指示中，也跳出
                    break;
                }
                self.file
                    .write_all_at(payload, cur as u64)
                    .await
                    .0
                    .inspect(|_| spare_downloaded.insert_range(recv_rng))
                    .map_err(|err| WorkFailed { id: self.id, revert: assigned_rng.clone().into(), err: err.into() })?;
            }
            total_downloaded |= spare_downloaded;
        }
        if assigned_rng != total_downloaded {
            return Err(WorkFailed {
                id: self.id,
                revert: assigned_rng.clone().into(),
                err: WorkerError::ParitalDownloaded(total_downloaded),
            });
        }
        Ok(WorkSuccess { id: self.id, downloaded: total_downloaded })
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
        let client = cyper::Client::new();
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let cancel = CancellationToken::new();
        let std_file = NamedTempFile::new().unwrap().into_file();
        let file = unsafe { compio::fs::File::from_raw_fd(std_file.into_raw_fd()) };
        let (tx, _rx) = watch::channel(TaskStatus::default());
        (
            file.clone(),
            Worker::new(TaskId::new(), &url, Some(rng), client, file, cancel.child_token(), rate_limiter, tx),
        )
    }

    #[compio::test]
    async fn test_spare_worker() {
        let rng: FileMultiRange = ([0..=128, 200..=296].as_slice()).into();
        let (file, worker) = mock_worker(rng.clone());
        let result = worker.run().await.unwrap();
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
        let result = worker.run().await.unwrap();
        println!("{:?}", result);
        assert_eq!(result.unwrap().downloaded, rng);
        let buf = vec![];
        let BufResult(res, buf) = file.read_to_end_at(buf, 0).await;
        res.unwrap();
        println!("{:?}", buf);
    }

    fn mock_worker_any() -> (File, Worker) {
        let client = cyper::Client::new();
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        // 使用一个较小的、已知大小的文件进行测试会更稳定和快速
        let url = Url::parse("https://www.rust-lang.org/static/images/rust-logo-blk.svg").unwrap();
        let cancel = CancellationToken::new();
        let std_file = NamedTempFile::new().unwrap().into_file();
        let file = unsafe { compio::fs::File::from_raw_fd(std_file.into_raw_fd()) };

        let (tx, _rx) = watch::channel(TaskStatus::default());
        // 注意这里 range 传入 None
        (file.clone(), Worker::new(TaskId::new(), &url, None, client, file, cancel.child_token(), rate_limiter, tx))
    }

    #[compio::test]
    async fn test_any_worker() {
        let (_file, worker) = mock_worker_any();
        let result = worker.run().await.unwrap();
        println!("{:?}", result);

        // 对于 download_any，我们不知道确切大小，但可以断言结果是 Ok 且范围不为空
        assert!(result.is_ok());
        assert!(!result.unwrap().downloaded.is_empty());
    }
}
