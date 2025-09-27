use crate::{
    http::{
        file_range::{FileMultiRange, IntoRangeHeader},
        header_map_ext::{HeaderMapExt, HeaderMapExtError},
    },
    utils::rate_limiter::SharedRateLimiter,
};
use futures_util::{StreamExt, TryStreamExt};
use multipart_async_stream::{LendingIterator, MultipartStream};
use reqwest::Client;
use thiserror::Error;
use tokio::{spawn, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use url::Url;

pub struct Worker {
    url: Url,
    range: Option<FileMultiRange>,
    client: Client,
    file: (),
    child_cancel: CancellationToken,
    rate_limit: SharedRateLimiter,
}

#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("reqwest internal error: {0:?}")]
    ReqwestInternal(#[from] reqwest::Error),
    #[error("")]
    HeaderExt(#[from] HeaderMapExtError),
    #[error("")]
    ParitalDownloaded(FileMultiRange),
    #[error("")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("")]
    MultiPartStream(#[from] multipart_async_stream::Error),
    #[error("")]
    RateLimit(#[from] governor::InsufficientCapacity),
}
// <已下载，(注销提交，错误)>
pub type WorkerResult = Result<FileMultiRange, (Option<FileMultiRange>, DownloadError)>;
pub type WorkerFuture = JoinHandle<WorkerResult>;
impl Worker {
    pub fn new(
        url: &Url, range: Option<FileMultiRange>, client: Client, file: (), child_cancel: CancellationToken,
        rate_limit: SharedRateLimiter,
    ) -> Self {
        Self { url: url.clone(), range, client, file, child_cancel, rate_limit }
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
    async fn download_any(self) -> WorkerResult {
        let resp = self
            .client
            .get(self.url)
            .send()
            .await
            .map_err(|err| (FileMultiRange::default().into(), err.into()))?
            .error_for_status()
            .map_err(|err| (FileMultiRange::default().into(), err.into()))?;
        let mut body_stream = resp.bytes_stream();
        let mut downloaded = FileMultiRange::default();
        while let Some(body_result) = body_stream.next().await {
            let body = body_result.map_err(|err| (FileMultiRange::default().into(), err.into()))?;
            downloaded.push_n_at(0, body.len());
        }
        Ok(downloaded)
    }

    /// 用于一个连续的文件切片下载
    async fn download_continuous(self) -> WorkerResult {
        let full = self.range.unwrap();
        let resp = self
            .client
            .get(self.url)
            .header("Range", full.into_header_value().unwrap())
            .send()
            .await
            .map_err(|err| (full.clone().into(), err.into()))?
            .error_for_status()
            .map_err(|err| (full.clone().into(), err.into()))?;
        let mut body_stream = resp.bytes_stream();
        let mut downloaded = FileMultiRange::default();
        while let Some(body_result) = body_stream.next().await {
            let body = body_result.map_err(|err| (full.clone().into(), err.into()))?;
            self.rate_limit.acquire(body.len() as u32).await.map_err(|err| (full.clone().into(), err.into()))?;
            downloaded.push_n_at(full.first().unwrap(), body.len());
        }
        if full != downloaded {
            return Err((full.into(), DownloadError::ParitalDownloaded(downloaded)));
        }
        Ok(downloaded)
    }

    /// 用于稀疏文件切片下载
    async fn download_spare(self) -> WorkerResult {
        let full = self.range.unwrap();
        let resp = self
            .client
            .get(self.url)
            .header("Range", full.into_header_value().unwrap())
            .send()
            .await
            .map_err(|err| (full.clone().into(), err.into()))?
            .error_for_status()
            .map_err(|err| (full.clone().into(), err.into()))?;
        let boundary = resp.headers().parse_boundary().map_err(|err| (full.clone().into(), err.into()))?;
        let body_stream = resp.bytes_stream();
        let mut multipart = MultipartStream::new(body_stream, &boundary);
        let mut downloaded = FileMultiRange::default();
        while let Some(part_result) = multipart.next().await {
            let part = part_result.map_err(|err| (full.clone().into(), err.into()))?;
            let hdr = part.headers();
            let (rng, _) = hdr.parse_content_range().map_err(|err| (full.clone().into(), err.into()))?;
            let mut body = part.body();
            while let Ok(Some(b)) = body.try_next().await {
                // todo： 文件拓展提供一个 file window 抽象，一旦超出window 就报错
                println!("recv bytes: {:?}\n", b);
            }
            downloaded.insert_range(rng);
        }
        if full != downloaded {
            return Err((full.into(), DownloadError::ParitalDownloaded(downloaded)));
        }
        Ok(downloaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    fn mock_worker(rng: FileMultiRange) -> Worker {
        let client = reqwest::Client::new();
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let cancel = CancellationToken::new();
        Worker::new(&url, Some(rng), client, (), cancel.child_token(), rate_limiter)
    }
    #[tokio::test]
    async fn test_spare_worker() {
        let rng: FileMultiRange = ([0..=128, 200..=296].as_slice()).into();
        let worker = mock_worker(rng.clone());
        let result = worker.run().await.unwrap();
        assert_eq!(result.ok(), Some(rng));
    }

    #[tokio::test]
    async fn test_continuous_worker() {
        let rng: FileMultiRange = (64..=128).into();
        let worker = mock_worker(rng.clone());
        let result = worker.run().await.unwrap();
        assert_eq!(result.ok(), Some(rng));
    }
}
