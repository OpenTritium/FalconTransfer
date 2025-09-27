use crate::{
    http::{
        file_range::{FileMultiRnage, FileRange, IntoRangeHeader},
        header_map_ext::{HeaderMapExt, HeaderMapExtError},
    },
    utils::{rate_limiter::SharedRateLimiter, safe_filename::SafeFileName},
};
use camino::{Utf8Path, Utf8PathBuf};
use fastdate::DateTime;
use futures_util::{
    FutureExt, StreamExt, TryStreamExt,
    future::{Lazy, lazy},
    stream::FuturesUnordered,
};
use mime::{APPLICATION_OCTET_STREAM, Mime};
use multipart_async_stream::{LendingIterator, MultipartStream};
use rand::seq::IndexedRandom;
use reqwest::{Client, Response, header::CONTENT_TYPE};
use sanitize_filename_reader_friendly::sanitize;
use std::{
    collections::{HashMap, VecDeque},
    default,
    fs::File,
    mem,
    num::NonZeroU32,
    ops::Not,
    range,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    task::Context,
};
use thiserror::Error;
use tokio::{
    select, spawn,
    sync::{broadcast, mpsc, watch},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;
use url::Url;

pub struct HttpTaskMeta {
    url: Url,
    name: SafeFileName,
    size: Option<usize>,
    mime: Mime,
    ranges_support: bool,
}

impl HttpTaskMeta {
    fn name(&self) -> &str { self.name.as_str() }

    fn path(&self) -> &Utf8Path { &self.name }

    fn content_range(&self) -> Option<FileMultiRnage> {
        let end = self.size? - 1;
        let rgn = 0..=end;
        Some(FileMultiRnage::from(rgn))
    }
}

impl From<Response> for HttpTaskMeta {
    fn from(resp: Response) -> Self {
        fn parse_filename_from_url(url: &Url) -> Option<String> {
            url.path_segments()
                .and_then(|mut segs| segs.next_back())
                .and_then(|name| name.is_empty().then_some(name.to_string()))
        }

        let headers = resp.headers();
        let url = resp.url();
        let content_length = headers.parse_content_length();
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<Mime>().ok())
            .unwrap_or(APPLICATION_OCTET_STREAM);
        let filename = headers
            .parse_filename()
            .ok()
            .or_else(|| parse_filename_from_url(url))
            .map(|ref s| sanitize(s))
            .unwrap_or_else(|| format!("{}.bin", DateTime::now()))
            .as_str()
            .into();
        let ranges_support = headers.parse_accept_ranges();
        Self { name: filename, size: content_length, mime: content_type, ranges_support, url: url.clone() }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TaskCommand {
    ChangeRateLimited(Option<NonZeroU32>),
    Pause,
    Resume,
    Cancel,
}

struct Task {
    name: String,
    path: Utf8PathBuf,
    cmd: mpsc::Sender<TaskCommand>,
    controller: LazyHandle,
    status: watch::Receiver<TaskStatus>,
    meta: Arc<HttpTaskMeta>,
}

#[derive(Debug, Error)]
enum TaskError {
    #[error("task was running already")]
    AlreadyRun,
    #[error("")]
    AlreadyPause,
    #[error("")]
    AlreadyCancelOrFinished,
    #[error("failed to send cmd: {cmd:?} via channel, err: {err:?}")]
    ChannelError { cmd: TaskCommand, err: mpsc::error::SendError<TaskCommand> },
    #[error("")]
    JoinError(#[from] JoinError),
}

impl Task {
    fn new(
        name: String, path: &Utf8Path, meta: HttpTaskMeta, qos: broadcast::Receiver<QosAdvice>,
        rate_limit: SharedRateLimiter, client: Client,
    ) -> Self {
        let meta = Arc::new(meta);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (status_tx, status_rx) = watch::channel(Default::default());
        let meta_clone = meta.clone();
        let controller = LazyHandle::Ready(lazy(Box::new(|_| {
            let controller = ControllActor {
                commited: Default::default(),
                concurrency: AtomicUsize::new(32),
                file: (),
                cmd_rx,
                qos_rx: qos,
                workers: FuturesUnordered::new(),
                status: status_tx,
                rate_limit,
                retries_left: 3,
                meta: meta_clone,
                client,
                parent_cancel: CancellationToken::new().into(),
            };
            controller.run()
        })));
        Self { name, path: path.into(), cmd: cmd_tx, controller, status: status_rx, meta }
    }

    async fn run(&mut self) -> Result<(), TaskError> {
        use LazyHandle::*;
        if let Ready(ref mut lazy_handle) = self.controller {
            let join_handle = lazy_handle.await;
            self.controller = Running(join_handle);
            return Ok(());
        }
        Err(TaskError::AlreadyRun)
    }

    async fn send_cmd(&self, cmd: TaskCommand) -> Result<(), mpsc::error::SendError<TaskCommand>> {
        // todo： 接入日志
        self.cmd.send(cmd).await
    }

    async fn pause(&self) -> Result<(), TaskError> {
        use LazyHandle::*;
        use TaskCommand::*;
        use TaskError::*;
        if self.status.borrow().flag_pending && matches!(self.controller, Running(_)) {
            return Err(AlreadyPause);
        }
        self.send_cmd(Pause).await.map_err(|err| ChannelError { cmd: Pause, err })?;
        Ok(())
    }

    async fn resume(&self) -> Result<(), TaskError> {
        use LazyHandle::*;
        use TaskCommand::*;
        use TaskError::*;
        if self.status.borrow().flag_pending.not() && matches!(self.controller, Running(_)) {
            return Err(AlreadyRun);
        }
        self.send_cmd(Pause).await.map_err(|err| ChannelError { cmd: Pause, err })?;
        Ok(())
    }

    // 约定好调用此方法后不可再调用任何通道
    async fn cancel(&mut self) -> Result<(), TaskError> {
        use LazyHandle::*;
        use TaskCommand::*;
        use TaskError::*;
        match mem::take(&mut self.controller) {
            Ready(_) => Ok(()),
            Running(handle) => {
                self.send_cmd(Cancel).await.map_err(|err| ChannelError { cmd: Cancel, err })?;
                handle.await?;
                Ok(())
            }
            None => Err(AlreadyCancelOrFinished),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum QosAdvice {}

// todo: 使用闭包代替
type LazyJoinHandle<T> = Lazy<Box<dyn FnOnce(&mut Context<'_>) -> JoinHandle<T>>>;

#[derive(Default)]
enum LazyHandle {
    Ready(LazyJoinHandle<()>),
    Running(JoinHandle<()>),
    #[default]
    None, // 任务被取消或完成
}

#[derive(Debug, Default)]
struct TaskStatus {
    total: FileMultiRnage, // 用于展示下载总量，当目标大小未知时，与已下载量同步
    downloaded: FileMultiRnage,
    flag_pending: bool,
    flag_eof: bool, // 用于表达流已经被动终止或主动终止
    last_err: Option<DownloadError>,
}

struct ControllActor {
    meta: Arc<HttpTaskMeta>,
    client: Client,
    commited: FileMultiRnage, /* 用于记录任务分配的工作量，如果任务只完成了部分工作量，则归还。
                               * 这是为了解决分配工作量给任务时，无法及时反馈会导致工作量会被重复分配 */
    concurrency: AtomicUsize,
    retries_left: u8,
    file: (),                               // 并发写入文件实例
    cmd_rx: mpsc::Receiver<TaskCommand>,    // 命令邮箱
    qos_rx: broadcast::Receiver<QosAdvice>, // qos 邮箱
    parent_cancel: CancellationToken,
    workers: FuturesUnordered<WorkerFuture>, // 工作者队列
    status: watch::Sender<TaskStatus>,       // 任务
    rate_limit: SharedRateLimiter,           // 令牌桶
}

impl ControllActor {
    const BLOCK_SIZE: usize = 0x100_0000;

    fn run(mut self) -> JoinHandle<()> {
        spawn(async move {
            // 还有个分配工作量线程
            use DownloadError::*;
            use TaskCommand::*;
            let join_worker = self.workers.next().map(|o| o.map(|outter| outter.map_err(JoinError).flatten()));
            select! {
                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_cmd(cmd);
                }
                Ok(qos) = self.qos_rx.recv() => {
                    unimplemented!()
                }
                work_result  = join_worker => {
                    self.handle_worker_result(work_result);
                }
            }
        })
    }

    /// 根据总量计算没有提交的部分
    fn calculate_not_commited(&self) -> Option<FileMultiRnage> {
        self.meta.content_range().as_ref().map(|total| total - &self.commited)
    }

    #[inline]
    fn handle_cmd(&mut self, cmd: TaskCommand) {
        use TaskCommand::*;
        match cmd {
            ChangeRateLimited(limit) => {
                self.rate_limit.change_limit(limit);
            }
            Pause => {
                self.status.send_if_modified(|status| {
                    let need_update = status.flag_pending ^ false;
                    status.flag_pending = true;
                    need_update
                });
            }
            Resume => {
                self.status.send_if_modified(|status| {
                    let need_update = status.flag_pending ^ true;
                    status.flag_pending = false;
                    need_update
                });
            }
            Cancel => {
                // 设置eof让工作者队列停止继续分配，
                self.status.send_if_modified(|status| {
                    let need_update = status.flag_eof ^ true;
                    status.flag_eof = true;
                    need_update
                });
                // 销毁所有工作者
                // 销毁文件
                // 销毁控制器
            }
        }
    }

    /// 在当前并发数的限制下，创建若干工作者，按照块大小分配工作量，此函数返回0 则代表工作量不足以分配新工作者
    fn spwan_many_worker(&mut self) -> usize {
        if self.status.borrow().flag_eof {
            return 0;
        }
        let Some(mut not_commited) = self.calculate_not_commited() else {
            return 0;
        };
        let mut window_iter = not_commited.window(Self::BLOCK_SIZE);
        let more_worker_count = self.concurrency.load(Ordering::Relaxed).saturating_sub(self.workers.len());
        let mut acc = 0;
        while let Some(rng) = window_iter.next()
            && more_worker_count > acc
        {
            acc += 1;
            self.spawn_and_push_worker(Some(rng));
        }
        acc
    }

    /// 创建一个线程，用于不知道目标大小或只支持单线程的链接
    /// 这个不需要考虑提交，只需要更新下载内容和全部内容即可
    fn spawn_single_worker(&mut self) {
        if self.status.borrow().flag_eof {
            return;
        }
        self.spawn_and_push_worker(None);
    }

    /// 根据 range 自动创建 worker 并 push 进队列
    fn spawn_and_push_worker(&mut self, range: Option<FileMultiRnage>) {
        let worker = Worker::new(
            &self.meta.url,
            range,
            self.client.clone(),
            self.file,
            self.parent_cancel.child_token(),
            self.rate_limit.clone(),
        );
        let fut = worker.run();
        self.workers.push(fut);
    }

    fn handle_worker_result(&mut self, result: Option<WorkerResult>) {
        use DownloadError::*;
        match result {
            Some(Ok(rng)) => {
                self.status.send_if_modified(|status| {
                    if rng.is_empty() {
                        return false;
                    }
                    status.downloaded |= rng;
                    true
                });
                if self.meta.ranges_support && self.spwan_many_worker() == 0 {
                    self.status.send_modify(|status| status.flag_eof = true);
                }
            }
            Some(Err(ParitalDownloaded { full, partial })) => {
                let not_downloaded = &full - &partial;
                self.status.send_if_modified(|status| {
                    status.downloaded |= partial;
                    if not_downloaded.is_empty() {
                        return false;
                    }
                    self.commited |= not_downloaded;
                    true
                });
                if self.meta.ranges_support && self.spwan_many_worker() == 0 {
                    self.status.send_modify(|status| status.flag_eof = true);
                }
            }

            None if self.meta.ranges_support && !self.status.borrow().flag_eof => {
                if self.spwan_many_worker() == 0 {
                    self.status.send_modify(|status| status.flag_eof = true);
                }
            }
            None if !self.status.borrow().flag_eof => {
                self.spawn_single_worker();
            }
            // fallback 到这里说明 eof 被设置了
            None => {
                // 没有工作者且设置了终止 flag 才结束
            }
            Some(Err(err)) => unimplemented!(),
        }
        return;
    }
}

struct Worker {
    url: Url,
    range: Option<FileMultiRnage>, //有可能会被分配不仅一块切片，如果none 则
    client: Client,
    file: (),
    child_cancel: CancellationToken,
    rate_limit: SharedRateLimiter,
}

#[derive(Debug, Error)]
enum DownloadError {
    #[error("reqwest internal error: {0:?}")]
    ReqwestInternal(#[from] reqwest::Error),
    #[error("")]
    HeaderExt(#[from] HeaderMapExtError),
    #[error("")]
    ParitalDownloaded { full: FileMultiRnage, partial: FileMultiRnage },
    #[error("")]
    JoinError(#[from] JoinError),
    #[error("")]
    MultiPartStream(#[from] multipart_async_stream::Error),
    #[error("")]
    RateLimit(#[from] governor::InsufficientCapacity),
}

type WorkerResult = Result<FileMultiRnage, DownloadError>;
type WorkerFuture = JoinHandle<WorkerResult>;
impl Worker {
    fn new(
        url: &Url, range: Option<FileMultiRnage>, client: Client, file: (), child_cancel: CancellationToken,
        rate_limit: SharedRateLimiter,
    ) -> Self {
        Self { url: url.clone(), range, client, file, child_cancel, rate_limit }
    }

    /// 如果下载完成，返回所有 Range，如果部分成功则返回一个异常（所有和已经下载的 Range）
    fn run(self) -> WorkerFuture {
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
        let resp = self.client.get(self.url).send().await?.error_for_status()?;
        let mut body_stream = resp.bytes_stream();
        let mut downloaded = FileMultiRnage::default();
        while let Some(body_result) = body_stream.next().await {
            let body = body_result?;
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
            .await?
            .error_for_status()?;
        let mut body_stream = resp.bytes_stream();
        let mut downloaded = FileMultiRnage::default();
        while let Some(body_result) = body_stream.next().await {
            let body = body_result?;
            self.rate_limit.acquire(body.len() as u32).await?;
            downloaded.push_n_at(full.first().unwrap(), body.len());
        }
        if full != downloaded {
            return Err(DownloadError::ParitalDownloaded { full, partial: downloaded });
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
            .await?
            .error_for_status()?;
        let boundary = resp.headers().parse_boundary()?;
        let body_stream = resp.bytes_stream();
        let mut multipart = MultipartStream::new(body_stream, &boundary);
        let mut downloaded = FileMultiRnage::default();
        while let Some(part_result) = multipart.next().await {
            let part = part_result?;
            let hdr = part.headers();
            let (rng, _) = hdr.parse_content_range()?;

            let mut body = part.body();
            while let Ok(Some(b)) = body.try_next().await {
                // todo： 文件拓展提供一个 file window 抽象，一旦超出window 就报错
                println!("recv bytes: {:?}\n", b);
            }
            downloaded.insert_range(rng);
        }
        if full != downloaded {
            return Err(DownloadError::ParitalDownloaded { full, partial: downloaded });
        }
        Ok(downloaded)
    }
}

#[cfg(test)]
mod tests {
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use crate::{
        http::{file_range::FileMultiRnage, task::Worker},
        utils::rate_limiter::SharedRateLimiter,
    };

    fn mock_worker(rng: FileMultiRnage) -> Worker {
        let client = reqwest::Client::new();
        let rate_limiter = SharedRateLimiter::new_with_no_limit();
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let cancel = CancellationToken::new();
        Worker::new(&url, Some(rng), client, (), cancel.child_token(), rate_limiter)
    }
    #[tokio::test]
    async fn test_spare_worker() {
        let rng: FileMultiRnage = ([0..=128, 200..=296].as_slice()).into();
        let worker = mock_worker(rng.clone());
        let result = worker.run().await.unwrap();
        assert_eq!(result.ok(), Some(rng));
    }

    #[tokio::test]
    async fn test_continuous_worker() {
        let rng: FileMultiRnage = (64..=128).into();
        let worker = mock_worker(rng.clone());
        let result = worker.run().await.unwrap();
        assert_eq!(result.ok(), Some(rng));
    }
}
