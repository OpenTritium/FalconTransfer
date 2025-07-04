use crate::{file::WritedRange, poll_state::PollState};
use tokio::{
    io::{
        AsyncReadExt, AsyncWrite, Error as IoError, ErrorKind as IoErrorKind, Result as IoResult,
    },
    sync::MutexGuard,
    task::JoinHandle,
};

pub(crate) async fn asyncify<F, T>(f: F) -> IoResult<T>
where
    F: FnOnce() -> IoResult<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|err| Err(IoError::new(IoErrorKind::Other, err)))
}

#[inline(always)]
pub fn spawn_mandatory_blocking<F, R>(f: F) -> Option<JoinHandle<R>>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let handle = tokio::runtime::Handle::try_current().ok()?;
    Some(handle.spawn_blocking(f))
}

#[inline(always)]
pub(crate) fn new_io_other_err(msg: &str) -> IoError {
    IoError::new(IoErrorKind::Other, msg)
}

pub trait SyncReadable: AsyncReadExt + Unpin + Send + 'static {
    fn read_offset(&self) -> impl Future<Output = u64> + Send;
    fn get_poll_state(&'_ self) -> impl Future<Output = MutexGuard<'_, PollState>> + Send;
    fn get_writed_range(&self) -> &WritedRange;

    /// 获取有效字节范围读取，返回的是读取的有效字节数
    /// 如果不交就返回未预期的结束符
    /// 如果发生错误，记得清理脏数据
    async fn sync_read(&mut self, mut dst: impl AsMut<[u8]>) -> IoResult<usize> {
        self.get_poll_state().await.complete_inflight().await;
        let start = self.read_offset().await + 1;
        let n = self.read(dst.as_mut()).await?;
        let end = start + n as u64 - 1;
        let sub = range_set_blaze::RangeSetBlaze::from_iter([start..=end]);
        let sup = self.get_writed_range().read().await.clone();
        if sup.is_disjoint(&sub) {
            // 不相交
            return Err(IoErrorKind::UnexpectedEof.into());
        }
        let its = &sub & &sup;
        let itv = its.ranges().next().unwrap();
        if *itv.start() != start {
            // 中间有空洞
            return Err(IoErrorKind::UnexpectedEof.into());
        }
        // 即使后面有空洞也返回空洞之前的字节数量
        return Ok(itv.count());
    }

    /// 读到最长没有空洞的地方，注意这并不代表文件结束了，你还可以移动游标继续读取
    async fn sync_read_to_end(&mut self, mut dst: impl AsMut<Vec<u8>>) -> IoResult<usize> {
        self.get_poll_state().await.complete_inflight().await;
        let start = self.read_offset().await + 1;
        let n = self.read_to_end(dst.as_mut()).await?;
        let end = start + n as u64 - 1;
        let sub = range_set_blaze::RangeSetBlaze::from_iter([start..=end]);
        let sup = self.get_writed_range().read().await.clone();
        if sup.is_disjoint(&sub) {
            // 不相交
            return Err(IoErrorKind::UnexpectedEof.into());
        }
        let its = &sub & &sup;
        let itv = its.ranges().next().unwrap();
        if *itv.start() != start {
            // 中间有空洞
            return Err(IoErrorKind::UnexpectedEof.into());
        }
        // 即使后面有空洞也返回空洞之前的字节数量
        return Ok(itv.count());
    }
}

/// 此trait 保证同步策略
pub trait SyncWritable: AsyncWrite + Send + 'static {
    async fn sync_data(&self) -> IoResult<()>;
    async fn sync_all(&self) -> IoResult<()>;
    async fn set_len(&self, size: u64) -> IoResult<()>;
    async fn write_offset(&mut self) -> u64;
    async fn sync_write_all(&mut self, src: impl AsRef<[u8]>) -> IoResult<()>;
    async fn sync_write(&mut self, src: impl AsRef<[u8]>) -> IoResult<usize>;
    async fn metadata(&self) -> IoResult<std::fs::Metadata>;
    async fn set_permissions(&self, perm: std::fs::Permissions) -> IoResult<()>;
}
