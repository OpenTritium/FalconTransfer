use crate::buffer::{self, Buffer, VectoredBuffer};
use compio::{
    BufResult,
    buf::{IoBuf, IoVectoredBuf, buf_try},
    fs::File,
    io::{AsyncWrite, AsyncWriteAt},
};
use sparse_ranges::RangeSet;
use std::{
    io::{self, Cursor},
    mem,
};

pub const MAX_BUF_SIZE: usize = 0x40 * 0x400 * 0x400;
pub const BASE_BUF_SIZE: usize = buffer::DEFAULT_BUF_SIZE;

pub struct SeqBufFile {
    file: Cursor<File>,
    buffered: RangeSet,
    flushed: RangeSet,
    buf: Buffer,
}

impl SeqBufFile {
    #[inline]
    pub fn with_position(file: File, pos: u64) -> Self {
        let mut cursor = Cursor::new(file);
        cursor.set_position(pos);
        Self {
            file: cursor,
            buffered: RangeSet::new(),
            flushed: RangeSet::new(),
            buf: Buffer::with_capacity(BASE_BUF_SIZE),
        }
    }

    #[inline]
    pub fn buffered_range(&self) -> &RangeSet { &self.buffered }

    #[inline]
    pub fn flushed_range(&self) -> &RangeSet { &self.flushed }

    #[inline]
    pub fn into_flushed_range(mut self) -> RangeSet { mem::take(&mut self.flushed) }

    async fn flush_if_needed(&mut self, next_chunk: Option<usize>) -> io::Result<()> {
        // 缓冲区空的，没有东西可以 flush
        if self.buf.is_empty() {
            return Ok(());
        }
        // 缓冲区有东西但是已经被写完了，直接重置缓冲区
        if self.buf.all_done() {
            self.buf.reset(); // 其实 compact 也可以
            return Ok(());
        }
        self.buf.compact(); // 现在compact 是为了让两个卫语句都处理 compact 后的情况
        // 前瞻性 flush
        if let Some(chunk_size) = next_chunk
            && {
                let new_remaining = self.buf.remaining_len() + chunk_size;
                new_remaining > MAX_BUF_SIZE
            }
        {
            self.flush().await?;
            return Ok(());
        }
        // 托底的回收方法
        if self.buf.remaining_len() > BASE_BUF_SIZE {
            self.flush().await?;
            self.buf.shrink_to(BASE_BUF_SIZE);
        }
        Ok(())
    }
}

impl AsyncWrite for SeqBufFile {
    async fn write<B: IoBuf>(&mut self, mut buf: B) -> BufResult<usize, B> {
        // The previous flush may error because disk full. We need to make the buffer all-done before writing new data
        // to it.
        let pos = self.file.position() as usize;
        let buf_len = buf.buf_len();
        (_, buf) = buf_try!(self.flush_if_needed(buf_len.into()).await, buf);
        let written = self.buf.read_from(buf.as_slice());
        self.buffered.insert_n_at(written, pos);
        (_, buf) = buf_try!(self.flush_if_needed(None).await, buf);
        (Ok(written), buf).into()
    }

    /// flush 缓冲区， flush 游标，然后同步 flushed range
    async fn flush(&mut self) -> io::Result<()> {
        let Self { file, buf, .. } = self;
        buf.flush_to(file).await?;
        self.flushed = self.buffered.clone();
        Ok(())
    }

    async fn write_vectored<B: IoVectoredBuf>(&mut self, mut buf: B) -> BufResult<usize, B> {
        let pos = self.file.position() as usize;
        let buf_len = buf.iter_slice().map(|x| x.len()).sum::<usize>();
        (_, buf) = buf_try!(self.flush_if_needed(buf_len.into()).await, buf);
        let mut total_written = 0;
        for buf in buf.iter_slice() {
            if buf.is_empty() {
                continue; // 避免写入空切片导致提前 read from 提前返回0
            }
            let written = self.buf.read_from(buf);
            if written == 0 {
                break; // 缓冲区写不下了，终止
            }
            total_written += written;
        }
        self.buffered.insert_n_at(total_written, pos);
        (_, buf) = buf_try!(self.flush_if_needed(None).await, buf);
        (Ok(total_written), buf).into()
    }

    async fn shutdown(&mut self) -> io::Result<()> { self.flush().await }
}

impl From<File> for SeqBufFile {
    #[inline]
    fn from(file: File) -> Self { Self::with_position(file, 0) }
}

impl Drop for SeqBufFile {
    fn drop(&mut self) { debug_assert!(self.buf.all_done(), "Buffer not all done") }
}

pub struct RandBufFile {
    file: File,
    vbuf: VectoredBuffer,
    buffered: RangeSet,
    flushed: RangeSet,
}

impl RandBufFile {
    #[inline]
    pub fn new(file: File) -> Self {
        Self { file, buffered: RangeSet::new(), flushed: RangeSet::new(), vbuf: VectoredBuffer::new() }
    }

    #[inline]
    pub fn buffered_range(&self) -> &RangeSet { &self.buffered }

    #[inline]
    pub fn flushed_range(&self) -> &RangeSet { &self.flushed }

    #[inline]
    pub fn into_flushed_range(mut self) -> RangeSet { mem::take(&mut self.flushed) }

    pub async fn flush(&mut self) -> io::Result<()> {
        let Self { file, vbuf, .. } = self;
        vbuf.flush_to(file).await?;
        self.flushed = self.buffered.clone();
        Ok(())
    }

    pub async fn shutdown(&mut self) -> io::Result<()> { self.flush().await }

    async fn flush_if_needed(&mut self) -> io::Result<()> {
        // 有任何完成的缓冲区都要进行回收，防止 range 计算错误
        self.vbuf.release_done();
        // 缓冲区空的，没有东西可以 flush
        if self.vbuf.is_empty() {
            return Ok(());
        }
        // 尽量将每个 buf 回收到基础大小
        if self.vbuf.totoal_remaining_len() > BASE_BUF_SIZE {
            self.flush().await?;
            self.vbuf.release_done();
            self.vbuf.compact_all();
            self.vbuf.shrink_all_to(BASE_BUF_SIZE);
        }
        Ok(())
    }
}

impl AsyncWriteAt for RandBufFile {
    async fn write_at<B: IoBuf>(&mut self, mut buf: B, pos: u64) -> BufResult<usize, B> {
        (_, buf) = buf_try!(self.flush_if_needed().await, buf);
        let pos = pos as usize;
        let written = self.vbuf.read_from_at(buf.as_slice(), pos);
        self.buffered.insert_n_at(written, pos);
        (_, buf) = buf_try!(self.flush_if_needed().await, buf);
        (Ok(written), buf).into()
    }

    async fn write_vectored_at<T: IoVectoredBuf>(&mut self, mut buf: T, pos: u64) -> BufResult<usize, T> {
        let pos = pos as usize;
        let mut cur = pos;
        (_, buf) = buf_try!(self.flush_if_needed().await, buf);
        let mut total_written = 0;
        for buf in buf.iter_slice() {
            if buf.is_empty() {
                continue;
            }
            let written = self.vbuf.read_from_at(buf, cur);
            if written == 0 {
                break;
            }
            total_written += written;
            cur += written; // 更新下一个切片的写入位置
        }
        self.buffered.insert_n_at(total_written, pos);
        (_, buf) = buf_try!(self.flush_if_needed().await, buf);
        (Ok(total_written), buf).into()
    }
}

impl Drop for RandBufFile {
    fn drop(&mut self) { debug_assert!(self.vbuf.all_done(), "Buffer not all done") }
}

impl From<File> for RandBufFile {
    #[inline]
    fn from(file: File) -> Self { Self::new(file) }
}
