use crate::buffer::{Buffer, VectoredBuffer};
use compio::{
    BufResult,
    buf::{IoBuf, IoVectoredBuf, buf_try},
    fs::File,
    io::{AsyncWrite, AsyncWriteAt},
};
use falcon_config::config;
use sparse_ranges::RangeSet;
use std::{
    io::{self, Cursor},
    mem,
};
use tracing::{debug, trace};

/// Sequential buffered file with automatic memory management.
pub struct SeqBufFile {
    file: Cursor<File>,
    buffered: RangeSet,
    flushed: RangeSet,
    buf: Buffer,
}

impl SeqBufFile {
    /// Creates a new sequential buffered file starting at the specified position.
    #[inline]
    pub fn with_position(file: File, pos: u64) -> Self {
        let mut cursor = Cursor::new(file);
        cursor.set_position(pos);
        Self {
            file: cursor,
            buffered: RangeSet::new(),
            flushed: RangeSet::new(),
            buf: Buffer::with_capacity(config!(file_buffer_base)),
        }
    }

    /// Gets the range of buffered data.
    #[inline]
    pub const fn buffered_range(&self) -> &RangeSet { &self.buffered }

    /// Gets the range of flushed data.
    #[inline]
    pub const fn flushed_range(&self) -> &RangeSet { &self.flushed }

    /// Consumes the file and returns the flushed range.
    #[inline]
    pub fn into_flushed_range(mut self) -> RangeSet { mem::take(&mut self.flushed) }

    async fn flush_if_needed(&mut self, next_chunk: Option<usize>) -> io::Result<()> {
        // 卫语句：缓冲区为空
        if self.buf.is_empty() {
            return Ok(());
        }
        // 卫语句：缓冲区已全部写入，直接重置
        if self.buf.all_done() {
            self.buf.reset();
            return Ok(());
        }
        self.buf.compact();
        // 前瞻性 flush：如果写入下个块会超过上限，先 flush 当前数据
        let should_proactive_flush = || {
            let file_buffer_max = config!(file_buffer_max);
            let new_remaining = self.buf.remaining_len() + next_chunk.unwrap_or(0);
            new_remaining > file_buffer_max
        };
        if should_proactive_flush() {
            self.flush().await?;
            return Ok(());
        }
        // 托底的回收方法：超过基础大小时 flush 并回收
        let file_buffer_base = config!(file_buffer_base);
        // 托底的回收方法
        if self.buf.remaining_len() > file_buffer_base {
            self.flush().await?;
            self.buf.shrink_to_limit(file_buffer_base);
        }
        Ok(())
    }
}

impl AsyncWrite for SeqBufFile {
    async fn write<B: IoBuf>(&mut self, mut buf: B) -> BufResult<usize, B> {
        // The previous flush may error because disk full. We need to make the buffer all-done before writing new data
        // to it.
        let pos = self.file.position() as usize + self.buf.remaining_len();
        debug!(
            cursor_pos = self.file.position() as usize,
            buf_remaining = self.buf.remaining_len(),
            calculated_pos = pos,
            flush_range = self.flushed.len(),
            "SeqBufFile: calculating write position"
        );
        let buf_len = buf.buf_len();
        (_, buf) = buf_try!(self.flush_if_needed(buf_len.into()).await, buf);
        let written = self.buf.read_from(buf.as_slice());
        self.buffered.insert_n_at(written, pos);
        trace!(
            written,
            insert_pos = pos,
            new_buffered = ?self.buffered,
            "SeqBufFile: data written to buffer"
        );
        (_, buf) = buf_try!(self.flush_if_needed(None).await, buf);
        (Ok(written), buf).into()
    }

    /// Flushes the buffer to the underlying file and updates ranges.
    ///
    /// # Side Effects
    ///
    /// - Advances the file cursor position
    /// - Updates `flushed` range to match `buffered` range
    async fn flush(&mut self) -> io::Result<()> {
        let Self { file, buf, .. } = self;
        trace!(
            before_buffered = ?self.buffered,
            before_flushed = ?self.flushed,
            buf_remaining_len = buf.remaining_len(),
            "SeqBufFile: before flush"
        );
        buf.flush_to(file).await?;
        debug!(
            after_flushed = ?self.buffered,
            "SeqBufFile: after flush"
        );
        self.flushed = self.buffered.clone();
        Ok(())
    }

    async fn write_vectored<B: IoVectoredBuf>(&mut self, mut buf: B) -> BufResult<usize, B> {
        let pos = self.file.position() as usize + self.buf.remaining_len();
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

/// Random-access buffered file with automatic memory management.
pub struct RandBufFile {
    file: File,
    vbuf: VectoredBuffer,
    buffered: RangeSet,
    flushed: RangeSet,
}

impl RandBufFile {
    /// Creates a new random-access buffered file.
    #[inline]
    pub fn new(file: File) -> Self {
        Self { file, buffered: RangeSet::new(), flushed: RangeSet::new(), vbuf: VectoredBuffer::new() }
    }

    /// Gets the range of buffered data.
    #[inline]
    pub const fn buffered_range(&self) -> &RangeSet { &self.buffered }

    /// Gets the range of flushed data.
    #[inline]
    pub const fn flushed_range(&self) -> &RangeSet { &self.flushed }

    /// Consumes the file and returns the flushed range.
    #[inline]
    pub fn into_flushed_range(mut self) -> RangeSet { mem::take(&mut self.flushed) }

    /// Flushes all buffers to the underlying file.
    ///
    /// # Side Effects
    ///
    /// - Calls `release_done()` to remove empty buffers
    /// - Updates `flushed` range to match `buffered` range
    pub async fn flush(&mut self) -> io::Result<()> {
        let Self { file, vbuf, .. } = self;
        trace!(
            before_buffered = ?self.buffered,
            before_flushed = ?self.flushed,
            vbuf_total_remaining = vbuf.total_remaining_len(),
            "RandBufFile: before flush"
        );
        vbuf.flush_to(file).await?;
        self.vbuf.release_done();
        debug!(
            after_flushed = ?self.buffered,
            "RandBufFile: after flush"
        );
        self.flushed = self.buffered.clone();
        Ok(())
    }

    pub async fn shutdown(&mut self) -> io::Result<()> { self.flush().await }

    async fn flush_if_needed(&mut self) -> io::Result<()> {
        self.vbuf.release_done();
        if self.vbuf.is_empty() {
            return Ok(());
        }
        let file_buffer_base = config!(file_buffer_base);
        if self.vbuf.total_remaining_len() > file_buffer_base {
            self.flush().await?;
            // Recycle memory after flush
            self.vbuf.compact_all();
            self.vbuf.shrink_all_to_limit(file_buffer_base);
        }
        Ok(())
    }
}

impl AsyncWriteAt for RandBufFile {
    async fn write_at<B: IoBuf>(&mut self, mut buf: B, pos: u64) -> BufResult<usize, B> {
        (_, buf) = buf_try!(self.flush_if_needed().await, buf);
        let pos = pos as usize;
        trace!(
            write_offset = pos,
            data_len = buf.buf_len(),
            current_buffered = ?self.buffered,
            "RandBufFile: write_at called"
        );
        let written = self.vbuf.read_from_at(buf.as_slice(), pos);
        self.buffered.insert_n_at(written, pos);
        debug!(
            written,
            insert_pos = pos,
            new_buffered = ?self.buffered,
            "RandBufFile: data written to buffer"
        );
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
            cur += written;
        }
        self.buffered.insert_n_at(total_written, pos);
        (_, buf) = buf_try!(self.flush_if_needed().await, buf);
        (Ok(total_written), buf).into()
    }
}

impl From<File> for RandBufFile {
    #[inline]
    fn from(file: File) -> Self { Self::new(file) }
}

#[cfg(test)]
mod file_tests {
    use super::*;
    use compio::{
        fs::OpenOptions,
        io::{AsyncReadAtExt, AsyncWrite, AsyncWriteAt},
    };
    use std::path::Path;
    use tempfile::TempDir;
    async fn read_all_at(path: &Path, pos: u64) -> (usize, Vec<u8>) {
        let another_file = OpenOptions::new().read(true).open(&path).await.unwrap();
        another_file.read_to_end_at(vec![], pos).await.unwrap()
    }

    #[compio::test]
    async fn test_seq_basic_write_flush() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_basic.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);
        let (written, _) = seq_file.write(b"Hello").await.unwrap();
        assert_eq!(written, 5);
        let (written, _) = seq_file.write(b" World").await.unwrap();
        assert_eq!(written, 6);
        assert!(seq_file.flushed_range().is_empty(), "Should not flush yet");
        seq_file.flush().await.unwrap();
        assert_eq!(seq_file.flushed_range().len(), 11);
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 11);
        assert_eq!(buf, b"Hello World");
    }

    #[compio::test]
    async fn test_seq_auto_flush_threshold() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_threshold.bin");
        let file = OpenOptions::new().create_new(true).write(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);
        let first_chunk = vec![1u8; 10_000]; // 第一块要小于 BASE_BUF_SIZE
        let second_chunk = vec![1u8; config!(file_buffer_max) - 10_000]; // 第二个块大于 BASE 但小于 MAX
        let third_chunk = vec![1u8; config!(file_buffer_max)]; // 巨无霸
        let (written, _) = seq_file.write(first_chunk).await.unwrap();
        assert_eq!(written, 10_000);
        assert!(seq_file.flushed_range().is_empty()); // 此时应该没有 flush
        // 第二次写入，此时会提前前瞻性 flush，然后写入最大量，然后继续 flush
        let (written, _) = seq_file.write(second_chunk).await.unwrap();
        assert_eq!(written, config!(file_buffer_max) - 10_000); // 第二个块被全部写入了
        assert_eq!(seq_file.flushed_range().len(), config!(file_buffer_max));
        seq_file.flush().await.unwrap();
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, config!(file_buffer_max));
        assert!(buf.iter().all(|&n| n == 1));
        let (written, _) = seq_file.write(third_chunk).await.unwrap(); // 只能写入最大值
        assert_eq!(written, config!(file_buffer_max));
        assert_eq!(seq_file.flushed_range().len(), config!(file_buffer_max) * 2);
        assert_eq!(seq_file.buffered_range().len(), config!(file_buffer_max) * 2);
    }

    #[compio::test]
    async fn test_rand_basic_write_flush() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_basic.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 写入 "World" at 10
        let (written, _) = rand_file.write_at(b"World", 10).await.unwrap();
        assert_eq!(written, 5);
        assert_eq!(rand_file.buffered_range().len(), 5); // [10, 15)
        assert!(rand_file.flushed_range().is_empty());

        // 写入 "Hello" at 0
        let (written, _) = rand_file.write_at(b"Hello", 0).await.unwrap();
        assert_eq!(written, 5);
        // buffered range 应该是 [0, 5) 和 [10, 15)，总长度 10
        assert_eq!(rand_file.buffered_range().len(), 10);

        rand_file.flush().await.unwrap();

        // 验证 flush 结果
        assert_eq!(rand_file.flushed_range().len(), 10);
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        // 文件实际大小为 15，因为中间有 5 个字节的空洞
        assert_eq!(bytes_read, 15);
        // 检查实际内容：Hello [5空字节] World
        assert_eq!(buf[0..5], *b"Hello");
        assert_eq!(buf[5..10].iter().filter(|&&b| b == 0).count(), 5); // 默认填充 0
        assert_eq!(buf[10..15], *b"World");
    }

    #[compio::test]
    async fn test_rand_overwrite_and_merge() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_overwrite.bin");
        let file = OpenOptions::new().create_new(true).write(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 写入 "ABCDEFG" at 0
        rand_file.write_at(b"ABCDEFG", 0).await.unwrap(); // [0, 7)
        assert_eq!(rand_file.buffered_range().len(), 7);

        // 覆盖写入 "XYZ" at 2 (覆盖 C, D, E; 保留 A, B)
        rand_file.write_at(b"XYZ", 2).await.unwrap(); // [2, 5)

        // buffered range 仍应是 [0, 7)
        assert_eq!(rand_file.buffered_range().len(), 7);

        rand_file.flush().await.unwrap();

        // 3. 验证结果 A B X Y Z F G
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 7);
        // 修改这里：预期结果应该是 ABXYZFG
        assert_eq!(buf, b"ABXYZFG");
    }

    #[compio::test]
    async fn test_rand_vectored_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_vec.bin");
        let file = OpenOptions::new().create_new(true).write(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        let parts: Vec<&[u8]> = vec![b"Data", b" ", b"Written", b" "];
        // expected_written = 4 + 1 + 7 + 1 = 13
        let expected_written = parts.iter().map(|s| s.len()).sum::<usize>();

        // 1. 矢量写入到 10
        let (written, _) = rand_file.write_vectored_at(parts, 10).await.unwrap();
        assert_eq!(written, expected_written);
        assert_eq!(rand_file.buffered_range().len(), expected_written); // [10, 23)

        rand_file.flush().await.unwrap();

        // 2. 验证结果
        let (bytes_read, buf) = read_all_at(&path, 0).await;

        // 修改这里：10 (空洞) + 13 (数据) = 23
        assert_eq!(bytes_read, 23);
        // 修改这里：切片范围是 10..23
        assert_eq!(buf[10..23], *b"Data Written ");
    }

    #[compio::test]
    async fn test_rand_auto_flush_recycle() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_recycle.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 构造足够多的数据来触发 flush_if_needed 的托底逻辑 (> BASE_BUF_SIZE)
        // 使用多个小块，确保 VectoredBuffer 内部创建多个 Buffer
        let chunk_size = config!(file_buffer_base) / 4;
        let num_chunks = 5; // 5 * (BASE_BUF_SIZE/4) > BASE_BUF_SIZE

        // 1. 连续写入多个不重叠的小块
        for i in 0..num_chunks {
            let data = vec![i as u8; chunk_size];
            rand_file.write_at(data, (i * chunk_size) as u64).await.unwrap();
        }

        let total_written = num_chunks * chunk_size;

        // 2. 检查是否自动 flush
        // 因为 total_remaining_len > BASE_BUF_SIZE，应该触发 flush
        assert_eq!(rand_file.flushed_range().len(), total_written);
        assert_eq!(rand_file.buffered_range().len(), total_written);

        // 3. 验证磁盘内容
        rand_file.shutdown().await.unwrap(); // 确保所有数据都落盘

        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, total_written);

        // 检查数据内容是否正确 (0, 1, 2, 3, 4)
        for i in 0..num_chunks {
            let start = i * chunk_size;
            let end = (i + 1) * chunk_size;
            assert!(buf[start..end].iter().all(|&n| n == i as u8));
        }
    }

    #[compio::test]
    async fn test_seq_vectored_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_vec.bin");
        let file = OpenOptions::new().create_new(true).write(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);

        let parts: Vec<&[u8]> = vec![b"Part1", b"", b"Part2", b"Part3"];
        // Part2 是空的，测试空切片跳过逻辑
        let (written, _) = seq_file.write_vectored(parts).await.unwrap();

        assert_eq!(written, 5 + 5 + 5);
        seq_file.flush().await.unwrap();

        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 15);
        assert_eq!(buf, b"Part1Part2Part3");
    }

    #[compio::test]
    async fn test_rand_gap_bridging() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_bridge.bin");
        let file = OpenOptions::new().create_new(true).write(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 1. 写入两头
        rand_file.write_at(b"AAAAA", 0).await.unwrap(); // [0, 5)
        rand_file.write_at(b"BBBBB", 10).await.unwrap(); // [10, 15)
        // 2. 写入中间，连接两头
        // 写入内容: XXXXXXXX (8 bytes) at pos 3
        // 这会覆盖 A 的后两个字节(3,4)，填补空洞(5..10)，并覆盖 B 的前一个字节(10)
        rand_file.write_at(b"XXXXXXXX", 3).await.unwrap();

        rand_file.flush().await.unwrap();

        let (bytes_read, buf) = read_all_at(&path, 0).await;

        // 修正期望：
        // 0..3: AAA (前3个A保留)
        // 3..11: XXXXXXXX (8字节新数据)
        // 11..15: BBBB (B的后4个字节保留，因为第一个B在位置10被覆盖了)
        assert_eq!(bytes_read, 15);
        assert_eq!(&buf[0..3], b"AAA");
        assert_eq!(&buf[3..11], b"XXXXXXXX");
        assert_eq!(&buf[11..15], b"BBBB");
    }

    #[compio::test]
    async fn test_zero_length_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("zero_len.bin");
        let file = OpenOptions::new().create_new(true).write(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        rand_file.write_at(b"Start", 0).await.unwrap();

        // 写入空数据
        let empty: &[u8] = &[];
        let (written, _) = rand_file.write_at(empty, 2).await.unwrap();
        assert_eq!(written, 0);

        // 写入空的 Vector
        let empty_vec: Vec<&[u8]> = vec![];
        let (written, _) = rand_file.write_vectored_at(empty_vec, 5).await.unwrap();
        assert_eq!(written, 0);

        rand_file.flush().await.unwrap();

        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 5);
        assert_eq!(buf, b"Start");
    }

    #[compio::test]
    async fn test_mixed_flush_cycles() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mixed_cycle.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);

        // 第一波
        seq_file.write(b"123").await.unwrap();
        seq_file.flush().await.unwrap();
        assert_eq!(seq_file.flushed_range().len(), 3);

        // 第二波
        seq_file.write(b"456").await.unwrap();
        // 注意：这里不手动 flush，直接由 Drop 时的 assert 检查（如果不掉用 shutdown，你的代码在 Drop 时会
        // panic，这里测试正常 shutdown 流程）
        seq_file.shutdown().await.unwrap();

        // 验证全量
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 6);
        assert_eq!(buf, b"123456");
    }

    #[compio::test]
    async fn test_seq_range_tracking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);

        // 初始状态：ranges 都为空
        assert_eq!(seq_file.buffered_range().len(), 0);
        assert_eq!(seq_file.flushed_range().len(), 0);

        // 写入第一块数据
        seq_file.write(b"AAAAA").await.unwrap(); // 5 bytes at pos 0
        assert_eq!(seq_file.buffered_range().len(), 5);
        assert_eq!(seq_file.flushed_range().len(), 0);

        // 写入第二块数据
        seq_file.write(b"BBBBB").await.unwrap(); // 5 bytes at pos 5
        assert_eq!(seq_file.buffered_range().len(), 10);
        assert_eq!(seq_file.flushed_range().len(), 0);

        // Flush 后，buffered 和 flushed 应该一致
        seq_file.flush().await.unwrap();
        assert_eq!(seq_file.buffered_range().len(), 10);
        assert_eq!(seq_file.flushed_range().len(), 10);
        assert_eq!(seq_file.buffered_range(), seq_file.flushed_range());

        // 再写入更多数据
        seq_file.write(b"CCCCC").await.unwrap(); // 5 bytes at pos 10
        assert_eq!(seq_file.buffered_range().len(), 15);
        assert_eq!(seq_file.flushed_range().len(), 10); // flushed 不变

        // 再次 flush
        seq_file.flush().await.unwrap();
        assert_eq!(seq_file.buffered_range().len(), 15);
        assert_eq!(seq_file.flushed_range().len(), 15);

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 15);
        assert_eq!(buf, b"AAAAABBBBBCCCCC");
    }

    #[compio::test]
    async fn test_rand_range_tracking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 初始状态
        assert_eq!(rand_file.buffered_range().len(), 0);
        assert_eq!(rand_file.flushed_range().len(), 0);

        // 写入第一块：位置 10
        rand_file.write_at(b"AAAAA", 10).await.unwrap();
        assert_eq!(rand_file.buffered_range().len(), 5);
        assert_eq!(rand_file.flushed_range().len(), 0);

        // 写入第二块：位置 20（不连续）
        rand_file.write_at(b"BBBBB", 20).await.unwrap();
        assert_eq!(rand_file.buffered_range().len(), 10); // 两段：[10,15) 和 [20,25)
        assert_eq!(rand_file.flushed_range().len(), 0);

        // 写入第三块：位置 0（在前面）
        rand_file.write_at(b"CCCCC", 0).await.unwrap();
        assert_eq!(rand_file.buffered_range().len(), 15); // 三段：[0,5), [10,15), [20,25)
        assert_eq!(rand_file.flushed_range().len(), 0);

        // Flush 后检查
        rand_file.flush().await.unwrap();
        assert_eq!(rand_file.buffered_range().len(), 15);
        assert_eq!(rand_file.flushed_range().len(), 15);
        assert_eq!(rand_file.buffered_range(), rand_file.flushed_range());

        // 写入连接两个段的数据：位置 5，长度 5，连接 [0,5) 和 [10,15)
        rand_file.write_at(b"DDDDD", 5).await.unwrap();
        assert_eq!(rand_file.buffered_range().len(), 20); // 两段：[0,15) 和 [20,25)
        assert_eq!(rand_file.flushed_range().len(), 15); // flushed 不变

        // 再次 flush
        rand_file.flush().await.unwrap();
        assert_eq!(rand_file.buffered_range().len(), 20);
        assert_eq!(rand_file.flushed_range().len(), 20);

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 25);
        assert_eq!(&buf[0..5], b"CCCCC");
        assert_eq!(&buf[5..10], b"DDDDD");
        assert_eq!(&buf[10..15], b"AAAAA");
        assert_eq!(&buf[15..20], &[0u8; 5]); // 空洞
        assert_eq!(&buf[20..25], b"BBBBB");
    }

    #[compio::test]
    async fn test_rand_range_merge_on_write() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_merge.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 写入三个分离的块
        rand_file.write_at(b"AAA", 0).await.unwrap(); // [0, 3)
        rand_file.write_at(b"BBB", 6).await.unwrap(); // [6, 9)
        rand_file.write_at(b"CCC", 12).await.unwrap(); // [12, 15)

        // 三个不连续的段
        assert_eq!(rand_file.buffered_range().len(), 9);

        // 写入连接前两个段的数据
        rand_file.write_at(b"XXX", 3).await.unwrap(); // [3, 6) 连接 [0,3) 和 [6,9)

        // 现在应该是两段：[0, 9) 和 [12, 15)
        assert_eq!(rand_file.buffered_range().len(), 12);

        // 写入连接所有段的数据
        rand_file.write_at(b"YYY", 9).await.unwrap(); // [9, 12) 连接 [0,9) 和 [12,15)

        // 现在应该是一段：[0, 15)
        assert_eq!(rand_file.buffered_range().len(), 15);

        rand_file.flush().await.unwrap();

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 15);
        assert_eq!(&buf[0..3], b"AAA");
        assert_eq!(&buf[3..6], b"XXX");
        assert_eq!(&buf[6..9], b"BBB");
        assert_eq!(&buf[9..12], b"YYY");
        assert_eq!(&buf[12..15], b"CCC");
    }

    #[compio::test]
    async fn test_rand_range_overwrite() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_overwrite_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 写入一个大块
        rand_file.write_at(b"AAAAAAAAAA", 0).await.unwrap(); // [0, 10)
        assert_eq!(rand_file.buffered_range().len(), 10);

        // 覆盖中间部分
        rand_file.write_at(b"XXXX", 3).await.unwrap(); // [3, 7) 覆盖

        // range 应该还是 [0, 10)，长度不变
        assert_eq!(rand_file.buffered_range().len(), 10);

        // 覆盖开头部分
        rand_file.write_at(b"YY", 0).await.unwrap(); // [0, 2) 覆盖
        assert_eq!(rand_file.buffered_range().len(), 10);

        // 覆盖结尾部分
        rand_file.write_at(b"ZZ", 8).await.unwrap(); // [8, 10) 覆盖
        assert_eq!(rand_file.buffered_range().len(), 10);

        rand_file.flush().await.unwrap();

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 10);
        assert_eq!(&buf[0..2], b"YY");
        assert_eq!(&buf[2..3], b"A");
        assert_eq!(&buf[3..7], b"XXXX");
        assert_eq!(&buf[7..8], b"A");
        assert_eq!(&buf[8..10], b"ZZ");
    }

    #[compio::test]
    async fn test_seq_range_auto_flush() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_auto_flush_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);

        // 写入小块数据，不应触发 auto flush
        let small_chunk = vec![1u8; 1000];
        seq_file.write(small_chunk).await.unwrap();
        assert_eq!(seq_file.buffered_range().len(), 1000);
        assert_eq!(seq_file.flushed_range().len(), 0);

        // 写入大块数据，应触发 auto flush
        let large_chunk = vec![2u8; config!(file_buffer_max)];
        seq_file.write(large_chunk).await.unwrap();

        // 因为前瞻性 flush，第一块和部分第二块应该被 flush
        assert_eq!(seq_file.buffered_range().len(), 1000 + config!(file_buffer_max));
        assert_eq!(seq_file.flushed_range().len(), 1000 + config!(file_buffer_max));

        seq_file.shutdown().await.unwrap();

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 1000 + config!(file_buffer_max));
        assert!(buf[0..1000].iter().all(|&b| b == 1));
        assert!(buf[1000..1000 + config!(file_buffer_max)].iter().all(|&b| b == 2));
    }

    #[compio::test]
    async fn test_rand_range_auto_flush() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_auto_flush_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 写入多个小块，累积超过 BASE_BUF_SIZE
        let chunk_size = config!(file_buffer_base) / 3;

        // 第一块
        let chunk1 = vec![1u8; chunk_size];
        rand_file.write_at(chunk1, 0).await.unwrap();
        assert_eq!(rand_file.flushed_range().len(), 0);

        // 第二块
        let chunk2 = vec![2u8; chunk_size];
        rand_file.write_at(chunk2, (chunk_size * 2) as u64).await.unwrap();
        assert_eq!(rand_file.flushed_range().len(), 0);

        // 第三块
        let chunk3 = vec![3u8; chunk_size];
        rand_file.write_at(chunk3, (chunk_size * 4) as u64).await.unwrap();
        assert_eq!(rand_file.flushed_range().len(), 0);

        // 第四块，这次应该触发 auto flush
        let chunk4 = vec![4u8; chunk_size];
        rand_file.write_at(chunk4, (chunk_size * 6) as u64).await.unwrap();

        // 应该已经 flush
        let total_data_len = chunk_size * 4;
        assert_eq!(rand_file.buffered_range().len(), total_data_len);
        assert_eq!(rand_file.flushed_range().len(), total_data_len);

        rand_file.shutdown().await.unwrap();

        // 验证文件内容（有空洞）
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, chunk_size * 7);

        // 检查各段数据
        assert!(buf[0..chunk_size].iter().all(|&b| b == 1));
        assert!(buf[chunk_size..chunk_size * 2].iter().all(|&b| b == 0)); // 空洞
        assert!(buf[chunk_size * 2..chunk_size * 3].iter().all(|&b| b == 2));
        assert!(buf[chunk_size * 3..chunk_size * 4].iter().all(|&b| b == 0)); // 空洞
        assert!(buf[chunk_size * 4..chunk_size * 5].iter().all(|&b| b == 3));
        assert!(buf[chunk_size * 5..chunk_size * 6].iter().all(|&b| b == 0)); // 空洞
        assert!(buf[chunk_size * 6..chunk_size * 7].iter().all(|&b| b == 4));
    }

    #[compio::test]
    async fn test_seq_range_consistency_after_multiple_flushes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_multi_flush.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);

        // 多次 write + flush 循环
        for i in 0..10 {
            let data = vec![i as u8; 100];
            seq_file.write(data).await.unwrap();

            let expected_len = (i + 1) * 100;
            assert_eq!(seq_file.buffered_range().len(), expected_len);

            seq_file.flush().await.unwrap();

            assert_eq!(seq_file.buffered_range().len(), expected_len);
            assert_eq!(seq_file.flushed_range().len(), expected_len);
            assert_eq!(seq_file.buffered_range(), seq_file.flushed_range());
        }

        seq_file.shutdown().await.unwrap();

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 1000);

        for i in 0..10 {
            let start = i * 100;
            let end = (i + 1) * 100;
            assert!(buf[start..end].iter().all(|&b| b == i as u8));
        }
    }

    #[compio::test]
    async fn test_rand_range_consistency_after_multiple_flushes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_multi_flush.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        let mut expected_len = 0;

        // 多次 write + flush 循环，随机位置
        for i in 0..10 {
            let data = vec![i as u8; 50];
            let pos = (i * 100) as u64; // 位置：0, 100, 200, 300, ...
            rand_file.write_at(data, pos).await.unwrap();

            expected_len += 50;
            assert_eq!(rand_file.buffered_range().len(), expected_len);

            rand_file.flush().await.unwrap();

            assert_eq!(rand_file.buffered_range().len(), expected_len);
            assert_eq!(rand_file.flushed_range().len(), expected_len);
            assert_eq!(rand_file.buffered_range(), rand_file.flushed_range());
        }

        rand_file.shutdown().await.unwrap();

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 950); // 最后一块在 900..950

        for i in 0..10 {
            let start = i * 100;
            let end = i * 100 + 50;
            assert!(buf[start..end].iter().all(|&b| b == i as u8));
        }
    }

    #[compio::test]
    async fn test_rand_range_with_large_gap() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_large_gap.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 写入距离很远的两个块
        rand_file.write_at(b"START", 0).await.unwrap();
        rand_file.write_at(b"END", 1_000_000).await.unwrap();

        assert_eq!(rand_file.buffered_range().len(), 8); // 5 + 3

        rand_file.flush().await.unwrap();
        assert_eq!(rand_file.flushed_range().len(), 8);

        // 验证文件内容
        let (_bytes_read_start, buf_start) = read_all_at(&path, 0).await;
        assert_eq!(&buf_start[0..5], b"START");

        let (_bytes_read_end, buf_end) = read_all_at(&path, 1_000_000).await;
        assert_eq!(&buf_end[0..3], b"END");
    }

    #[compio::test]
    async fn test_seq_vectored_range_tracking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seq_vec_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut seq_file = SeqBufFile::from(file);

        // 向量写入
        let parts: Vec<&[u8]> = vec![b"AAA", b"BBB", b"CCC"];
        seq_file.write_vectored(parts).await.unwrap();

        assert_eq!(seq_file.buffered_range().len(), 9);
        assert_eq!(seq_file.flushed_range().len(), 0);

        seq_file.flush().await.unwrap();

        assert_eq!(seq_file.buffered_range().len(), 9);
        assert_eq!(seq_file.flushed_range().len(), 9);

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 0).await;
        assert_eq!(bytes_read, 9);
        assert_eq!(buf, b"AAABBBCCC");
    }

    #[compio::test]
    async fn test_rand_vectored_range_tracking() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rand_vec_range.bin");
        let file = OpenOptions::new().create_new(true).write(true).read(true).open(&path).await.unwrap();
        let mut rand_file = RandBufFile::from(file);

        // 向量写入到指定位置
        let parts: Vec<&[u8]> = vec![b"AAA", b"BBB", b"CCC"];
        rand_file.write_vectored_at(parts, 100).await.unwrap();

        assert_eq!(rand_file.buffered_range().len(), 9);
        assert_eq!(rand_file.flushed_range().len(), 0);

        rand_file.flush().await.unwrap();

        assert_eq!(rand_file.buffered_range().len(), 9);
        assert_eq!(rand_file.flushed_range().len(), 9);

        // 验证文件内容
        let (bytes_read, buf) = read_all_at(&path, 100).await;
        assert_eq!(bytes_read, 9);
        assert_eq!(buf, b"AAABBBCCC");
    }
}
