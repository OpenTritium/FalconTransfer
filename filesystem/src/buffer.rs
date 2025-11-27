/// 来自于 Compio
/// 它并不负责限制缓冲区上限，需要调用者自己确定什么时候 flush，默认情况是缓冲区会有一个上界然后回收至默认大小
use compio::{
    BufResult,
    buf::{IntoInner, IoBuf, IoBufMut, SetBufInit, Slice},
    io::{AsyncWrite, AsyncWriteAt},
};
use sparse_ranges::Range;
use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    io,
    ops::{Deref, DerefMut},
};

pub const DEFAULT_BUF_SIZE: usize = 0x4000;
pub const MAX_BUF_SIZE: usize = 0x40 * 0x400 * 0x400;
pub const MISSING_BUF_MSG: &str = "The buffer was submitted for io and never returned";

pub struct Inner {
    buf: Vec<u8>,
    pos: usize,
}

impl Inner {
    /// Returns true if the buffer is empty.
    #[inline]
    fn all_done(&self) -> bool { self.buf.len() == self.pos }

    /// 缓冲区是否有数据（不论是否被处理）
    #[inline]
    fn is_empty(&self) -> bool { self.buf.is_empty() }

    /// Move pos & init needle to 0
    #[inline]
    fn reset(&mut self) {
        self.pos = 0;
        unsafe { self.buf.set_len(0) };
    }

    /// Returns a slice that to be processed of the buffer.
    #[inline]
    fn remaining(&self) -> &[u8] { &self.buf[self.pos..] }

    /// Returns a owned slice that to be processed of the buffer.
    #[inline]
    pub fn into_owned_remaining(self) -> Slice<Self> {
        let pos = self.pos;
        IoBuf::slice(self, pos..)
    }
}

unsafe impl IoBuf for Inner {
    #[inline]
    fn as_buf_ptr(&self) -> *const u8 { self.buf.as_ptr() }

    #[inline]
    fn buf_len(&self) -> usize { self.buf.len() }

    #[inline]
    fn buf_capacity(&self) -> usize { self.buf.capacity() }
}

impl SetBufInit for Inner {
    #[inline]
    unsafe fn set_buf_init(&mut self, len: usize) {
        unsafe {
            self.buf.set_buf_init(len);
        }
    }
}

unsafe impl IoBufMut for Inner {
    #[inline]
    fn as_buf_mut_ptr(&mut self) -> *mut u8 { self.buf.as_mut_ptr() }
}

/// A buffer with an internal progress tracker
///
/// ```plain
/// +------------------------------------------------+
/// |               Buf: Vec<u8> cap                 |
/// +--------------------+-------------------+-------+
/// +-- Progress (pos) --^                   |
/// +-------- Initialized (vec len) ---------^
///                      +------ remaining ------^
/// ```
pub struct Buffer(Option<Inner>);

impl Buffer {
    /// Create a buffer with capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self { Self(Inner { buf: Vec::with_capacity(cap), pos: 0 }.into()) }

    /// Get the initialized but not consumed part of the buffer.
    #[inline]
    pub fn remaining(&self) -> &[u8] { self.inner().remaining() }

    /// Return the byte count that has not been processed
    #[inline]
    pub fn remaining_len(&self) -> usize { self.remaining().len() }

    /// If the inner buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool { self.inner().is_empty() }

    /// All bytes in the buffer have been read
    #[inline]
    pub fn all_done(&self) -> bool { self.inner().all_done() }

    /// Returns the capacity of the uninitialized part of the buffer (`capacity() - len()`).
    #[inline]
    pub fn unintialized_capacity(&self) -> usize { self.buf().capacity() - self.buf().len() }

    /// Clear the inner buffer and reset the position to the start.
    #[inline]
    pub fn reset(&mut self) { self.inner_mut().reset(); }

    /// 如果你想回收已经写入的空间，就使用此函数，如果你想更彻底一点回收未使用的内存，继续调用 shrink 即可
    /// compact 的语义是不改变缓冲区容量的情况下回收已经写入的字节，而 shrink 的语义是改变缓冲区容量
    /// 如果为缓冲区空就直接返回
    #[inline]
    pub fn compact(&mut self) {
        let inner = self.inner_mut();
        // If the buffer is empty (len is 0), there is nothing to compact
        if inner.buf.is_empty() {
            return;
        }
        // If pos is 0, there is nothing to compact.
        if inner.pos > 0 {
            let filled_len = inner.buf.len() - inner.pos;
            inner.buf.copy_within(inner.pos.., 0);
            inner.pos = 0;
            unsafe { inner.buf.set_len(filled_len) };
        }
    }

    /// Shrinks the capacity of the underlying buffer as much as possible.
    ///
    /// This directly calls [`Vec::shrink_to_fit()`] on the internal buffer.
    ///
    /// # Warning
    ///
    /// This method does **not** compact the buffer first. If there is consumed
    /// space at the beginning of the buffer (i.e., `pos > 0`), that space will
    /// **not** be freed.
    ///
    /// To shrink the capacity based on the amount of *unprocessed* data,
    /// you should call [`compact()`] **before** calling this method.
    ///
    /// [`compact()`]: Self::compact
    #[inline]
    pub fn shrink_to_fit(&mut self) { self.inner_mut().buf.shrink_to_fit(); }

    /// Shrinks the capacity of the underlying buffer with a lower bound.
    ///
    /// This directly calls [`Vec::shrink_to()`] on the internal buffer. The
    /// capacity will remain at least as large as `min_cap`.
    ///
    /// # Warning
    ///
    /// This method does **not** compact the buffer first. The capacity will be
    /// shrunk relative to the current `len()` of the buffer, not the length of
    /// the *unprocessed* data.
    ///
    /// To ensure the capacity is at least `min_cap` and also fits all
    /// unprocessed data, you should call [`compact()`] **before** calling this
    /// method.
    #[inline]
    pub fn shrink_to(&mut self, min_cap: usize) { self.inner_mut().buf.shrink_to(min_cap); }

    /// Reserves capacity for at least `additional` more bytes to be inserted into the buffer's underlying `Vec<u8>`.
    ///
    /// Unlike the `reserve` method on `Vec`, this does **not** take into account any space that could be freed by
    /// `compact()`. Call `compact()` first if you want to make use of that space.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity overflows `isize::MAX`.
    #[inline]
    pub fn reserve(&mut self, additional: usize) { self.inner_mut().buf.reserve(additional); }

    #[must_use]
    pub fn read_from(&mut self, src: &[u8]) -> usize {
        /// 尝试在空余缓冲区容量足够时全部写入
        /// 如果当前容量足够，则返回 true
        /// 如果当前容量不足，则返回 false
        fn try_write_all(dst: &mut Buffer, src: &[u8]) -> bool {
            if dst.unintialized_capacity() >= src.len() {
                dst.buf_mut().extend_from_slice(src);
                true
            } else {
                false
            }
        }
        // Check if there's enough space. If so, copy and return.
        if try_write_all(self, src) {
            return src.len();
        }
        // Not enough space, try compacting.
        self.compact();
        // Check again. If it's enough now, copy and return.
        if try_write_all(self, src) {
            return src.len();
        }
        // 在经过整理后缓冲区仍然不够写
        // 如果写入的切片大小 + 缓冲区待写入数据的长度小于最大缓冲区大小，则直接写入（自动按需扩容）
        if src.len() + self.remaining_len() <= MAX_BUF_SIZE {
            self.buf_mut().extend_from_slice(src);
            return src.len();
        }
        // 写入的切片太大（超过上限了），一次性写不完，这次只写一点，确保不超过上限就行
        let tobe_written = MAX_BUF_SIZE - self.remaining_len();
        self.buf_mut().extend_from_slice(&src[..tobe_written]);
        tobe_written
    }

    /// Execute a funcition with ownership of the buffer, and restore the buffer afterwards
    pub async fn with_inner<R>(&mut self, mut func: impl AsyncFnMut(Inner) -> BufResult<R, Inner>) -> io::Result<R> {
        let BufResult(res, buf) = func(self.take_inner()).await;
        self.restore_inner(buf);
        res
    }

    /// 如果写入量为0会抛出异常
    pub async fn flush_with(
        &mut self, mut func: impl AsyncFnMut(Inner) -> BufResult<usize, Inner>,
    ) -> io::Result<usize> {
        if self.all_done() {
            return Ok(0);
        }
        let mut total_written = 0;
        loop {
            let written = self.with_inner(&mut func).await?;
            if written == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Cannot flush all buffer data"));
            }
            total_written += written;
            self.advance(written);
            if self.all_done() {
                break;
            }
        }
        self.reset();
        Ok(total_written)
    }

    /// Wrapper to flush the buffer to a writer with error safety.
    pub async fn flush_to(&mut self, dst: &mut impl AsyncWrite) -> io::Result<usize> {
        self.flush_with(async |inner| dst.write(inner.into_owned_remaining()).await.into_inner()).await
    }

    pub async fn flush_at_to(&mut self, pos: u64, dst: &mut impl AsyncWriteAt) -> io::Result<usize> {
        self.flush_with(async |inner| dst.write_at(inner.into_owned_remaining(), pos).await.into_inner()).await
    }

    #[inline]
    pub fn advance(&mut self, amount: usize) {
        debug_assert!(self.inner().pos.checked_add(amount) <= Some(self.inner().buf_capacity()));
        let inner = self.inner_mut();
        inner.pos += amount;
    }

    #[inline]
    #[must_use]
    fn from_inner(inner: Inner) -> Self { Self(Some(inner)) }

    #[inline]
    #[must_use]
    fn take_inner(&mut self) -> Inner { self.0.take().expect(MISSING_BUF_MSG) }

    #[inline]
    fn restore_inner(&mut self, buf: Inner) {
        debug_assert!(self.0.is_none());
        self.0 = Some(buf);
    }

    #[inline]
    #[must_use]
    fn inner(&self) -> &Inner { self.0.as_ref().expect(MISSING_BUF_MSG) }

    #[inline]
    #[must_use]
    fn inner_mut(&mut self) -> &mut Inner { self.0.as_mut().expect(MISSING_BUF_MSG) }

    #[inline]
    #[must_use]
    fn buf(&self) -> &Vec<u8> { &self.inner().buf }

    #[inline]
    #[must_use]
    fn buf_mut(&mut self) -> &mut Vec<u8> { &mut self.inner_mut().buf }
}

impl From<&[u8]> for Buffer {
    #[inline]
    fn from(slice: &[u8]) -> Self { Self::from_inner(Inner { buf: slice.to_vec(), pos: 0 }) }
}

impl Debug for Buffer {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner();
        fmt.debug_struct("Buffer")
            .field("capacity", &inner.buf_capacity())
            .field("init", &inner.buf_len())
            .field("progress", &inner.pos)
            .finish()
    }
}

pub struct VectoredBuffer(BTreeMap<usize, Buffer>); // start ,buf

pub struct BufferAt<'a> {
    offset: usize,
    buf: &'a mut Buffer,
}

impl Deref for BufferAt<'_> {
    type Target = Buffer;

    #[inline]
    fn deref(&self) -> &Self::Target { self.buf }
}

impl DerefMut for BufferAt<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target { self.buf }
}

impl Default for VectoredBuffer {
    #[inline]
    fn default() -> Self { Self::new() }
}

impl VectoredBuffer {
    #[inline]
    pub fn new() -> Self { Self(BTreeMap::new()) }

    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = BufferAt<'_>> {
        self.0.iter_mut().map(|(&offset, buf)| BufferAt { offset, buf })
    }

    /// 调用此函数前请先调用 release_done
    #[inline]
    pub fn compact_all(&mut self) {
        for buf in self.0.values_mut() {
            buf.compact();
        }
    }

    // 如果你想将已经处理的也给shrink 掉，请调用此函数之前调用 compact_all
    #[inline]
    pub fn shrink_all_to_fit(&mut self) {
        for buf in self.0.values_mut() {
            buf.shrink_to_fit();
        }
    }

    /// 有效的回收内存
    #[inline]
    pub fn shrink_all_to(&mut self, min_cap: usize) {
        for buf in self.0.values_mut() {
            buf.shrink_to(min_cap);
        }
    }

    #[inline]
    pub fn release_done(&mut self) { self.0.retain(|_, buf| !buf.all_done()); }

    #[inline]
    pub fn all_done(&self) -> bool { self.0.values().all(|buf| buf.all_done()) }

    #[inline]
    pub fn totoal_remaining_len(&self) -> usize { self.0.values().map(|buf| buf.remaining_len()).sum() }

    #[inline]
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    /// 理想状态是这个api总是会合并相邻的缓冲区？
    /// 需要注意的是，记得回收已经写完的缓冲区防止 range 错误，当然我相信 flush_to 已经做好了
    pub fn read_from_at(&mut self, src: &[u8], pos: usize) -> usize {
        if src.is_empty() {
            return 0;
        }
        let src_rng: Range = (&(pos..pos + src.len())).try_into().unwrap();
        let rhs = self.0.iter_mut().find_map(|(&start, buf)| {
            if buf.is_empty() {
                return None;
            }
            let tobe_merged_rng: Range = (&(start..start + buf.remaining_len())).try_into().unwrap();
            if !tobe_merged_rng.is_adjacent(&src_rng) {
                return None;
            }
            debug_assert!(src_rng != tobe_merged_rng);
            // 确保新增的数据在某个缓冲右边
            if src_rng <= tobe_merged_rng {
                return None;
            }
            Some(buf)
        });
        if let Some(rhs) = rhs {
            return rhs.read_from(src);
        }
        let written = if src.len() <= MAX_BUF_SIZE {
            // 如果缓冲比较小，就直接塞到 map
            self.0.insert(pos, src.into());
            src.len()
        } else {
            // 如果比较大先塞大部分
            self.0.insert(pos, (&src[..MAX_BUF_SIZE]).into());
            MAX_BUF_SIZE
        };
        debug_assert!(written <= MAX_BUF_SIZE);
        debug_assert!(self.0.values().all(|buf| buf.remaining_len() <= MAX_BUF_SIZE));
        written
    }

    pub async fn flush_to(&mut self, dst: &mut impl AsyncWriteAt) -> io::Result<usize> {
        if self.all_done() {
            return Ok(0);
        }
        let mut total_written = 0;
        for mut buf_at in self.iter_mut() {
            let offset = buf_at.offset as u64;
            total_written += buf_at.flush_at_to(offset, dst).await?;
        }
        self.release_done(); // 释放掉空buf,防止range 计算出错
        Ok(total_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::BufResult;
    use std::sync::{Arc, Mutex};

    // 模拟一个既支持顺序写也支持随机写的设备（类似文件）
    #[derive(Clone, Default)]
    struct MockFile {
        // 使用 BTreeMap 模拟稀疏文件存储，key 是 offset
        data: Arc<Mutex<BTreeMap<u64, u8>>>,
    }

    impl MockFile {
        fn get_content_at(&self, start: u64, len: usize) -> Vec<u8> {
            let map = self.data.lock().unwrap();
            (start..start + len as u64).map(|i| *map.get(&i).unwrap_or(&0)).collect()
        }

        fn len(&self) -> u64 {
            if let Some((&k, _)) = self.data.lock().unwrap().last_key_value() {
                k + 1
            } else {
                0
            }
        }
    }

    impl AsyncWrite for MockFile {
        async fn write<T: IoBuf>(&mut self, buf: T) -> BufResult<usize, T> {
            // 这里简单模拟 append，假设当前 pos 是 map 的最大值
            let current_len = self.len();
            let slice = unsafe { std::slice::from_raw_parts(buf.as_buf_ptr(), buf.buf_len()) };
            let mut map = self.data.lock().unwrap();
            for (i, &b) in slice.iter().enumerate() {
                map.insert(current_len + i as u64, b);
            }
            BufResult(Ok(slice.len()), buf)
        }

        async fn flush(&mut self) -> io::Result<()> { Ok(()) }

        async fn shutdown(&mut self) -> io::Result<()> { Ok(()) }
    }

    impl AsyncWriteAt for MockFile {
        async fn write_at<T: IoBuf>(&mut self, buf: T, pos: u64) -> BufResult<usize, T> {
            let slice = unsafe { std::slice::from_raw_parts(buf.as_buf_ptr(), buf.buf_len()) };
            let mut map = self.data.lock().unwrap();
            for (i, &b) in slice.iter().enumerate() {
                map.insert(pos + i as u64, b);
            }
            BufResult(Ok(slice.len()), buf)
        }
    }

    // --- Buffer Tests ---

    #[compio::test]
    async fn test_buffer_basic_io() {
        let mut buf = Buffer::with_capacity(128);
        assert!(buf.is_empty());

        // 写入数据
        let data = b"Hello World";
        let n = buf.read_from(data);
        assert_eq!(n, data.len());
        assert_eq!(buf.remaining(), data);

        // 消费部分数据
        buf.advance(6); // "Hello " consumed
        assert_eq!(buf.remaining(), b"World");
        assert_eq!(buf.remaining_len(), 5);

        // Compact
        // 此时内部: [H,e,l,l,o, ,W,o,r,l,d], pos=6
        // Compact后: [W,o,r,l,d, ...], pos=0
        buf.compact();
        assert_eq!(buf.remaining(), b"World");

        // 验证内部 pos 是否归零 (通过再次写入验证)
        let _ = buf.read_from(b"!");
        assert_eq!(buf.remaining(), b"World!");
    }

    #[compio::test]
    async fn test_buffer_auto_expand() {
        let mut buf = Buffer::with_capacity(4); // 初始容量 4
        let data = b"12345678"; // 长度 8，超过容量

        // read_from 内部应该会自动扩容
        let n = buf.read_from(data);

        assert_eq!(n, data.len());
        assert_eq!(buf.remaining(), data);

        // 我们改为检查“现在的总容量”是否确实大于等于数据长度，证明扩容成功。
        assert!(buf.inner().buf_capacity() >= data.len());
    }

    #[compio::test]
    async fn test_buffer_flush_to() {
        let mut buf = Buffer::from(&b"test_data"[..]);
        let mut mock_file = MockFile::default();

        // Flush 全部数据
        let n = buf.flush_to(&mut mock_file).await.unwrap();
        assert_eq!(n, 9);

        // 验证 buffer 被重置
        assert!(buf.is_empty());
        assert!(buf.all_done());

        // 验证 mock file 收到数据
        assert_eq!(mock_file.get_content_at(0, 9), b"test_data");
    }

    #[compio::test]
    async fn test_buffer_partial_flush_logic() {
        // 这个测试验证 compact 后的写入行为
        let mut buf = Buffer::with_capacity(10);
        let _ = buf.read_from(b"12345");
        buf.advance(2); // 剩下 "345"
        buf.compact(); // 此时 buf len=3, cap=10

        let _ = buf.read_from(b"678");
        assert_eq!(buf.remaining(), b"345678");
    }

    // --- VectoredBuffer Tests ---

    #[compio::test]
    async fn test_vectored_insert_basic() {
        let mut vbuf = VectoredBuffer::new();

        // 插入两段不连续的数据
        vbuf.read_from_at(b"Hello", 0);
        vbuf.read_from_at(b"World", 100);

        assert_eq!(vbuf.totoal_remaining_len(), 10);

        // 内部结构校验：应该有两个 Buffer
        let mut iter = vbuf.iter_mut();
        let buf1 = iter.next().unwrap();
        assert_eq!(buf1.offset, 0);
        assert_eq!(buf1.remaining(), b"Hello");

        let buf2 = iter.next().unwrap();
        assert_eq!(buf2.offset, 100);
        assert_eq!(buf2.remaining(), b"World");
    }

    #[compio::test]
    async fn test_vectored_merge_logic() {
        let mut vbuf = VectoredBuffer::new();

        // 插入第一段: [10..15) "Hello"
        vbuf.read_from_at(b"Hello", 10);

        // 插入紧邻的第二段: [15..20) "World"
        // 根据代码逻辑：Range(10..15) is adjacent to Range(15..20) 且 src > existing
        // 应该触发合并
        vbuf.read_from_at(b"World", 15);

        // 验证：应该只有一个 Buffer，包含 "HelloWorld"
        let count = vbuf.iter_mut().count();
        assert_eq!(count, 1, "Should merge adjacent buffers");

        let mut iter = vbuf.iter_mut();
        let buf = iter.next().unwrap();
        assert_eq!(buf.offset, 10);
        assert_eq!(buf.remaining(), b"HelloWorld");
    }

    #[compio::test]
    async fn test_vectored_no_merge_gaps() {
        let mut vbuf = VectoredBuffer::new();

        vbuf.read_from_at(b"A", 10);
        // 插入到 12，中间隔了一个 11，不应该合并
        vbuf.read_from_at(b"B", 12);

        assert_eq!(vbuf.iter_mut().count(), 2);
    }

    #[compio::test]
    async fn test_vectored_no_merge_order() {
        let mut vbuf = VectoredBuffer::new();

        // 先插入后面的: [20..25)
        vbuf.read_from_at(b"Later", 20);

        // 再插入前面的: [15..20)
        // 你的逻辑中：
        // src_rng (15..20) <= tobe_merged_rng (20..25) 是 True (Range 比较通常比较 start)
        // 代码里: if src_rng <= tobe_merged_rng { return None; }
        // 所以这应该**不会**触发合并，而是插入新的 entry
        vbuf.read_from_at(b"Prior", 15);

        assert_eq!(vbuf.iter_mut().count(), 2, "Current logic does not support prepend-merge");
    }

    #[compio::test]
    async fn test_vectored_flush_and_cleanup() {
        let mut vbuf = VectoredBuffer::new();
        let mut mock_file = MockFile::default();

        vbuf.read_from_at(b"Part1", 10);
        vbuf.read_from_at(b"Part2", 50);

        // 执行 Flush
        let written = vbuf.flush_to(&mut mock_file).await.unwrap();
        assert_eq!(written, 10);

        // 验证文件内容
        assert_eq!(mock_file.get_content_at(10, 5), b"Part1");
        assert_eq!(mock_file.get_content_at(50, 5), b"Part2");
        assert_eq!(mock_file.get_content_at(20, 1), b"\0"); // 中间应该是空的

        // 验证 VectoredBuffer 清理逻辑 (release_done)
        assert!(vbuf.is_empty(), "Buffer should be empty after flush");
    }

    #[compio::test]
    async fn test_vectored_partial_flush() {
        // 模拟一个写入会失败或只写入一半的情况比较麻烦，
        // 这里主要测试 flush_to 调用 release_done 的行为
        // 我们手动构造一个未完成的 buffer

        let mut vbuf = VectoredBuffer::new();
        vbuf.read_from_at(b"keep_me", 100);

        // 手动干预：假设我们只 flush 了其中一部分
        // 由于 flush_to 内部是 await 循环，我们很难在单元测试里精准控制中间状态
        // 只能测试全量 flush 后是否 clean。

        // 但我们可以测试 release_done 逻辑
        let mut iter = vbuf.iter_mut();
        let mut buf = iter.next().unwrap();
        buf.advance(4); // "keep" consumed, "_me" remains
        drop(iter);

        // 此时 buffer 还没 all_done
        vbuf.release_done();
        assert!(!vbuf.is_empty());

        // 消费剩余部分
        let mut iter = vbuf.iter_mut();
        let mut buf = iter.next().unwrap();
        buf.advance(3);
        drop(iter);

        // 此时 all_done 为 true
        vbuf.release_done();
        assert!(vbuf.is_empty());
    }
}
