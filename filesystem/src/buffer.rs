/// 来自于 Compio
/// 它并不负责限制缓冲区上限，需要调用者自己确定什么时候 flush，默认情况是缓冲区会有一个上界然后回收至默认大小
use compio::{
    BufResult,
    buf::{IntoInner, IoBuf, IoBufMut, SetBufInit, Slice},
    io::{AsyncWrite, AsyncWriteAt},
};
use std::{
    collections::BTreeMap,
    fmt::{self, Debug},
    io,
    ops::{self, Deref, DerefMut, RangeBounds},
    slice,
};

pub const DEFAULT_BUF_SIZE: usize = 0x4000;
pub const MAX_BUF_SIZE: usize = 0x40 * 0x400 * 0x400;
pub const MISSING_BUF_MSG: &str = "The buffer was submitted for io and never returned";

#[derive(Clone)]
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

#[derive(Clone)]
pub struct Buffer(Option<Inner>);

impl Buffer {
    /// Create a buffer with capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self { Self(Inner { buf: Vec::with_capacity(cap), pos: 0 }.into()) }

    #[inline]
    pub fn get_position(&self) -> usize { self.inner().pos }

    #[inline]
    pub fn set_position(&mut self, pos: usize) { self.inner_mut().pos = pos; }

    /// Get the initialized but not consumed part of the buffer.
    #[inline]
    pub fn remaining(&self) -> &[u8] { self.inner().remaining() }

    /// Return the byte count that has not been processed
    #[inline]
    pub fn remaining_len(&self) -> usize { self.remaining().len() }

    /// 截断 remaining 到指定长度
    /// maybe panic
    #[inline]
    pub fn retain_remaining<R>(&mut self, rng: R)
    where
        R: RangeBounds<usize>,
    {
        let pos = self.get_position();
        let len = self.remaining_len();
        let ops::Range { start, end } = slice::range(rng, ..len);
        if end < len {
            self.buf_mut().truncate(end + pos);
        }
        if start > 0 {
            self.buf_mut().drain(..start + pos);
            self.set_position(0); // 截断头部需要重新设置游标
        }
    }

    /// 劈掉 at.. 范围的字节
    #[inline]
    pub fn split_remaining_off(&mut self, at: usize) -> Self {
        let pos = self.get_position();
        let inner = self.buf_mut();
        let right = inner.split_off(pos + at);
        Buffer::from(right)
    }

    /// If the inner buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool { self.inner().is_empty() }

    /// All bytes in the buffer have been read
    #[inline]
    pub fn all_done(&self) -> bool { self.inner().all_done() }

    /// Returns the capacity of the uninitialized part of the buffer (`capacity() - len()`).
    #[inline]
    pub fn uninitialized_capacity(&self) -> usize { self.buf().capacity() - self.buf().len() }

    /// Clear the inner buffer and reset the position to the start.
    #[inline]
    pub fn reset(&mut self) { self.inner_mut().reset(); }

    /// 如果你想回收已经写入的空间，就使用此函数，如果你想更彻底一点回收未使用的内存，继续调用 shrink 即可
    /// compact 的语义是不改变缓冲区容量的情况下回收已经写入的字节，而 shrink 的语义是改变缓冲区容量
    /// 如果为缓冲区空就直接返回
    #[inline]
    pub fn compact(&mut self) {
        // If the buffer is empty (len is 0), there is nothing to compact
        if self.buf().is_empty() {
            return;
        }
        // If pos is 0, there is nothing to compact.
        let pos = self.get_position();
        if pos > 0 {
            let filled_len = self.buf().len() - pos;
            self.buf_mut().copy_within(pos.., 0);
            self.set_position(0);
            unsafe { self.buf_mut().set_len(filled_len) };
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

    /// 尝试将容量缩减至 limit。
    ///
    /// 逻辑如下：
    /// 1. 如果当前数据长度 (len) > limit：视为高负载状态，为了性能**不做任何操作**（防止将容量缩减至 len
    ///    后，下次写入又要扩容）。
    /// 2. 如果当前容量 (cap) <= limit：无需回收，**不做任何操作**（标准库 shrink_to 也会处理这个，但提前判断更清晰）。
    /// 3. 如果 len <= limit < cap：将容量缩减至 limit，释放多余内存。
    #[inline]
    pub fn shrink_to_limit(&mut self, limit: usize) {
        if self.buf().len() > limit {
            return;
        }
        self.buf_mut().shrink_to(limit);
    }

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
            if dst.uninitialized_capacity() >= src.len() {
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

    /// 作为左半部分连接 other buffer
    /// 会先将 other compact 一下再 append
    #[inline]
    fn left_join(&mut self, mut other: Buffer) {
        other.compact();
        self.buf_mut().append(other.buf_mut());
    }
}

impl From<&[u8]> for Buffer {
    #[inline]
    fn from(slice: &[u8]) -> Self { Self::from(slice.to_vec()) }
}

impl From<Vec<u8>> for Buffer {
    #[inline]
    fn from(vec: Vec<u8>) -> Self { Self::from_inner(Inner { buf: vec, pos: 0 }) }
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

#[derive(Debug)]
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
    pub fn shrink_all_to_limit(&mut self, min_cap: usize) {
        for buf in self.0.values_mut() {
            buf.shrink_to_limit(min_cap);
        }
    }

    #[inline]
    pub fn release_done(&mut self) { self.0.retain(|_, buf| !buf.all_done()); }

    #[inline]
    pub fn all_done(&self) -> bool { self.0.values().all(|buf| buf.all_done()) }

    #[inline]
    pub fn total_remaining_len(&self) -> usize { self.0.values().map(|buf| buf.remaining_len()).sum() }

    #[inline]
    pub fn is_empty(&self) -> bool { self.0.is_empty() }

    /// 打洞或移除部分 range
    fn invalidate_overlapped(&mut self, start: usize, end: usize) {
        // 收集所有需要修改或删除的 key
        let mut to_process = Vec::new();

        // 查找到 end 之前的所有节点。
        // 实际上只需要检查 range(..end) 的最后一个节点（因为它可能跨越 start）以及 range(start..end) 的所有节点
        // 但为了实现简单稳健，我们遍历 ..end，反向查找直到 buffer 的 end <= start 为止
        for (&buf_start, buf) in self.0.range(..end).rev() {
            let buf_end = buf_start + buf.remaining_len();
            if buf_end <= start {
                // 因为是按 key 排序的，如果当前 buf 的结束位置都在 start 之前，
                // 那更前面的 buf 肯定也不相关，可以直接停止搜索
                break;
            }
            // 发生重叠
            to_process.push(buf_start);
        }

        // 打洞
        for buf_start in to_process {
            // 弹出旧 buffer
            let mut buf = self.0.remove(&buf_start).unwrap();
            let buf_len = buf.remaining_len();
            debug_assert_ne!(buf_len, 0);
            let buf_end = buf_start + buf_len;
            // 快速跳过那些中间地带
            // 如果旧 Buffer 完全在 [start, end) 之间，它就被直接丢弃了（remove 后不 insert）
            if !(buf_start < start || buf_end > end) {
                continue;
            }
            // 检查是否需要分裂 (Split)
            let keep_left = buf_start < start;
            let keep_right = buf_end > end;

            if keep_left && keep_right {
                let left_len = start - buf_start;
                let mut new_right = buf.split_remaining_off(left_len);
                self.0.insert(buf_start, buf);
                let right_skip = end - start; // (end - buf_start) - left_len 等价于 end - start
                new_right.retain_remaining(right_skip..);
                self.0.insert(end, new_right);
            } else if keep_left {
                let keep_len = start - buf_start;
                buf.retain_remaining(..keep_len);
                self.0.insert(buf_start, buf);
            } else if keep_right {
                let skip_len = end - buf_start;
                buf.retain_remaining(skip_len..);
                self.0.insert(end, buf);
            }
        }
    }

    /// 智能写入：处理重叠、左合并、右合并
    pub fn read_from_at(&mut self, src: &[u8], pos: usize) -> usize {
        if src.is_empty() {
            return 0;
        }
        // 限制单次写入最大长度
        let write_len = src.len().min(MAX_BUF_SIZE);
        let partial_src = &src[..write_len];
        let write_end = pos + write_len;
        // 清理重叠区域
        self.invalidate_overlapped(pos, write_end);
        // 尝试左合并
        // 检查是否存在一个 Buffer 刚好结束在 pos
        // 注意：BTreeMap.range(..=pos) 的最后一个元素
        let merged_left = if let Some((&prev_key, prev_buf)) = self.0.range_mut(..=pos).next_back()
            && {
                let prev_end = prev_key + prev_buf.remaining_len();
                prev_end == pos
            }
            && prev_buf.remaining_len() + partial_src.len() <= MAX_BUF_SIZE
        {
            let _ = prev_buf.read_from(partial_src);
            true
        } else {
            false
        };
        // 如果没有左合并，就作为一个新节点插入
        if !merged_left {
            self.0.insert(pos, Buffer::from(partial_src));
        }

        // 此时，pos 处的数据已经存在（要么在左边的 buffer 里，要么是新插入的 buffer）
        // 我们需要知道当前生效的 buffer 的 key 和引用
        // 因为可能是左合并的 (key < pos)，也可能是新插入的 (key == pos)
        let (cur_buf_start, cur_buf_end) = {
            // 找 <= pos 的最后一个
            let (&k, buf) = self.0.range(..=pos).next_back().expect("Should exist after insert/merge");
            (k, k + buf.remaining_len())
        };

        // 尝试右合并 (Merge Right/Bridging)
        // 检查 current_buf_end 位置是否刚好是下一个 buffer 的开始
        // 这里的逻辑可以把两个原本断开的 buffer 连起来（填补空洞的情况）
        if let Some(&next_buf_start) = self.0.range(cur_buf_end..).next().map(|(k, _)| k) {
            if next_buf_start == cur_buf_end {
                // 只有当合并后不超过 MAX_BUF_SIZE 才合并
                let next_len = self.0.get(&next_buf_start).unwrap().remaining_len();
                let cur_len = self.0.get(&cur_buf_start).unwrap().remaining_len();
                if cur_len + next_len <= MAX_BUF_SIZE {
                    let next_buf = self.0.remove(&next_buf_start).unwrap();
                    // 拿出数据追加到左边
                    let left_buf = self.0.get_mut(&cur_buf_start).unwrap();
                    left_buf.left_join(next_buf);
                }
            }
        }
        write_len
    }

    // todo 实际上现在还是串行写入
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

        assert_eq!(vbuf.total_remaining_len(), 10);

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
        // 你的 read_from_at 实现中包含了 "Right Merge (Bridging)" 逻辑
        // 当插入 15..20 后，它会发现 end(20) == next_start(20)，从而触发合并
        vbuf.read_from_at(b"Prior", 15);

        // 修正：实际上现在的逻辑支持这种合并，结果应该是一个 Buffer
        assert_eq!(vbuf.iter_mut().count(), 1, "Current logic SHOULD support prepend-merge (bridging)");

        // 验证合并后的内容是否正确
        let mut iter = vbuf.iter_mut();
        let buf = iter.next().unwrap();
        assert_eq!(buf.offset, 15);
        assert_eq!(buf.remaining(), b"PriorLater");
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

    #[compio::test]
    async fn test_vectored_overwrite_middle() {
        let mut vbuf = VectoredBuffer::new();
        // 1. 初始: [0..10) "AAAAAAAAAA"
        vbuf.read_from_at(b"AAAAAAAAAA", 0);

        // 2. 覆盖中间: [3..7) "BBBB"
        // 这一步会触发 invalidate_overlapped：
        // - 旧 buffer 被取出
        // - 分裂出左边 [0..3) "AAA"
        // - 分裂出右边 [7..10) "AAA"
        // - 插入中间 [3..7) "BBBB"
        // - 触发左合并 (0+3) -> [0..7)
        // - 触发右合并 (7+3) -> [0..10)
        vbuf.read_from_at(b"BBBB", 3);

        assert_eq!(vbuf.total_remaining_len(), 10);

        // 如果合并逻辑完美，这里应该只有1个buffer
        assert_eq!(vbuf.iter_mut().count(), 1, "Should merge back into single buffer after overwrite");

        let mut iter = vbuf.iter_mut();
        let buf = iter.next().unwrap();
        assert_eq!(buf.remaining(), b"AAABBBBAAA");
    }

    #[compio::test]
    async fn test_vectored_overwrite_overlap_multiple() {
        let mut vbuf = VectoredBuffer::new();
        // 构造碎片: [0..5) "A...", [10..15) "B..."
        vbuf.read_from_at(b"AAAAA", 0);
        vbuf.read_from_at(b"BBBBB", 10);

        // 写入一个跨越两个 buffer 的数据: [3..12) "CCCCCCCC" (len 9)
        // 这应该：
        // 1. 截断第一个 buffer 为 [0..3) "AAA"
        // 2. 完全覆盖掉 [3..5) 的旧数据
        // 3. 填充中间空洞 [5..10)
        // 4. 覆盖掉第二个 buffer 的头部 [10..12)
        // 5. 截断第二个 buffer 为 [12..15) "BBB"
        // 6. 最终尝试合并
        vbuf.read_from_at(b"CCCCCCCCC", 3);

        // 预期结果:
        // 0..3: AAA
        // 3..12: CCCCCCCCC
        // 12..15: BBB (原 10..15 的 offset 12..15 部分，即后3个字节)
        // 总共: "AAACCCCCCCCCBBB"

        // 检查内容
        // 同样，由于首尾相接，应该合并成一个 buffer [0..15)
        assert_eq!(vbuf.iter_mut().count(), 1);
        let mut iter = vbuf.iter_mut();
        let buf = iter.next().unwrap();
        assert_eq!(buf.remaining(), b"AAACCCCCCCCCBBB");
    }

    #[compio::test]
    async fn test_vectored_bridge_gap() {
        let mut vbuf = VectoredBuffer::new();

        // 1. 左边: [0..5) "AAAAA"
        vbuf.read_from_at(b"AAAAA", 0);
        // 2. 右边: [10..15) "BBBBB"
        vbuf.read_from_at(b"BBBBB", 10);

        assert_eq!(vbuf.iter_mut().count(), 2);

        // 3. 填补中间: [5..10) "CCCCC"
        // 这应该先与左边合并 -> [0..10)
        // 然后检测到尾部 10 与右边头部 10 重合 -> 合并右边 -> [0..15)
        vbuf.read_from_at(b"CCCCC", 5);

        assert_eq!(vbuf.iter_mut().count(), 1, "Should bridge two buffers");
        let mut iter = vbuf.iter_mut();
        let buf = iter.next().unwrap();
        assert_eq!(buf.remaining(), b"AAAAACCCCCBBBBB");
    }

    #[compio::test]
    async fn test_buffer_split_and_retain() {
        // 准备一个 buffer，模拟已经被读取了一部分的情况
        let mut buf = Buffer::from(b"0123456789".as_slice());
        buf.advance(2); // pos=2, remaining="23456789"

        // 测试 retain_remaining (保留前部)
        // range 是相对于 remaining 的。 retain(..3) 意味着保留 "234"
        let mut buf_copy = buf.clone();
        buf_copy.retain_remaining(..3);
        assert_eq!(buf_copy.remaining(), b"234");
        // 验证 offset 是否正确: 原始 pos=2, 保留3个, 内部 buf 应该变短
        assert_eq!(buf_copy.get_position(), 2);

        // 测试 retain_remaining (保留后部 / 掐头)
        // retain(3..) 意味着保留 "56789"
        let mut buf_copy2 = buf.clone();
        buf_copy2.retain_remaining(3..);
        assert_eq!(buf_copy2.remaining(), b"56789");
        // 此时内部逻辑通常是移动数据或增加 pos。
        // 你的实现: self.buf_mut().drain(..start + pos);
        // start=3, pos=2 -> drain(..5). 也就是扔掉前5个字节 (01234)。
        // 剩下的就是 56789. 新的 pos 应该保持不变或者被 drain 影响?
        // Vec::drain 会移除元素，所以 pos 应该需要调整吗？
        // 看你的代码： `self.buf_mut().drain(..start + pos);`
        // Drain 之后，原来的 index 5 变成了 index 0。
        // 但是你的 `retain_remaining` 并没有修改 `self.pos`！
        // 如果 pos 还是 2，而 vec 变短了，可能会导致读取错误或者 panic。
        // **这里可能是一个潜在 Bug，需要这个测试来验证**。
    }

    #[compio::test]
    async fn test_vectored_swallow_entire_buffer() {
        let mut vbuf = VectoredBuffer::new();
        // [5..10)
        vbuf.read_from_at(b"SMALL", 5);

        // 写入 [0..20), 完全包围了 [5..10)
        vbuf.read_from_at(b"BBBBBBBBBBBBBBBBBBBB", 0);

        assert_eq!(vbuf.iter_mut().count(), 1);
        let mut iter = vbuf.iter_mut();
        let buf = iter.next().unwrap();
        assert_eq!(buf.remaining().len(), 20);
        assert_eq!(buf.remaining(), b"BBBBBBBBBBBBBBBBBBBB");
    }
}
