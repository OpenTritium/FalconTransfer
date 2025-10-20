use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use range_set_blaze::RangeSetBlaze;
use std::{
    io, mem,
    ops::{self, BitOr, BitOrAssign, Deref, DerefMut, Sub, SubAssign},
    range,
};

#[derive(Debug, Clone, Copy)]
pub struct FileRange(range::RangeInclusive<usize>);

impl Deref for FileRange {
    type Target = range::RangeInclusive<usize>;

    #[inline]
    fn deref(&self) -> &Self::Target { &self.0 }
}

pub trait IntoRangeHeader {
    fn as_header_value(&self) -> Option<String>;
}

impl From<ops::RangeInclusive<usize>> for FileRange {
    #[inline]
    fn from(value: ops::RangeInclusive<usize>) -> Self { Self(value.into()) }
}

impl From<ops::Range<usize>> for FileRange {
    #[inline]
    fn from(value: ops::Range<usize>) -> Self { Self((value.start..=value.end - 1).into()) }
}

impl From<FileRange> for ops::RangeInclusive<usize> {
    #[inline]
    fn from(value: FileRange) -> Self { value.0.into() }
}

impl From<FileRange> for range::RangeInclusive<usize> {
    #[inline]
    fn from(value: FileRange) -> Self { value.0 }
}

impl IntoRangeHeader for FileRange {
    fn as_header_value(&self) -> Option<String> {
        Some(format!("bytes={start}-{end}", start = self.0.start, end = self.0.last))
    }
}

impl FileRange {
    #[inline]
    pub fn contains(&self, rhs: Self) -> bool { self.start >= rhs.start && self.last >= rhs.last }
}

#[derive(Debug)]
pub struct BlockChunks<'a> {
    remaining: &'a mut FileMultiRange,
    block_size: usize,
}

impl Iterator for BlockChunks<'_> {
    type Item = FileMultiRange;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mid = self.remaining.first()? + self.block_size - 1;
        let mut tail = self.remaining.split_off(mid);
        // 尾巴已经没东西了，检查刚切出来的东西
        if tail.is_empty() {
            if self.remaining.is_empty() {
                return None;
            }
            return Some(mem::take(self.remaining));
        }
        mem::swap(&mut tail, self.remaining);
        Some(FileMultiRange(tail))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileMultiRange(RangeSetBlaze<usize>);

impl FileMultiRange {
    #[inline]
    pub fn is_superset_for(&self, rng: FileRange) -> bool {
        let rng: range::RangeInclusive<usize> = rng.into();
        self.range(rng).next().is_some()
    }

    #[inline]
    pub fn as_block_chunks(&mut self, block_size: usize) -> Option<BlockChunks<'_>> {
        Some(BlockChunks { remaining: self, block_size })
    }

    /// 指向待写入位置
    #[inline]
    pub fn cursor(&self) -> usize { self.last().map_or(0, |x| x + 1) }

    #[inline]
    pub fn insert_range(&mut self, rng: FileRange) {
        let rng = rng.0.start..=rng.0.last;
        self.0.ranges_insert(rng);
    }

    #[inline]
    pub fn union_assign(&mut self, rhs: &Self) { self.0 = self.as_ref() | rhs.as_ref() }

    #[inline]
    pub fn difference_assign(&mut self, rhs: &Self) { self.0 = self.as_ref() - rhs.as_ref() }
}

impl BitOr for &FileMultiRange {
    type Output = FileMultiRange;

    #[inline]
    fn bitor(self, rhs: Self) -> Self::Output { (self.as_ref() | rhs.as_ref()).into() }
}

impl Sub for &FileMultiRange {
    type Output = FileMultiRange;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output { (self.as_ref() - rhs.as_ref()).into() }
}

impl IntoRangeHeader for FileMultiRange {
    /// 如果这是一个空返回则返回 None，不会返回空字符串
    #[inline]
    fn as_header_value(&self) -> Option<String> {
        if self.0.is_empty() {
            return None;
        }
        let parts = self.0.ranges().map(|r| format!("{}-{}", r.start(), r.end())).collect::<Box<[_]>>().join(",");
        Some(format!("bytes={parts}"))
    }
}

impl Deref for FileMultiRange {
    type Target = RangeSetBlaze<usize>;

    #[inline]
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl DerefMut for FileMultiRange {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
}

impl AsRef<RangeSetBlaze<usize>> for FileMultiRange {
    #[inline]
    fn as_ref(&self) -> &RangeSetBlaze<usize> { &self.0 }
}

impl From<RangeSetBlaze<usize>> for FileMultiRange {
    #[inline]
    fn from(value: RangeSetBlaze<usize>) -> Self { FileMultiRange(value) }
}

impl From<std::ops::RangeInclusive<usize>> for FileMultiRange {
    #[inline]
    fn from(value: std::ops::RangeInclusive<usize>) -> Self { Self(RangeSetBlaze::from_iter(value)) }
}

impl From<&[std::ops::RangeInclusive<usize>]> for FileMultiRange {
    #[inline]
    fn from(value: &[std::ops::RangeInclusive<usize>]) -> Self { Self(RangeSetBlaze::from_iter(value)) }
}

impl From<range::RangeInclusive<usize>> for FileMultiRange {
    #[inline]
    fn from(value: range::RangeInclusive<usize>) -> Self { (value.start..=value.last).into() }
}

impl From<FileRange> for FileMultiRange {
    #[inline]
    fn from(value: FileRange) -> Self { value.0.into() }
}

pub struct FileCursor<'a> {
    file: &'a mut File,
    rng: FileMultiRange,
    pos: u64,
}

impl<'a> FileCursor<'a> {
    #[inline]
    pub fn into_range(self) -> FileMultiRange { self.rng }

    pub async fn write_all<T: IoBuf>(&mut self, buf: T) -> io::Result<()> {
        let len = buf.buf_len();
        // 修改点：使用 self.position 作为写入位置
        let result = self.file.write_all_at(buf, self.pos).await.0;

        result.inspect(|_| {
            let pos = self.pos as usize;
            let written_range = (pos)..(pos + len);
            self.rng.insert_range(written_range.into());
            self.pos += len as u64;
        })
    }

    pub fn with_position(file: &'a mut File, pos: u64) -> FileCursor<'a> {
        FileCursor { file, rng: FileMultiRange::default(), pos }
    }

    pub fn range(&self) -> &FileMultiRange { &self.rng }
}

impl<'a> From<&'a mut File> for FileCursor<'a> {
    #[inline]
    fn from(file: &'a mut File) -> Self { FileCursor { file, rng: FileMultiRange::default(), pos: 0 } }
}
