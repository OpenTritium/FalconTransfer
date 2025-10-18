use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use range_set_blaze::RangeSetBlaze;
use std::{
    io,
    ops::{self, BitOr, BitOrAssign, Deref, Sub, SubAssign},
    range,
};

#[derive(Debug, Clone, Copy)]
pub struct FileRange(range::RangeInclusive<usize>);

impl Deref for FileRange {
    type Target = range::RangeInclusive<usize>;

    fn deref(&self) -> &Self::Target { &self.0 }
}

pub trait IntoRangeHeader {
    fn into_header_value(&self) -> Option<String>;
}

impl From<ops::RangeInclusive<usize>> for FileRange {
    fn from(value: ops::RangeInclusive<usize>) -> Self { Self(value.into()) }
}

impl From<ops::Range<usize>> for FileRange {
    fn from(value: ops::Range<usize>) -> Self { Self((value.start..=value.end - 1).into()) }
}

impl From<FileRange> for ops::RangeInclusive<usize> {
    fn from(value: FileRange) -> Self { value.0.into() }
}

impl From<FileRange> for range::RangeInclusive<usize> {
    fn from(value: FileRange) -> Self { value.0 }
}

impl IntoRangeHeader for FileRange {
    fn into_header_value(&self) -> Option<String> {
        Some(format!("bytes={start}-{end}", start = self.0.start, end = self.0.last))
    }
}

impl FileRange {
    pub fn contains(&self, rhs: Self) -> bool { self.start >= rhs.start && self.last >= rhs.last }
}

pub struct BlockChunks {
    remaining: FileMultiRange,
    block_size: usize,
}

impl Iterator for BlockChunks {
    type Item = FileMultiRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        let (head, tail) = self.remaining.split_at(self.block_size);
        self.remaining = tail;
        Some(head)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileMultiRange(RangeSetBlaze<usize>);

impl FileMultiRange {
    pub fn split_at(&self, block_size: usize) -> (FileMultiRange, FileMultiRange) {
        let mut head_set = RangeSetBlaze::new();
        let mut tail_set = RangeSetBlaze::new();
        let mut acc_len: usize = 0;

        for rng in self.0.ranges() {
            if acc_len >= block_size {
                tail_set.ranges_insert(rng);
                continue;
            }
            let rng_len = rng.end() - rng.start() + 1;
            let needed = block_size - acc_len;
            if rng_len <= needed {
                head_set.ranges_insert(rng);
                acc_len += rng_len;
            } else {
                let split_point = rng.start() + needed - 1;
                head_set.ranges_insert(*rng.start()..=split_point);
                if split_point < *rng.end() {
                    tail_set.ranges_insert((split_point + 1)..=*rng.end());
                }
                acc_len += needed;
            }
        }
        (head_set.into(), tail_set.into())
    }

    pub fn is_superset_for(&self, rng: FileRange) -> bool {
        let rng: range::RangeInclusive<usize> = rng.into();
        self.range(rng).next().is_some()
    }

    /// 指向待写入位置
    pub fn cursor(&self) -> usize { self.last().map_or(0, |x| x + 1) }

    pub fn into_chunks(self, block_size: usize) -> BlockChunks { BlockChunks { remaining: self, block_size } }

    pub fn insert_range(&mut self, rng: FileRange) {
        let rng = rng.0.start..=rng.0.last;
        self.0.ranges_insert(rng);
    }
}

impl BitOrAssign for FileMultiRange {
    fn bitor_assign(&mut self, rhs: Self) { self.0 = self.as_ref() | rhs.as_ref() }
}

impl SubAssign for FileMultiRange {
    fn sub_assign(&mut self, rhs: Self) { self.0 = self.as_ref() - rhs.as_ref() }
}

impl BitOr for &FileMultiRange {
    type Output = FileMultiRange;

    fn bitor(self, rhs: Self) -> Self::Output { (self.as_ref() | rhs.as_ref()).into() }
}

impl Sub for &FileMultiRange {
    type Output = FileMultiRange;

    fn sub(self, rhs: Self) -> Self::Output { (self.as_ref() - rhs.as_ref()).into() }
}

impl IntoRangeHeader for FileMultiRange {
    /// 如果这是一个空返回则返回 None，不会返回空字符串
    fn into_header_value(&self) -> Option<String> {
        if self.0.is_empty() {
            return None;
        }
        let parts = self.0.ranges().map(|r| format!("{}-{}", r.start(), r.end())).collect::<Vec<_>>().join(",");
        Some(format!("bytes={parts}"))
    }
}

impl Deref for FileMultiRange {
    type Target = RangeSetBlaze<usize>;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl AsRef<RangeSetBlaze<usize>> for FileMultiRange {
    fn as_ref(&self) -> &RangeSetBlaze<usize> { &self.0 }
}

impl From<RangeSetBlaze<usize>> for FileMultiRange {
    fn from(value: RangeSetBlaze<usize>) -> Self { FileMultiRange(value) }
}

impl From<std::ops::RangeInclusive<usize>> for FileMultiRange {
    fn from(value: std::ops::RangeInclusive<usize>) -> Self { Self(RangeSetBlaze::from_iter(value)) }
}

impl From<&[std::ops::RangeInclusive<usize>]> for FileMultiRange {
    fn from(value: &[std::ops::RangeInclusive<usize>]) -> Self { Self(RangeSetBlaze::from_iter(value)) }
}

impl From<range::RangeInclusive<usize>> for FileMultiRange {
    fn from(value: range::RangeInclusive<usize>) -> Self { (value.start..=value.last).into() }
}

impl From<FileRange> for FileMultiRange {
    fn from(value: FileRange) -> Self { value.0.into() }
}

pub struct FileCursor<'a> {
    file: &'a mut File,
    rng: FileMultiRange,
    pos: u64,
}

impl<'a> FileCursor<'a> {
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
    fn from(file: &'a mut File) -> Self { FileCursor { file, rng: FileMultiRange::default(), pos: 0 } }
}
