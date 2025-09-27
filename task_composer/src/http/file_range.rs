use range_set_blaze::RangeSetBlaze;
use std::{
    ops::{BitOr, BitOrAssign, Deref, Sub, SubAssign},
    range,
};

pub trait IntoRangeHeader {
    fn into_header_value(&self) -> Option<String>;
}

impl From<std::ops::RangeInclusive<usize>> for FileRange {
    fn from(value: std::ops::RangeInclusive<usize>) -> Self { Self(value.into()) }
}

#[derive(Debug, Clone, Copy)]
pub struct FileRange(range::RangeInclusive<usize>);

impl IntoRangeHeader for FileRange {
    fn into_header_value(&self) -> Option<String> {
        Some(format!("bytes={start}-{end}", start = self.0.start, end = self.0.end))
    }
}

pub struct Window<'a> {
    max: usize,
    inner: &'a mut FileMultiRange,
}

impl<'a> Iterator for Window<'a> {
    type Item = FileMultiRange;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_empty() {
            return None;
        }
        let (head, tail) = self.inner.split_at(self.max);
        *self.inner = tail;
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

    pub fn window(&mut self, max: usize) -> Window<'_> { Window { max, inner: self } }

    pub fn insert_range(&mut self, rng: FileRange) {
        let rng = rng.0.start..=rng.0.end;
        self.0.ranges_insert(rng);
    }

    pub fn push_n_at(&mut self, at: usize, n: usize) {
        if n == 0 {
            return;
        }
        let start = at;
        let end = self.last().and_then(|end| (end > at).then_some(end + n)).unwrap_or(at + n) - 1;
        let rng = start..=end;
        self.0.ranges_insert(rng);
    }
}

impl BitOrAssign for FileMultiRange {
    fn bitor_assign(&mut self, rhs: Self) { self.0 = (self.as_ref() | rhs.as_ref()).into() }
}

impl SubAssign for FileMultiRange {
    fn sub_assign(&mut self, rhs: Self) { self.0 = (self.as_ref() - rhs.as_ref()).into() }
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
    fn from(value: std::ops::RangeInclusive<usize>) -> Self { Self(RangeSetBlaze::from_iter([value])) }
}

impl From<&[std::ops::RangeInclusive<usize>]> for FileMultiRange {
    fn from(value: &[std::ops::RangeInclusive<usize>]) -> Self { Self(RangeSetBlaze::from_iter(value)) }
}
