use compio::{buf::IoBuf, fs::File, io::AsyncWriteAtExt};
use sparse_ranges::{Range, RangeSet};
use std::io;
use tracing::instrument;

pub struct FileCursor<'a> {
    file: &'a mut File,
    rng: RangeSet,
    pos: u64,
}

impl FileCursor<'_> {
    #[inline]
    pub fn into_range(self) -> RangeSet { self.rng }

    #[instrument(skip_all)]
    pub async fn write_all<T: IoBuf>(&mut self, buf: T) -> io::Result<()> {
        let len = buf.buf_len();
        let result = self.file.write_all_at(buf, self.pos).await.0;
        result.inspect(|_| {
            if len == 0 {
                return;
            }
            let pos = self.pos as usize;
            self.rng.insert_range(&Range::new(pos, pos + len - 1));
            self.pos += len as u64;
        })
    }

    pub fn with_position(file: &'_ mut File, pos: u64) -> FileCursor<'_> {
        FileCursor { file, rng: RangeSet::new(), pos }
    }

    pub fn range(&self) -> &RangeSet { &self.rng }
}

impl<'a> From<&'a mut File> for FileCursor<'a> {
    #[inline]
    fn from(file: &'a mut File) -> Self { FileCursor { file, rng: RangeSet::default(), pos: 0 } }
}
