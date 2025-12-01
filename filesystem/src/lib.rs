#![feature(slice_range)]
mod buffer;
mod buffered_file;
mod mime;
pub use buffered_file::{RandBufFile, SeqBufFile};
pub use mime::{assign_path, recommand_path_for_mime};
