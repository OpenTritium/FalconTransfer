#![feature(slice_range)]
#![cfg_attr(not(test), warn(clippy::nursery, clippy::unwrap_used, clippy::todo, clippy::dbg_macro,))]
#![allow(clippy::future_not_send)]
mod buffer;
mod buffered_file;
mod mime;
pub use buffered_file::{RandBufFile, SeqBufFile};
pub use mime::assign_path;
