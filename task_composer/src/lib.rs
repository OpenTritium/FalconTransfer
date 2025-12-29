#![feature(new_range_api)]
#![feature(if_let_guard)]
#![feature(box_patterns)]
#![feature(try_blocks)]
#![feature(impl_trait_in_assoc_type)]
// 仅在非 test 构建时启用这些 Clippy 警告
#![cfg_attr(not(test), warn(clippy::nursery, clippy::unwrap_used, clippy::todo, clippy::dbg_macro,))]
#![allow(clippy::future_not_send)]
mod http;
mod utils;
pub use http::*;
pub use utils::*;
