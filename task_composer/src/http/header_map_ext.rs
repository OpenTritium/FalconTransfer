use std::sync::LazyLock;

use http::{
    HeaderMap, HeaderName,
    header::{ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
};
use percent_encoding::percent_decode_str;
use regex::Regex;
use thiserror::Error;
use url::Url;

use crate::http::file_range::FileRange;

#[derive(Debug, Error)]
pub enum HeaderMapExtError {
    #[error("")]
    FieldNotExist(HeaderName),
    #[error("")]
    FieldFormat(HeaderName, String),
    #[error("")]
    HeaderToStr(#[from] http::header::ToStrError),
}

pub trait HeaderMapExt {
    fn parse_boundary(&self) -> Result<Box<[u8]>, HeaderMapExtError>;
    fn parse_content_range(&self) -> Result<(FileRange, usize), HeaderMapExtError>;
    fn parse_filename(&self) -> Result<String, HeaderMapExtError>;
    fn parse_accept_ranges(&self) -> bool;
    fn parse_content_length(&self) -> Option<usize>;
}

impl HeaderMapExt for HeaderMap {
    fn parse_boundary(&self) -> Result<Box<[u8]>, HeaderMapExtError> {
        use HeaderMapExtError::*;
        let content_type =
            self.get(CONTENT_TYPE).ok_or_else(|| FieldNotExist(CONTENT_TYPE)).and_then(|h| Ok(h.to_str()?))?;
        if !content_type.contains("multipart/byteranges") {
            return Err(FieldFormat(CONTENT_TYPE, content_type.to_string()));
        }
        let Some(bd_str) = content_type.split("boundary=").nth(1) else {
            return Err(FieldFormat(CONTENT_TYPE, content_type.to_string()));
        };
        let bd = bd_str.trim().as_bytes().to_vec().into_boxed_slice();
        Ok(bd)
    }

    // 总是返回单个rng
    fn parse_content_range(&self) -> Result<(FileRange, usize), HeaderMapExtError> {
        use HeaderMapExtError::*;
        let content_range =
            self.get(CONTENT_RANGE).ok_or_else(|| FieldNotExist(CONTENT_RANGE)).and_then(|h| Ok(h.to_str()?))?;
        static REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?-u:^bytes\s+(\d+)-(\d+)/(\d+)$)").unwrap());
        let caps =
            REGEX.captures(content_range).ok_or_else(|| FieldFormat(CONTENT_RANGE, content_range.to_string()))?;
        let cap_idx = |idx| {
            caps.get(idx)
                .ok_or_else(|| FieldFormat(CONTENT_RANGE, content_range.to_string()))?
                .as_str()
                .parse::<usize>()
                .map_err(|err| {
                    FieldFormat(CONTENT_RANGE, format!("raw: {}, parse int err: {:?}", content_range.to_string(), err))
                })
        };
        let start = cap_idx(1)?;
        let end = cap_idx(2)?;
        let total = cap_idx(3)?;
        let rng = (start..=end).into();
        Ok((rng, total))
    }

    fn parse_filename(&self) -> Result<String, HeaderMapExtError> {
        use HeaderMapExtError::*;
        let content_disposition = self
            .get(CONTENT_DISPOSITION)
            .ok_or_else(|| FieldNotExist(CONTENT_DISPOSITION))
            .and_then(|h| Ok(h.to_str()?))?;
        static REGEX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r#"filename\*=(?:UTF-8'')?([^;]+)|filename="([^"]+)"|filename=([^;]+)"#).unwrap()
        });
        REGEX
            .captures(content_disposition)
            .and_then(|caps| {
                let raw_filename = caps.get(1).or_else(|| caps.get(2)).or_else(|| caps.get(3))?.as_str();
                let decoded_filename = if raw_filename.contains("%") {
                    percent_decode_str(raw_filename).decode_utf8_lossy().to_string()
                } else {
                    raw_filename.to_string()
                };
                Some(decoded_filename)
            })
            .ok_or_else(|| FieldFormat(CONTENT_DISPOSITION, content_disposition.to_string()))
    }

    fn parse_accept_ranges(&self) -> bool {
        self.get(ACCEPT_RANGES).and_then(|v| v.to_str().ok()).map(|s| s.contains("bytes")).unwrap_or(false)
    }

    fn parse_content_length(&self) -> Option<usize> {
        self.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<usize>().ok())
    }
}
