use crate::safe_filename::timebased_filename;
use HeaderMapExtError::*;
use http::{
    HeaderMap, HeaderName,
    header::{ACCEPT_RANGES, CONTENT_DISPOSITION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
};
use mime::{FromStrError, Mime};
use percent_encoding::percent_decode_str;
use regex::Regex;
use sparse_ranges::Range;
use thiserror::Error;
use tracing::instrument;

#[derive(Debug, Error)]
pub enum HeaderMapExtError {
    #[error("{0} does not exist")]
    FieldNotExist(HeaderName),
    #[error("Invalid format for header {0}: {1}")]
    FieldFormat(HeaderName, String),
    #[error(transparent)]
    HeaderToStr(#[from] http::header::ToStrError),
    #[error(transparent)]
    InvalidMime(#[from] FromStrError),
    #[error("Not multipart content type: {0}")]
    NotMultipart(Mime),
    #[error("Not found boundary in mime: {0}")]
    NotFoundBoundary(Mime),
}

static INVALID_REGEX_PANIC_MSG: &str = "Invalid regex";
thread_local! {
    // 加上 (?i) 忽略大小写以防万一，虽然标准通常是 bytes
    static SINGLE_CONTENT_RANGE_PATTERN: Regex =
        Regex::new(r"(?i-u:^bytes\s+(\d+)-(\d+)/(\d+)$)").expect(INVALID_REGEX_PANIC_MSG);

    static CONTENT_DISPOSITION_PATTERN: Regex =
    Regex::new(r#"(?xi) # 使用 x (忽略空白) 和 i (忽略大小写) 模式
        # 优先匹配 filename* (RFC 5987 / 8187)
        filename\*=
            ([\w-]+) # Group 1: Charset (e.g., UTF-8)
            ''
            ([^;]+)          # Group 2: Encoded value (e.g., %E6%96%87...)
        | # OR
        # 其次匹配 filename="..." (RFC 2616)
        filename="
            ([^"]+)          # Group 3: Quoted value
        "
        | # OR
        # 最后匹配 filename=... (兼容性写法)
        filename=
            ([^;]+)          # Group 4: Unquoted value
    "#).expect(INVALID_REGEX_PANIC_MSG);
}

pub trait HeaderMapExt {
    fn parse_boundary(&self) -> Result<Box<[u8]>, HeaderMapExtError>;
    fn parse_single_content_range(&self) -> Result<(Range, usize), HeaderMapExtError>;
    fn parse_filename(&self) -> Result<String, HeaderMapExtError>;
    fn parse_accept_ranges(&self) -> bool;
    fn parse_content_length(&self) -> Option<usize>;
}

impl HeaderMapExt for HeaderMap {
    #[instrument(skip_all)]
    fn parse_boundary(&self) -> Result<Box<[u8]>, HeaderMapExtError> {
        let content_type = self.get(CONTENT_TYPE).ok_or(FieldNotExist(CONTENT_TYPE))?.to_str()?;
        let mime: Mime = content_type.parse()?;
        if mime.type_() != mime::MULTIPART {
            return Err(NotMultipart(mime));
        }
        let bnd = mime.get_param(mime::BOUNDARY).ok_or_else(|| NotFoundBoundary(mime.clone()))?.as_str();
        Ok(bnd.as_bytes().into())
    }

    fn parse_single_content_range(&self) -> Result<(Range, usize), HeaderMapExtError> {
        let content_range = self.get(CONTENT_RANGE).ok_or(FieldNotExist(CONTENT_RANGE))?.to_str()?;
        let caps = SINGLE_CONTENT_RANGE_PATTERN
            .with(|regex| regex.captures(content_range))
            .ok_or_else(|| FieldFormat(CONTENT_RANGE, content_range.to_string()))?;
        let cap_idx = |idx| {
            caps.get(idx)
                .ok_or_else(|| FieldFormat(CONTENT_RANGE, content_range.to_string()))?
                .as_str()
                .parse::<usize>()
                .map_err(|err| FieldFormat(CONTENT_RANGE, format!("raw: {content_range}, parse int err: {err}")))
        };
        let start = cap_idx(1)?;
        let last = cap_idx(2)?;
        let total = cap_idx(3)?;
        let rng = Range::new(start, last);
        Ok((rng, total))
    }

    #[instrument(skip_all)]
    fn parse_filename(&self) -> Result<String, HeaderMapExtError> {
        let content_disposition =
            self.get(CONTENT_DISPOSITION).ok_or_else(|| FieldNotExist(CONTENT_DISPOSITION))?.to_str()?;
        CONTENT_DISPOSITION_PATTERN
            .with(|regex| regex.captures(content_disposition))
            .and_then(|caps| {
                // 匹配 Group 1(charset) 和 2 (filename*)
                if let (Some(charset), Some(encoded_value)) = (caps.get(1), caps.get(2)) {
                    if charset.as_str() != "UTF-8" {
                        return Some(timebased_filename(None));
                    }
                    // 这个值是必须 percent-decoded 的
                    return Some(percent_decode_str(encoded_value.as_str()).decode_utf8_lossy().to_string());
                }
                // 匹配 Group 3 (Quoted filename)
                if let Some(quoted_value) = caps.get(3) {
                    // 普通 filename 不需要 percent-decode
                    return Some(quoted_value.as_str().to_string());
                }
                // 匹配 Group 4 (Unquoted)
                if let Some(unquoted_value) = caps.get(4) {
                    return Some(unquoted_value.as_str().trim().to_string());
                }
                None
            })
            .ok_or_else(|| FieldFormat(CONTENT_DISPOSITION, content_disposition.to_string()))
    }

    #[inline]
    fn parse_accept_ranges(&self) -> bool {
        self.get(ACCEPT_RANGES).and_then(|v| v.to_str().ok()).map(|s| s.contains("bytes")).unwrap_or(false)
    }

    #[inline]
    fn parse_content_length(&self) -> Option<usize> {
        self.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<usize>().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};
    use rstest::rstest;

    fn create_headers(name: HeaderName, value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(name, HeaderValue::from_str(value).unwrap());
        headers
    }

    fn content_type_headers(value: &str) -> HeaderMap { create_headers(CONTENT_TYPE, value) }

    #[test]
    fn parse_boundary_success() {
        let headers = content_type_headers(r#"multipart/form-data; boundary=----WebKitFormBoundaryXQc971Yqg1DkF38c"#);
        let result = headers.parse_boundary();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), b"----WebKitFormBoundaryXQc971Yqg1DkF38c");
    }

    #[test]
    fn parse_boundary_not_multipart() {
        let headers = content_type_headers("application/json");
        let result = headers.parse_boundary();
        assert!(matches!(result, Err(HeaderMapExtError::NotMultipart(_))));
    }

    #[test]
    fn parse_boundary_not_found_boundary() {
        let headers = content_type_headers("multipart/mixed"); // Standard requires boundary
        let result = headers.parse_boundary();
        assert!(matches!(result, Err(HeaderMapExtError::NotFoundBoundary(_))));
    }

    #[test]
    fn parse_boundary_missing_header() {
        let headers = HeaderMap::new();
        let result = headers.parse_boundary();
        assert!(matches!(result, Err(HeaderMapExtError::FieldNotExist(ref name)) if name == CONTENT_TYPE));
    }

    // --- parse_single_content_range Tests ---

    #[rstest]
    #[case("bytes 0-1023/2048", 0, 1023, 2048)] // Typical first range
    #[case("bytes 1024-2047/2048", 1024, 2047, 2048)] // Typical middle range
    #[case("bytes 2048-2048/2049", 2048, 2048, 2049)] // Single byte range (e.g., last byte)
    // 忽略大小写测试
    #[case("BYTES 0-1023/2048", 0, 1023, 2048)]
    fn parse_single_content_range_success(
        #[case] value: &str, #[case] expected_start: usize, #[case] expected_end: usize, #[case] expected_total: usize,
    ) {
        let headers = create_headers(CONTENT_RANGE, value);
        let result = headers.parse_single_content_range();
        assert!(result.is_ok());
        let (range, total) = result.unwrap();
        assert_eq!(range.start(), expected_start);
        assert_eq!(range.last(), expected_end);
        assert_eq!(total, expected_total);
    }

    #[rstest]
    #[case("0-1023/2048")] // Missing 'bytes'
    #[case("bytes 0-1023")] // Missing total
    #[case("bytes 0-1023/total")] // Invalid number format
    #[case("bytes 0-1023/")] // Invalid format (trailing slash)
    fn parse_single_content_range_format_error(#[case] value: &str) {
        let headers = create_headers(CONTENT_RANGE, value);
        let result = headers.parse_single_content_range();
        assert!(matches!(result, Err(HeaderMapExtError::FieldFormat(ref name, _)) if name == CONTENT_RANGE));
    }

    #[test]
    fn parse_single_content_range_missing_header() {
        let headers = HeaderMap::new();
        let result = headers.parse_single_content_range();
        assert!(matches!(result, Err(HeaderMapExtError::FieldNotExist(ref name)) if name == CONTENT_RANGE));
    }

    // --- parse_filename Tests ---

    #[test]
    fn parse_filename_rfc5987_utf8() {
        let value = r#"attachment; filename*=UTF-8''%E6%B5%8B%E8%AF%95%E6%96%87%E4%BB%B6.txt"#;

        let headers = create_headers(CONTENT_DISPOSITION, value);
        let result = headers.parse_filename();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "测试文件.txt");
    }

    #[test]
    fn parse_filename_rfc5987_non_utf8_fallback() {
        let value = r#"attachment; filename*=GBK''%E6%B5%8B%E8%AF%95%E6%96%87%E4%BB%B6.txt"#;
        let headers = create_headers(CONTENT_DISPOSITION, value);
        let result = headers.parse_filename();
        assert!(result.is_ok());
        let filename = result.unwrap();
        // 检查返回值是否符合动态时间戳的模式（如果 timebased_filename 返回动态时间）
        // 示例：检查字符串是否以 .bin 结尾，并且长度合理（表示不是文件名本身，而是 fallback）
        assert!(filename.ends_with(".bin"));
        // 并且检查它不是预期的文件名（它应该是一个 fallback 值）
        assert_ne!(filename, "测试文件.txt");
        // 如果您确定 timebased_filename 的输出格式，可以更严格地检查：
        // assert!(filename.starts_with("20")); // 检查是否以年份开始
    }

    // RFC 2616: filename="..." (其次匹配)
    #[test]
    fn parse_filename_quoted_success() {
        // 普通 filename 不需要 percent-decode，取 Group 3
        let value = r#"attachment; filename="document.pdf""#;
        let headers = create_headers(CONTENT_DISPOSITION, value);
        let result = headers.parse_filename();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "document.pdf");
    }

    // Unquoted: filename=... (最后匹配，兼容性)
    #[test]
    fn parse_filename_unquoted_success() {
        // Group 4
        let value = r#"attachment; filename=myimage.jpg; other=value"#;
        let headers = create_headers(CONTENT_DISPOSITION, value);
        let result = headers.parse_filename();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "myimage.jpg");
    }

    #[test]
    fn parse_filename_format_error() {
        // 无法匹配任何一种格式
        let value = r#"inline; name=field"#;
        let headers = create_headers(CONTENT_DISPOSITION, value);
        let result = headers.parse_filename();
        assert!(matches!(result, Err(HeaderMapExtError::FieldFormat(ref name, _)) if name == CONTENT_DISPOSITION));
    }

    #[test]
    fn parse_filename_missing_header() {
        let headers = HeaderMap::new();
        let result = headers.parse_filename();
        assert!(matches!(result, Err(HeaderMapExtError::FieldNotExist(ref name)) if name == CONTENT_DISPOSITION));
    }

    // --- parse_accept_ranges Tests ---

    #[test]
    fn parse_accept_ranges_bytes_true() {
        let headers = create_headers(ACCEPT_RANGES, "bytes");
        assert!(headers.parse_accept_ranges());
    }

    #[test]
    fn parse_accept_ranges_none_false() {
        let headers = create_headers(ACCEPT_RANGES, "none");
        assert!(!headers.parse_accept_ranges());
    }

    #[test]
    fn parse_accept_ranges_missing_header_false() {
        let headers = HeaderMap::new();
        assert!(!headers.parse_accept_ranges());
    }

    // --- parse_content_length Tests ---

    #[test]
    fn parse_content_length_success() {
        let headers = create_headers(CONTENT_LENGTH, "123456");
        assert_eq!(headers.parse_content_length(), Some(123456));
    }

    #[test]
    fn parse_content_length_invalid_format_none() {
        let headers = create_headers(CONTENT_LENGTH, "not_a_number");
        assert_eq!(headers.parse_content_length(), None);
    }

    #[test]
    fn parse_content_length_missing_header_none() {
        let headers = HeaderMap::new();
        assert_eq!(headers.parse_content_length(), None);
    }
}
