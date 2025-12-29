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

/// Errors from parsing HTTP header values.
#[derive(Debug, Error)]
pub enum HeaderMapExtError {
    #[error("Required header '{0}' is missing")]
    FieldNotExist(HeaderName),
    #[error("Invalid format for header '{0}': {1}")]
    FieldFormat(HeaderName, String),
    #[error("Failed to convert header value to string: {0}")]
    HeaderToStr(#[from] http::header::ToStrError),
    #[error("Failed to parse MIME type: {0}")]
    InvalidMime(#[from] FromStrError),
    #[error("Expected multipart content type, got: {0}")]
    NotMultipart(Mime),
    #[error("Multipart content type '{0}' is missing required 'boundary' parameter")]
    NotFoundBoundary(Mime),
}

static INVALID_REGEX_PANIC_MSG: &str = "Invalid regex";

thread_local! {
    // (?i) for case-insensitive matching
    static SINGLE_CONTENT_RANGE_PATTERN: Regex =
        Regex::new(r"(?i-u:^bytes\s+(\d+)-(\d+)/(\d+)$)").expect(INVALID_REGEX_PANIC_MSG);

    static CONTENT_DISPOSITION_PATTERN: Regex =
    Regex::new(r#"(?xi) # x (ignore whitespace) and i (case-insensitive) mode
        # Priority 1: filename* (RFC 5987 / 8187)
        filename\*=
            ([\w-]+) # Group 1: Charset (e.g., UTF-8)
            ''
            ([^;]+)          # Group 2: Encoded value (e.g., %E6%96%87...)
        | # OR
        # Priority 2: filename="..." (RFC 2616)
        filename="
            ([^"]+)          # Group 3: Quoted value
        "
        | # OR
        # Priority 3: filename=... (fallback compatibility)
        filename=
            ([^;]+)          # Group 4: Unquoted value
    "#).expect(INVALID_REGEX_PANIC_MSG);
}

/// Extension trait for parsing HTTP headers.
pub trait HeaderMapExt {
    /// Parses multipart boundary from Content-Type header.
    fn parse_boundary(&self) -> Result<Box<[u8]>, HeaderMapExtError>;
    /// Parses single Content-Range header value.
    fn parse_single_content_range(&self) -> Result<(Range, usize), HeaderMapExtError>;
    /// Parses filename from Content-Disposition header.
    fn parse_filename(&self) -> Result<String, HeaderMapExtError>;
    /// Checks if server supports range requests.
    fn parse_accept_ranges(&self) -> bool;
    /// Parses Content-Length header value.
    fn parse_content_length(&self) -> Option<usize>;
}

impl HeaderMapExt for HeaderMap {
    fn parse_boundary(&self) -> Result<Box<[u8]>, HeaderMapExtError> {
        let content_type = self.get(CONTENT_TYPE).ok_or(FieldNotExist(CONTENT_TYPE))?.to_str()?;
        let mime: Mime = content_type.parse()?;
        if mime.type_() != mime::MULTIPART {
            return Err(NotMultipart(mime));
        }
        let boundary = mime.get_param(mime::BOUNDARY).ok_or_else(|| NotFoundBoundary(mime.clone()))?.as_str();
        Ok(boundary.as_bytes().into())
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

    fn parse_filename(&self) -> Result<String, HeaderMapExtError> {
        let content_disposition =
            self.get(CONTENT_DISPOSITION).ok_or_else(|| FieldNotExist(CONTENT_DISPOSITION))?.to_str()?;
        CONTENT_DISPOSITION_PATTERN
            .with(|regex| regex.captures(content_disposition))
            .and_then(|caps| {
                // Match Group 1 (charset) and 2 (filename*)
                if let (Some(charset), Some(encoded_value)) = (caps.get(1), caps.get(2)) {
                    if charset.as_str() != "UTF-8" {
                        return Some(timebased_filename(None));
                    }
                    // Value must be percent-decoded
                    return Some(percent_decode_str(encoded_value.as_str()).decode_utf8_lossy().to_string());
                }
                // Match Group 3 (Quoted filename)
                if let Some(quoted_value) = caps.get(3) {
                    // Regular filename does not need percent-decode
                    return Some(quoted_value.as_str().to_string());
                }
                // Match Group 4 (Unquoted)
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
    // Case-insensitive test
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
        // Check fallback to timebased filename pattern
        assert!(filename.ends_with(".bin"));
        assert_ne!(filename, "测试文件.txt");
    }

    // RFC 2616: filename="..." (Priority 2)
    #[test]
    fn parse_filename_quoted_success() {
        // Regular filename does not need percent-decode, Group 3
        let value = r#"attachment; filename="document.pdf""#;
        let headers = create_headers(CONTENT_DISPOSITION, value);
        let result = headers.parse_filename();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "document.pdf");
    }

    // Unquoted: filename=... (Priority 3, fallback compatibility)
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
        // Cannot match any format
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
