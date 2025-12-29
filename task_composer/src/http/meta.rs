use crate::{
    http::{header_map_ext::HeaderMapExt, worker::HTTP_CLIENT},
    safe_filename::timebased_filename,
    utils::safe_filename::SafeFileName,
};
use camino::Utf8Path;
use cyper::Response;
use http::header::CONTENT_TYPE;
use mime::{APPLICATION_OCTET_STREAM, Mime};
use sanitize_filename_reader_friendly::sanitize;
use serde::{Deserialize, Serialize};
use sparse_ranges::RangeSet;
use std::{fmt, ops::Not};
use ubyte::ByteUnit;
use url::Url;

// Custom serialization/deserialization helpers for Mime type
mod mime_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(mime: &Mime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(mime.as_ref())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Mime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// HTTP task metadata extracted from response headers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpTaskMeta {
    url: Url,
    name: SafeFileName,
    /// File size in bytes
    size: Option<usize>,
    #[serde(with = "mime_serde")]
    mime: Mime,
    /// Server supports range requests
    ranges_support: bool,
}

impl HttpTaskMeta {
    pub const fn is_support_ranges(&self) -> bool { self.ranges_support }

    pub const fn url(&self) -> &Url { &self.url }

    pub fn name(&self) -> &str { self.name.as_str() }

    pub const fn mime(&self) -> &Mime { &self.mime }

    pub fn as_path_name(&self) -> &Utf8Path { &self.name }

    /// Returns full content range.
    ///
    /// Returns `None` if header did not specify size.
    /// Returns empty set if size is 0.
    pub fn full_content_range(&self) -> Option<RangeSet> {
        let size = self.size?;
        let mut set = RangeSet::new();
        set.insert_n_at(size, 0);
        Some(set)
    }
}

impl From<Response> for HttpTaskMeta {
    fn from(resp: Response) -> Self {
        fn parse_filename_from_url(url: &Url) -> Option<String> {
            url.path_segments()
                .and_then(|mut segs| segs.next_back())
                .and_then(|name| name.is_empty().not().then_some(name.to_string()))
        }
        let headers = resp.headers();
        let url = resp.url();
        let content_length = headers.parse_content_length();
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<Mime>().ok())
            .unwrap_or(APPLICATION_OCTET_STREAM);
        let filename = headers
            .parse_filename()
            .ok()
            .or_else(|| parse_filename_from_url(url))
            .map(|ref s| sanitize(s))
            .unwrap_or_else(|| timebased_filename(None))
            .as_str()
            .into();
        let ranges_support = headers.parse_accept_ranges();
        Self { name: filename, size: content_length, mime: content_type, ranges_support, url: url.clone() }
    }
}

impl fmt::Display for HttpTaskMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Meta['{}']", self.name)?;
        if let Some(size) = self.size {
            write!(f, " size: {}", ByteUnit::Byte(size as u64))?;
        } else {
            write!(f, " size: unknown")?;
        }
        write!(f, ", mime: {}", self.mime)?;
        if self.ranges_support {
            write!(f, " [ranges ✓]")?;
        } else {
            write!(f, " [ranges ✗]")?;
        }
        Ok(())
    }
}

/// Fetches HTTP metadata from URL using HEAD request, falling back to GET.
///
/// TODO: Add request parameters.
/// TODO: Try range request for 1 byte before full GET fallback.
pub async fn fetch_meta(url: &Url) -> cyper::Result<HttpTaskMeta> {
    if let Ok(resp) = HTTP_CLIENT.head(url.clone())?.send().await {
        return Ok(resp.into());
    }
    // Fallback to GET if HEAD fails
    HTTP_CLIENT.get(url.clone())?.send().await.map(|resp| resp.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[compio::test]
    async fn test_fetch_meta() {
        let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
        let meta = fetch_meta(&url).await.unwrap();
        println!("{:?}", meta);
    }

    #[test]
    fn test_http_task_meta_display() {
        // Test with full size and ranges support
        let mut meta = HttpTaskMeta {
            url: Url::parse("https://example.com/file.iso").unwrap(),
            name: "ubuntu-24.04.iso".into(),
            size: Some(6_000_000_000), // 6 GB
            mime: "application/x-iso9660-image".parse().unwrap(),
            ranges_support: true,
        };
        println!("Full meta with ranges: {}", meta);
        assert!(format!("{}", meta).contains("ubuntu-24.04.iso"));
        assert!(format!("{}", meta).contains("GB"));
        assert!(format!("{}", meta).contains("✓"));

        // Test without size
        meta.size = None;
        println!("Meta without size: {}", meta);
        assert!(format!("{}", meta).contains("unknown"));

        // Test without ranges support
        meta.size = Some(1_048_576); // 1 MiB
        meta.ranges_support = false;
        println!("Meta without ranges support: {}", meta);
        assert!(format!("{}", meta).contains("MiB"));
        assert!(format!("{}", meta).contains("✗"));

        // Test small file (KiB)
        meta.size = Some(5120); // 5 KiB
        println!("Small file: {}", meta);
        assert!(format!("{}", meta).contains("KiB"));

        // Test very small file (Bytes)
        meta.size = Some(512); // 512 B
        println!("Very small file: {}", meta);
        assert!(format!("{}", meta).contains("B"));
    }
}
