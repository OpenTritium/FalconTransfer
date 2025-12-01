use crate::{
    http::{header_map_ext::HeaderMapExt, worker::GLOBAL_HTTP_CLIENT},
    safe_filename::timebased_filename,
    utils::safe_filename::SafeFileName,
};
use camino::Utf8Path;
use cyper::Response;
use http::header::CONTENT_TYPE;
use mime::{APPLICATION_OCTET_STREAM, Mime};
use sanitize_filename_reader_friendly::sanitize;
use sparse_ranges::RangeSet;
use std::{fmt, ops::Not};
use url::Url;

#[derive(Debug, Clone)]
pub struct HttpTaskMeta {
    url: Url,
    name: SafeFileName,
    size: Option<usize>,
    mime: Mime,
    ranges_support: bool,
}

impl HttpTaskMeta {
    pub fn is_support_ranges(&self) -> bool { self.ranges_support }

    pub fn url(&self) -> &Url { &self.url }

    pub fn name(&self) -> &str { self.name.as_str() }

    pub fn mime(&self) -> &Mime { &self.mime }

    pub fn as_path_name(&self) -> &Utf8Path { &self.name }

    /// 返回 None 代表 header 未告知文件， 返回空集合代表长度为 0
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
            let size_str = if size >= 1_073_741_824 {
                format!("{:.2} GB", size as f64 / 1_073_741_824.0)
            } else if size >= 1_048_576 {
                format!("{:.2} MB", size as f64 / 1_048_576.0)
            } else if size >= 1024 {
                format!("{:.2} KB", size as f64 / 1024.0)
            } else {
                format!("{} B", size)
            };
            write!(f, " size: {}", size_str)?;
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

pub async fn fetch_meta(url: &Url) -> cyper::Result<HttpTaskMeta> {
    if let Ok(resp) = GLOBAL_HTTP_CLIENT.head(url.clone())?.send().await {
        return Ok(resp.into());
    }
    // todo 加点请求参数
    // todo 先 range 请求一字节，如果连range 和 head 都不支持可以再 fallback
    GLOBAL_HTTP_CLIENT.get(url.clone())?.send().await.map(|resp| resp.into())
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
        meta.size = Some(1_048_576); // 1 MB
        meta.ranges_support = false;
        println!("Meta without ranges support: {}", meta);
        assert!(format!("{}", meta).contains("MB"));
        assert!(format!("{}", meta).contains("✗"));

        // Test small file (KB)
        meta.size = Some(5120); // 5 KB
        println!("Small file: {}", meta);
        assert!(format!("{}", meta).contains("KB"));

        // Test very small file (Bytes)
        meta.size = Some(512); // 512 B
        println!("Very small file: {}", meta);
        assert!(format!("{}", meta).contains("B"));
    }
}
