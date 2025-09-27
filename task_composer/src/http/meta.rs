use crate::{
    http::{file_range::FileMultiRange, header_map_ext::HeaderMapExt},
    utils::safe_filename::SafeFileName,
};
use camino::Utf8Path;
use fastdate::DateTime;
use http::header::CONTENT_TYPE;
use mime::{APPLICATION_OCTET_STREAM, Mime};
use reqwest::Response;
use sanitize_filename_reader_friendly::sanitize;
use url::Url;

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

    pub fn path(&self) -> &Utf8Path { &self.name }

    /// 返回 None 代表 header 未告知文件大小
    /// 返回空 rng 代表文件大小为 0
    pub fn content_range(&self) -> Option<FileMultiRange> {
        let size = self.size?;
        if size == 0 {
            return Some(FileMultiRange::default());
        }
        let end = size - 1;
        let rgn = 0..=end;
        Some(FileMultiRange::from(rgn))
    }
}

impl From<Response> for HttpTaskMeta {
    fn from(resp: Response) -> Self {
        fn parse_filename_from_url(url: &Url) -> Option<String> {
            url.path_segments()
                .and_then(|mut segs| segs.next_back())
                .and_then(|name| name.is_empty().then_some(name.to_string()))
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
            .unwrap_or_else(|| format!("{}.bin", DateTime::now()))
            .as_str()
            .into();
        let ranges_support = headers.parse_accept_ranges();
        Self { name: filename, size: content_length, mime: content_type, ranges_support, url: url.clone() }
    }
}
