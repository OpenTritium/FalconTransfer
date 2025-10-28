use std::{io, sync::LazyLock};

use camino::Utf8PathBuf;
use compio::fs::{File, OpenOptions};
use directories::UserDirs;
use mime::Mime;

pub mod mime_ext {
    // --- `application/*` subtypes that should be treated as DOCUMENTS ---
    // Office Suites & Standards
    pub const PDF: &str = "pdf";
    pub const RTF: &str = "rtf";
    pub const DOC: &str = "msword";
    pub const DOCX: &str = "vnd.openxmlformats-officedocument.wordprocessingml.document";
    pub const XLS: &str = "vnd.ms-excel";
    pub const XLSX: &str = "vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    pub const PPT: &str = "vnd.ms-powerpoint";
    pub const PPTX: &str = "vnd.openxmlformats-officedocument.presentationml.presentation";
    pub const ODT: &str = "vnd.oasis.opendocument.text";
    pub const ODS: &str = "vnd.oasis.opendocument.spreadsheet";
    pub const ODP: &str = "vnd.oasis.opendocument.presentation";
    pub const WPS_TEXT: &str = "wps";
    pub const WPS_SPREADSHEET: &str = "et";
    pub const WPS_PRESENTATION: &str = "dps";

    // E-books & Comics
    pub const EPUB: &str = "epub+zip";
    pub const MOBI: &str = "x-mobipocket-ebook";
    pub const AZW: &str = "vnd.amazon.ebook";
    pub const CBZ: &str = "vnd.comicbook+zip";
    pub const CBR: &str = "vnd.comicbook-rar";

    // Text-like Data Formats often served as application/*
    pub const JSON: &str = "json";
    pub const XML: &str = "xml";
    pub const MARKDOWN: &str = "markdown";
    pub const YAML: &str = "x-yaml";
    pub const TOML: &str = "toml";
    pub const CSV: &str = "csv";
    pub const JAVASCRIPT: &str = "javascript";
    pub const CONFIG: &str = "x-config";
    pub const INI: &str = "x-ini";

    // --- `application/*` subtypes that should be treated as IMAGES ---
    pub const SVG: &str = "svg+xml";
    pub const AI: &str = "postscript"; // Adobe Illustrator
    pub const PSD: &str = "vnd.adobe.photoshop";
    pub const ODG: &str = "vnd.oasis.opendocument.graphics"; // OpenDocument Drawing
    pub const DWG: &str = "vnd.dwg"; // CAD
    pub const DXF: &str = "vnd.dxf"; // CAD

    // 3D and CAD files
    pub const STL: &str = "stl";
    pub const OBJ: &str = "x-obj";
    pub const THREE_MF: &str = "3mf";
    pub const IGES: &str = "iges";
    pub const STEP: &str = "step";
}
use mime_ext::*;

pub static USER_DIR: LazyLock<UserDirs> = LazyLock::new(|| UserDirs::new().expect("get user dirs failed"));

const FOLDER_NAME: &str = "FalconTransfer_TEST";

/// 根据 MIME 类型推荐保存路径
pub fn recommand_path_for_mime(mime: &Mime, file_name: &str) -> Utf8PathBuf {
    let home_dir = || USER_DIR.home_dir();
    let download_dir = || USER_DIR.download_dir().unwrap_or_else(home_dir);
    let document_dir = || USER_DIR.document_dir().unwrap_or_else(download_dir);
    let audio_dir = || USER_DIR.audio_dir().unwrap_or_else(download_dir);
    let image_dir = || USER_DIR.picture_dir().unwrap_or_else(download_dir);
    let video_dir = || USER_DIR.video_dir().unwrap_or_else(download_dir);
    let font_dir = || USER_DIR.font_dir().unwrap_or_else(download_dir);

    let base_dir = match mime.type_() {
        mime::TEXT => document_dir(),
        mime::AUDIO => audio_dir(),
        mime::IMAGE => image_dir(),
        mime::VIDEO => video_dir(),
        mime::FONT => font_dir(),
        mime::APPLICATION => match mime.subtype().as_str() {
            PDF | RTF | DOC | DOCX | XLS | XLSX | PPT | PPTX | ODT | ODS | ODP | WPS_TEXT | WPS_SPREADSHEET
            | WPS_PRESENTATION => document_dir(),
            EPUB | MOBI | AZW | CBZ | CBR => document_dir(),
            JSON | XML | MARKDOWN | YAML | TOML | CSV | JAVASCRIPT | CONFIG | INI => document_dir(),
            SVG | AI | PSD | ODG | DWG | DXF | STL | OBJ | THREE_MF | IGES | STEP => image_dir(),
            _ => download_dir(),
        },
        _ => download_dir(),
    };
    let base_path: Utf8PathBuf = base_dir.to_path_buf().try_into().expect("Failed to convert base_dir to PathBuf");
    base_path.join(FOLDER_NAME).join(file_name)
}

pub async fn assigned_writable_file(file_name: &str, mime: &Mime) -> io::Result<File> {
    let path = recommand_path_for_mime(mime, file_name);
    // 确保父目录存在
    if let Some(parent) = path.parent() {
        compio::fs::create_dir_all(parent).await?;
    }
    let file = OpenOptions::new().create_new(true).write(true).open(path).await?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recommand_path() {
        let pdf_mime = "application/pdf".parse::<Mime>().unwrap();
        let path = recommand_path_for_mime(&pdf_mime, "document.pdf");
        assert!(path.ends_with("FalconTransfer/document.pdf"));

        let jpg_mime = "image/jpeg".parse::<Mime>().unwrap();
        let path = recommand_path_for_mime(&jpg_mime, "photo.jpg");
        assert!(path.ends_with("FalconTransfer/photo.jpg"));

        let zip_mime = "application/zip".parse::<Mime>().unwrap();
        let path = recommand_path_for_mime(&zip_mime, "archive.zip");
        assert!(path.ends_with("FalconTransfer/archive.zip"));

        let svg_mime = "application/svg+xml".parse::<Mime>().unwrap();
        let path = recommand_path_for_mime(&svg_mime, "image.svg");
        assert!(path.ends_with("FalconTransfer/image.svg"));

        let json_mime = "application/json".parse::<Mime>().unwrap();
        let path = recommand_path_for_mime(&json_mime, "data.json");
        assert!(path.ends_with("FalconTransfer/data.json"));
    }
}
