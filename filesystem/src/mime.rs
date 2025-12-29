use camino::{Utf8Path, Utf8PathBuf};
use directories::UserDirs;
use mime::Mime;
use std::{io, sync::LazyLock};

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

const FOLDER_NAME: &str = "FalconTransfer";
const FOLDER_NAME_TEST: &str = "FalconTransferTests";

/// 根据 MIME 类型推荐保存路径
fn recommand_path_for_mime(mime: &Mime, file_name: impl AsRef<Utf8Path>) -> Utf8PathBuf {
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

    // 在调试模式下（包括测试和 examples）使用当前工作目录，否则使用用户目录
    let base_path: Utf8PathBuf = if cfg!(debug_assertions) {
        std::env::current_dir()
            .expect("Failed to get current directory")
            .try_into()
            .expect("Failed to convert current dir to Utf8PathBuf")
    } else {
        base_dir.to_path_buf().try_into().expect("Failed to convert base_dir to PathBuf")
    };
    // 在调试模式下使用测试专用的文件夹名称
    let folder_name = if cfg!(debug_assertions) {
        FOLDER_NAME_TEST
    } else {
        FOLDER_NAME
    };
    base_path.join(folder_name).join(file_name)
}

pub async fn assign_path(file_name: impl AsRef<Utf8Path>, mime: &Mime) -> io::Result<Utf8PathBuf> {
    let path = recommand_path_for_mime(mime, file_name);
    // 确保父目录存在
    if let Some(parent) = path.parent() {
        compio::fs::create_dir_all(parent).await?;
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_redirection() {
        // 获取当前工作目录
        let current_dir: Utf8PathBuf = std::env::current_dir()
            .expect("Failed to get current directory")
            .try_into()
            .expect("Failed to convert to Utf8PathBuf");

        // 测试不同 MIME 类型的文件路径
        let test_cases: Vec<(&str, Mime, &str)> = vec![
            ("test.txt", mime::TEXT_PLAIN, "Documents"),
            ("test.pdf", "application/pdf".parse::<Mime>().unwrap(), "Documents"),
            ("test.jpg", mime::IMAGE_JPEG, "Pictures"),
            ("test.mp3", "audio/mpeg".parse::<Mime>().unwrap(), "Music"),
            ("test.mp4", "video/mp4".parse::<Mime>().unwrap(), "Videos"),
            ("test.json", "application/json".parse::<Mime>().unwrap(), "Documents"),
        ];

        for (file_name, mime_type, _expected_folder) in test_cases {
            let path = recommand_path_for_mime(&mime_type, file_name);

            // 验证路径以当前工作目录开头
            assert!(path.starts_with(&current_dir), "路径 '{}' 应该以当前工作目录 '{}' 开头", path, current_dir);

            // 验证路径包含 FalconTransferTests 文件夹
            assert!(
                path.to_string().contains("FalconTransferTests"),
                "路径 '{}' 应该包含 'FalconTransferTests' 文件夹",
                path
            );

            // 验证路径以文件名结尾
            assert!(path.ends_with(file_name), "路径 '{}' 应该以 '{}' 结尾", path, file_name);

            // 验证路径格式正确：当前目录/FalconTransferTests/文件名
            let expected_path = current_dir.join("FalconTransferTests").join(file_name);
            assert_eq!(path, expected_path, "路径 '{}' 应该等于 '{}'", path, expected_path);
        }
    }
}
