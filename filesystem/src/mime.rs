use camino::{Utf8Path, Utf8PathBuf};
use compio::fs::create_dir_all;
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

use crate::safe_filename::timebased_filename;

pub static USER_DIR: LazyLock<UserDirs> = LazyLock::new(|| UserDirs::new().expect("get user dirs failed"));

const FOLDER_NAME: &str = "FalconTransfer";
const FOLDER_NAME_TEST: &str = "FalconTransferTests";

/// 根据 MIME 类型推荐保存路径
fn recommend_path_for_mime(mime: &Mime, file_name: impl AsRef<Utf8Path>) -> Utf8PathBuf {
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

/// Helper function to ensure parent directory exists
async fn ensure_parent_exists(path: &Utf8Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent).await?;
    }
    Ok(())
}

pub async fn assign_path(file_name: impl AsRef<Utf8Path>, mime: &Mime) -> io::Result<Utf8PathBuf> {
    let path = recommend_path_for_mime(mime, file_name);
    ensure_parent_exists(&path).await?;
    Ok(path)
}

/// Assigns a unique file path for download tasks, avoiding conflicts.
///
/// Only appends a numeric suffix (e.g., "_1", "_2") when the target path already exists.
///
/// # Examples
///
/// ```ignore
/// // If "ubuntu.iso" doesn't exist
/// assign_path_unique("ubuntu.iso", &mime).await?; // => ".../ubuntu.iso"
///
/// // If "ubuntu.iso" already exists
/// assign_path_unique("ubuntu.iso", &mime).await?; // => ".../ubuntu.1.iso"
///
/// // If both "ubuntu.iso" and "ubuntu.1.iso" exist
/// assign_path_unique("ubuntu.iso", &mime).await?; // => ".../ubuntu.2.iso"
/// ```
#[inline]
pub async fn assign_path_unique(file_name: impl AsRef<Utf8Path>, mime: &Mime) -> io::Result<Utf8PathBuf> {
    let base_path = recommend_path_for_mime(mime, file_name.as_ref());
    ensure_parent_exists(&base_path).await?;
    // Check if path already exists using Path::exists
    if base_path.as_path().try_exists()? {
        return find_unique_path(&base_path).await;
    }
    Ok(base_path)
}

/// Finds a unique path by appending numeric suffixes (1, 2, 3, ...).
///
/// # Returns
///
/// The first unique path found, or an error if 100 attempts fail.
async fn find_unique_path(base_path: &Utf8Path) -> io::Result<Utf8PathBuf> {
    let Some(stem) = base_path.file_stem() else {
        return Ok(Utf8PathBuf::from(timebased_filename(None)));
    };
    let ext = base_path.extension();
    const MAX_ATTEMPTS: usize = 128;
    for i in 1..=MAX_ATTEMPTS {
        let new_name = ext.map_or_else(|| format!("{stem}.{i}"), |ext| format!("{stem}.{i}.{ext}"));
        let parent = base_path.parent().unwrap_or_else(|| Utf8Path::new(""));
        let new_path = parent.join(&new_name);
        if !new_path.as_path().try_exists()? {
            return Ok(new_path);
        }
    }
    Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("Cannot find unique path after {MAX_ATTEMPTS} attempts")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

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
            let path = recommend_path_for_mime(&mime_type, file_name);

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

    #[compio::test]
    async fn test_assign_path_unique_no_conflict() {
        // Get current directory to understand where FalconTransferTests will be created
        let current_dir: Utf8PathBuf = std::env::current_dir()
            .expect("Failed to get current directory")
            .try_into()
            .expect("Failed to convert to Utf8PathBuf");
        let expected_path = current_dir.join("FalconTransferTests").join("test_file.txt");

        // Clean up any existing file
        let _ = std::fs::remove_file(&expected_path);

        // No conflict: should return original path
        let result = assign_path_unique("test_file.txt", &mime::TEXT_PLAIN).await.expect("Failed to assign path");
        assert_eq!(result, expected_path, "Should return original path when no conflict");
    }

    #[compio::test]
    async fn test_assign_path_unique_with_conflict() {
        let current_dir: Utf8PathBuf = std::env::current_dir()
            .expect("Failed to get current directory")
            .try_into()
            .expect("Failed to convert to Utf8PathBuf");
        let base_path = current_dir.join("FalconTransferTests");

        // Ensure parent directory exists
        std::fs::create_dir_all(&base_path).expect("Failed to create FalconTransferTests directory");

        let test_path = base_path.join("ubuntu.iso");

        // Create a file at the target location
        File::create(&test_path).expect("Failed to create test file");

        // Conflict exists: should return path with suffix
        let result =
            assign_path_unique("ubuntu.iso", &mime::APPLICATION_OCTET_STREAM).await.expect("Failed to assign path");

        assert_ne!(result, test_path, "Should NOT return original path when conflict exists");
        assert!(result.to_string().contains(".1."), "Should contain '.1.' suffix");
        assert!(result.to_string().ends_with(".iso"), "Should preserve file extension");

        // Clean up
        let _ = std::fs::remove_file(&test_path);
    }

    #[compio::test]
    async fn test_assign_path_unique_multiple_conflicts() {
        let current_dir: Utf8PathBuf = std::env::current_dir()
            .expect("Failed to get current directory")
            .try_into()
            .expect("Failed to convert to Utf8PathBuf");
        let base_path = current_dir.join("FalconTransferTests");

        // Ensure parent directory exists
        std::fs::create_dir_all(&base_path).expect("Failed to create FalconTransferTests directory");

        // Create multiple conflicting files
        File::create(base_path.join("test.txt")).expect("Failed to create test.txt");
        File::create(base_path.join("test.1.txt")).expect("Failed to create test.1.txt");
        File::create(base_path.join("test.2.txt")).expect("Failed to create test.2.txt");

        // Multiple conflicts: should skip to ".3"
        let result = assign_path_unique("test.txt", &mime::TEXT_PLAIN).await.expect("Failed to assign path");

        assert!(result.to_string().contains(".3."), "Should skip to '.3.' when .1, .2 exist");
        assert!(result.to_string().ends_with(".txt"), "Should preserve file extension");

        // Clean up
        let _ = std::fs::remove_file(base_path.join("test.txt"));
        let _ = std::fs::remove_file(base_path.join("test.1.txt"));
        let _ = std::fs::remove_file(base_path.join("test.2.txt"));
    }

    #[compio::test]
    async fn test_assign_path_unique_no_extension() {
        let current_dir: Utf8PathBuf = std::env::current_dir()
            .expect("Failed to get current directory")
            .try_into()
            .expect("Failed to convert to Utf8PathBuf");
        let base_path = current_dir.join("FalconTransferTests");

        // Ensure parent directory exists
        std::fs::create_dir_all(&base_path).expect("Failed to create FalconTransferTests directory");

        let test_path = base_path.join("README");

        // Create a file without extension
        File::create(&test_path).expect("Failed to create test file");

        // Conflict exists with no extension: should append suffix without extension separator
        let result = assign_path_unique("README", &mime::TEXT_PLAIN).await.expect("Failed to assign path");

        assert!(result.to_string().ends_with(".1"), "Should append '.1' for files without extension");
        assert!(!result.to_string().contains(".1."), "Should NOT add extra dot for files without extension");

        // Clean up
        let _ = std::fs::remove_file(&test_path);
    }
}
