use camino::{Utf8Path, Utf8PathBuf};
use fastdate::DateTime;
use sanitize_filename_reader_friendly::sanitize;
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt, ops::Deref};

/// Wrapper around `Utf8PathBuf` guaranteeing filesystem-safe filename.
///
/// # Safety Guarantees
///
/// The inner filename is sanitized using `sanitize-filename-reader-friendly` crate:
/// - Control characters (0x00-0x1f) replaced
/// - Reserved chars removed: `<`, `>`, `:`, `"`, `/`, `\`, `|`, `?`, `*`
/// - Leading/trailing spaces and dots trimmed
/// - Windows reserved names blocked (CON, PRN, AUX, NUL, COM1-9, LPT1-9)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[must_use]
pub struct SafeFileName(Utf8PathBuf);

impl SafeFileName {
    /// Creates `SafeFileName` without sanitization.
    ///
    /// # Safety
    ///
    /// Caller must ensure `path` contains only filesystem-safe characters.
    /// Use only when input is pre-sanitized or from trusted sources.
    #[inline]
    pub const unsafe fn new_unchecked(path: Utf8PathBuf) -> Self {
        // Safety: Caller guarantees path is already sanitized
        Self(path)
    }
}

impl Deref for SafeFileName {
    type Target = Utf8Path;

    #[inline]
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl From<&str> for SafeFileName {
    /// Creates `SafeFileName` by sanitizing input string.
    ///
    /// Dangerous characters are automatically replaced. Recommended for untrusted input.
    #[inline]
    fn from(s: &str) -> Self { Self(sanitize(s).into()) }
}

impl Borrow<Utf8Path> for SafeFileName {
    #[inline]
    fn borrow(&self) -> &Utf8Path { &self.0 }
}

impl fmt::Display for SafeFileName {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

/// Generates timestamp-based filename with optional suffix.
///
/// Format: `{timestamp}.{suffix}` where suffix defaults to "bin".
/// Leading dots in suffix are trimmed automatically.
#[inline]
pub fn timebased_filename(suffix: Option<&str>) -> String {
    format!(
        "{now}.{suffix}",
        now = DateTime::now(),
        suffix = suffix.map(|s| s.trim_start_matches('.')).unwrap_or("bin")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_preserves_safe_chars() {
        // Test safe characters are preserved
        let filename = SafeFileName::from("file-123_ABC.txt");
        assert_eq!(filename.as_str(), "file-123_ABC.txt");
    }

    #[test]
    fn test_from_str_trims_dots_and_spaces() {
        // Test leading/trailing dots and spaces are trimmed
        let filename = SafeFileName::from("  .test.  ");
        assert_eq!(filename.as_str(), "test");
    }

    #[test]
    fn test_from_str_empty() {
        // Test empty string
        let filename = SafeFileName::from("");
        assert_eq!(filename.as_str(), "");
    }

    #[test]
    fn test_deref_to_utf8path() {
        // Test Deref implementation works
        let filename = SafeFileName::from("test.txt");

        // Verify the sanitized filename is correct
        assert_eq!(
            filename.as_str(),
            "test.txt",
            "Sanitized filename should be 'test.txt', got: {}",
            filename.as_str()
        );

        // Deref gives us Utf8Path methods
        // Note: Utf8Path::ends_with checks path components, not string suffix
        // So "test.txt".ends_with(".txt") is false (different components)
        // But we can check extension or use as_str() for string operations
        assert_eq!(filename.extension(), Some("txt"), "File extension should be 'txt'");

        // Verify we can use Utf8Path methods via Deref
        assert!(filename.file_name().is_some());
        assert_eq!(filename.file_name().unwrap(), "test.txt");
    }

    #[test]
    fn test_display() {
        // Test Display implementation
        let filename = SafeFileName::from("test.txt");
        assert_eq!(format!("{}", filename), "test.txt");
        assert_eq!(filename.to_string(), "test.txt");
    }

    #[test]
    fn test_timebased_filename_with_suffix() {
        // Test with custom suffix
        let filename = timebased_filename(Some("txt"));
        assert!(filename.ends_with(".txt"));
        assert!(!filename.contains(".."));
    }

    #[test]
    fn test_timebased_filename_without_suffix() {
        // Test with default suffix
        let filename = timebased_filename(None);
        assert!(filename.ends_with(".bin"));
    }

    #[test]
    fn test_timebased_filename_trims_leading_dot() {
        // Test that leading dots are trimmed
        let filename = timebased_filename(Some(".log"));
        assert!(filename.ends_with(".log"));
        assert!(!filename.contains(".log.log"));
    }

    #[test]
    fn test_safe_filename_equality() {
        // Test PartialEq implementation
        let filename1 = SafeFileName::from("test.txt");
        let filename2 = SafeFileName::from("test.txt");
        let filename3 = SafeFileName::from("other.txt");

        assert_eq!(filename1, filename2);
        assert_ne!(filename1, filename3);
    }

    #[test]
    fn test_safe_filename_hash() {
        // Test Hash implementation (useful for HashMap/HashSet)
        use std::collections::HashSet;

        let filename1 = SafeFileName::from("test.txt");
        let filename2 = SafeFileName::from("test.txt");

        let mut set = HashSet::new();
        set.insert(filename1);
        set.insert(filename2);

        assert_eq!(set.len(), 1); // Should only have one entry
    }

    #[test]
    fn test_borrow_utf8path() {
        // Test Borrow<Utf8Path> implementation
        let filename = SafeFileName::from("test.txt");

        fn takes_path(_: &Utf8Path) -> bool { true }

        assert!(takes_path(&filename)); // Should borrow to &Utf8Path
    }

    #[test]
    fn test_sanitization_removes_dangerous_chars() {
        // Test that dangerous characters are handled
        let filename = SafeFileName::from("file:test.txt");
        // Colon should be removed or replaced (not in output)
        assert_ne!(filename.as_str(), "file:test.txt");
        assert!(filename.as_str().contains("file"));
        assert!(filename.as_str().contains("test"));
        assert!(filename.as_str().contains("txt"));
    }

    #[test]
    fn test_sanitization_preserves_extension() {
        // Test that file extensions are preserved
        let cases = ["file.txt", "document.pdf", "archive.tar.gz"];

        for input in cases {
            let filename = SafeFileName::from(input);
            assert!(
                filename.as_str().ends_with(".txt")
                    || filename.as_str().ends_with(".pdf")
                    || filename.as_str().ends_with(".tar.gz")
                    || filename.as_str().ends_with("gz"),
                "Extension should be preserved for: {}",
                input
            );
        }
    }

    #[test]
    fn test_multiple_dots_in_filename() {
        // Test filenames with multiple dots
        let filename = SafeFileName::from("my.file.name.txt");
        assert!(filename.as_str().contains("my"));
        assert!(filename.as_str().contains("file"));
        assert!(filename.as_str().contains("name"));
        assert!(filename.as_str().ends_with("txt"));
    }
}
