use camino::{Utf8Path, Utf8PathBuf};
use fastdate::DateTime;
use sanitize_filename_reader_friendly::sanitize;
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt, ops::Deref};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafeFileName(Utf8PathBuf);

impl Deref for SafeFileName {
    type Target = Utf8Path;

    #[inline]
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl From<&str> for SafeFileName {
    #[inline]
    fn from(s: &str) -> Self { SafeFileName(sanitize(s).into()) }
}

impl Borrow<Utf8Path> for SafeFileName {
    #[inline]
    fn borrow(&self) -> &Utf8Path { &self.0 }
}

impl fmt::Display for SafeFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

/// 后缀不用带点
#[inline]
pub fn timebased_filename(suffix: Option<&str>) -> String {
    format!(
        "{now}.{suffix}",
        now = DateTime::now(),
        suffix = suffix.map(|s| s.trim_start_matches('.')).unwrap_or("bin")
    )
}
