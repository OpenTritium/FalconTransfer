use camino::{Utf8Path, Utf8PathBuf};
use sanitize_filename_reader_friendly::sanitize;
use std::{borrow::Borrow, ops::Deref};

#[derive(Debug, Clone)]
pub struct SafeFileName(Utf8PathBuf);

impl Deref for SafeFileName {
    type Target = Utf8Path;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl From<&str> for SafeFileName {
    fn from(s: &str) -> Self { SafeFileName(sanitize(s).into()) }
}

impl Borrow<Utf8Path> for SafeFileName {
    fn borrow(&self) -> &Utf8Path { &self.0 }
}
