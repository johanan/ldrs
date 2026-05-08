use std::path::{Path, PathBuf};

/// Root of the fixture data directory.
pub fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data")
}

/// Resolve a fixture path relative to the data directory.
pub fn fixture(relative: &str) -> PathBuf {
    data_dir().join(relative)
}

/// Resolve a fixture as a `file://` URL (for object_store / parquet readers).
pub fn fixture_url(relative: &str) -> String {
    format!("file://{}", fixture(relative).display())
}

pub fn fixture_str(relative: &str) -> String {
    fixture(relative).to_string_lossy().into_owned()
}

pub fn data_url() -> String {
    format!("file://{}/", data_dir().display())
}

pub fn data_str() -> String {
    data_dir().to_string_lossy().into_owned()
}
