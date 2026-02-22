use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct FileSource {
    pub name: String,
    pub filename: Option<String>,
}

pub fn get_full_path(source: &FileSource) -> String {
    match &source.filename {
        Some(filename) => filename.clone(),
        None => source.name.clone(),
    }
}

pub fn is_object_store_url(url: &Url) -> bool {
    matches!(
        url.scheme(),
        "file" | "az" | "adl" | "azure" | "abfs" | "abfss" | "https"
    )
}
