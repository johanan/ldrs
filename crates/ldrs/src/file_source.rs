use serde::{Deserialize, Serialize};

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
