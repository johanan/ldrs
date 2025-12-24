use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct FileSource {
    name: String,
    filename: Option<String>,
}
