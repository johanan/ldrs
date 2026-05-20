use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(
    description = "Object-store source. Reads a file (typically Parquet) from LDRS_SRC + filename."
)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonschema_scratch_test() {
        let schema = schemars::schema_for!(FileSource);
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
    }
}
