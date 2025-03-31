use clap::Args;
use serde::Deserialize;
use std::path::Path;

fn default_batch_size() -> usize {
    1024
}

#[derive(Args, Deserialize, Debug)]
pub struct LoadArgs {
    #[arg(short, long)]
    pub file: String,
    #[arg(short, long, default_value_t = 1024)]
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[arg(short, long)]
    pub table: String,
    #[arg(short, long)]
    pub post_sql: Option<String>,
    #[arg(short, long)]
    pub role: Option<String>,
}

#[derive(Args)]
pub struct PGFileLoadArgs {
    #[arg(short, long)]
    pub file_path: Option<String>,
    pub config_path: String,
    pub role: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PGFileLoad {
    pub file_path: Option<String>,
    pub batch_size: Option<usize>,
    pub tables: Vec<LoadArgs>,
    pub role: Option<String>,
}

#[derive(Debug)]
pub struct ProcessedPGFileLoad {
    pub tables: Vec<LoadArgs>,
}

impl TryFrom<PGFileLoad> for ProcessedPGFileLoad {
    type Error = anyhow::Error;

    fn try_from(pg_file_load: PGFileLoad) -> Result<Self, Self::Error> {
        let tables = pg_file_load
            .tables
            .into_iter()
            .map(|t| {
                let file_url = Path::new(&t.file);
                let full_path = pg_file_load.file_path.as_ref().map_or_else(
                    || file_url.to_path_buf(),
                    |p| {
                        let root_path = Path::new(p);
                        root_path.join(file_url)
                    },
                );
                // check if batch_size is equal to 1024 and if the outer batch_size is Some
                let batch_size = if t.batch_size == 1024 && pg_file_load.batch_size.is_some() {
                    pg_file_load.batch_size.unwrap()
                } else {
                    t.batch_size
                };
                Ok(LoadArgs {
                    file: full_path.to_string_lossy().into_owned(),
                    batch_size,
                    table: t.table,
                    post_sql: t.post_sql,
                    role: t.role.or(pg_file_load.role.clone()),
                })
            })
            .collect::<Result<Vec<LoadArgs>, anyhow::Error>>()?;
        Ok(ProcessedPGFileLoad { tables })
    }
}

impl PGFileLoad {
    /// Merge the cli args into self, returning a new PGFileLoad
    /// where a set file_path will override the file_path in self
    pub fn merge_cli_args(self, cli: PGFileLoadArgs) -> PGFileLoad {
        PGFileLoad {
            file_path: cli.file_path.or(self.file_path),
            batch_size: None,
            tables: self.tables,
            role: cli.role.or(self.role),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_path_join() {
        let tests = [
            ("home/data", "test.parquet", "home/data/test.parquet"),
            (
                "file:///home/data/",
                "test.parquet",
                "file:///home/data/test.parquet",
            ),
            (
                "https://example.com/data/",
                "test.parquet",
                "https://example.com/data/test.parquet",
            ),
        ];
        for (path, file, expected) in tests.iter() {
            let relative_path = Path::new(path);
            let file_path = Path::new(file);
            let full_path = relative_path.join(file_path);
            assert_eq!(full_path, Path::new(expected));
        }
    }

    #[test]
    fn test_pg_file_load() {
        let pg_file_load = r#"
file_path: /home/data
batch_size: 1024
tables:
    - file: test.parquet
      table: test_table
        "#;
        let pg_file_load: PGFileLoad = serde_yaml::from_str(pg_file_load).unwrap();
        assert_eq!(pg_file_load.file_path, Some(String::from("/home/data")));
        assert_eq!(pg_file_load.batch_size, Some(1024));
        assert_eq!(pg_file_load.tables.len(), 1);
        assert_eq!(pg_file_load.tables[0].file, "test.parquet");
        assert_eq!(pg_file_load.tables[0].batch_size, 1024);
        assert_eq!(pg_file_load.tables[0].table, "test_table");
        assert_eq!(pg_file_load.tables[0].post_sql, None);
    }

    #[test]
    fn test_pg_file_load_yaml_to_args() {
        let pg_file_load = r#"file_path: /home/data
batch_size: 1024
tables:
    - file: test.parquet
      table: test_table
      post_sql: null
        "#;
        let pg_file_load: PGFileLoad = serde_yaml::from_str(pg_file_load).unwrap();
        let args: ProcessedPGFileLoad = pg_file_load.try_into().unwrap();
        assert_eq!(args.tables.len(), 1);
        assert_eq!(args.tables[0].file, "/home/data/test.parquet");
        assert_eq!(args.tables[0].batch_size, 1024);
        assert_eq!(args.tables[0].table, "test_table");
        assert_eq!(args.tables[0].post_sql, None);
    }

    #[test]
    fn test_pg_file_config_to_args() {
        let tests = [
            (
                Some(String::from("/home/data")),
                Some(2048),
                "test.parquet",
                "/home/data/test.parquet",
                2048,
            ),
            (None, Some(2048), "test.parquet", "test.parquet", 2048),
            (
                Some(String::from("relative/data")),
                None,
                "test.parquet",
                "relative/data/test.parquet",
                1024,
            ),
            (None, None, "test.parquet", "test.parquet", 1024),
        ];
        for (file_path, batch_size, file, expected_file, expected_batch_size) in tests.iter() {
            let pg_file_load = PGFileLoad {
                file_path: file_path.clone(),
                batch_size: batch_size.clone(),
                role: None,
                tables: vec![LoadArgs {
                    file: String::from(*file),
                    batch_size: 1024,
                    table: String::from("test_table"),
                    post_sql: None,
                    role: None,
                }],
            };
            let args: ProcessedPGFileLoad = pg_file_load.try_into().unwrap();
            assert_eq!(args.tables[0].file, *expected_file);
            assert_eq!(args.tables[0].batch_size, *expected_batch_size);
        }
    }

    #[test]
    fn test_pg_file_load_merge_args() {
        let pg_file_load_yaml = r#"file_path: /home/data
batch_size: 1024
role: not_test_role
tables:
    - file: test.parquet
      table: test_table
        "#;
        let pg_file_load: PGFileLoad = serde_yaml::from_str(pg_file_load_yaml).unwrap();
        let cli_args = PGFileLoadArgs {
            file_path: Some(String::from("/cli/data")),
            config_path: String::from("config.yaml"),
            role: None,
        };
        let merged = pg_file_load.merge_cli_args(cli_args);
        let merged_file_path = merged.file_path.clone();
        let tasks: ProcessedPGFileLoad = merged.try_into().unwrap();
        assert_eq!(merged_file_path, Some(String::from("/cli/data")));
        assert_eq!(tasks.tables.len(), 1);
        assert_eq!(tasks.tables[0].file, "/cli/data/test.parquet");
        assert_eq!(tasks.tables[0].role, Some(String::from("not_test_role")));

        let pg_file_load: PGFileLoad = serde_yaml::from_str(pg_file_load_yaml).unwrap();
        let cli_args = PGFileLoadArgs {
            file_path: None,
            config_path: String::from("config.yaml"),
            role: Some(String::from("test_role")),
        };
        let merged = pg_file_load.merge_cli_args(cli_args);
        let merged_file_path = merged.file_path.clone();
        let tasks: ProcessedPGFileLoad = merged.try_into().unwrap();
        assert_eq!(merged_file_path, Some(String::from("/home/data")));
        assert_eq!(tasks.tables.len(), 1);
        assert_eq!(tasks.tables[0].file, "/home/data/test.parquet");
        assert_eq!(tasks.tables[0].role, Some(String::from("test_role")));
    }
}
