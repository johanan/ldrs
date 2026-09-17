use std::sync::Arc;

use anyhow::Context;
use object_store::{path::Path, ObjectStore, ObjectStoreScheme};
use url::{ParseError, Url};

pub fn base_or_relative_path(path: &str) -> Result<Url, anyhow::Error> {
    let try_parse = Url::parse(path);
    match try_parse {
        Ok(url)
            if url.scheme() == "file"
                && path
                    .strip_prefix("file://")
                    .is_some_and(|rest| !rest.starts_with('/')) =>
        {
            // A `file://` whose remainder has no leading slash (including a bare `file://`)
            // resolves relative to the working directory; `file:///abs` keeps its leading
            // slash and stays absolute via the `Ok(url)` arm below.
            // DEPRECATED: a future release removes this arm — `file://` becomes absolute-only;
            // use a plain path (the `RelativeUrlWithoutBase` arm) instead.
            let relative_path = &path[7..]; // Skip "file://"
            let suggestion = if relative_path.is_empty() {
                "."
            } else {
                relative_path
            };
            tracing::warn!(
                "`file://` with a relative path ({path}) is deprecated and will stop \
                 resolving to the working directory in a future release; use a plain path \
                 instead: `{suggestion}`"
            );
            let current_dir =
                std::env::current_dir().with_context(|| "Could not get current dir")?;
            let base = Url::parse(&format!("file://{}/", current_dir.display()))
                .with_context(|| "Could not parse base path file URL")?;
            Url::options()
                .base_url(Some(&base))
                .parse(relative_path)
                .with_context(|| "Could not parse relative path file URL")
        }
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let current_dir =
                std::env::current_dir().with_context(|| "Could not get current dir")?;
            let base = Url::parse(&format!("file://{}/", current_dir.display()))
                .with_context(|| "Could not parse base path file URL")?;
            Url::options()
                .base_url(Some(&base))
                .parse(path)
                .with_context(|| "Could not parse relative path file URL")
        }
        _ => Err(anyhow::Error::msg("Could not parse path URL")),
    }
}

/// Resolve `base` (via [`base_or_relative_path`]) and append `relative` onto its path with
/// exactly one separator: `base` + `/one` == `base/` + `one` == `base/one`. `..` and internal
/// `//` in `relative` follow [`Url::join`] resolution and are not guaranteed.
pub fn join_into_url(base: &str, relative: &str) -> Result<Url, anyhow::Error> {
    let mut url = base_or_relative_path(base)?;
    // Ensure the base path ends in `/` so the join appends to it.
    if !url.path().ends_with('/') {
        let dir_path = format!("{}/", url.path());
        url.set_path(&dir_path);
    }
    // Strip a leading slash so `relative` joins as relative, not absolute.
    url.join(relative.trim_start_matches('/'))
        .with_context(|| "Could not join relative path onto base URL")
}

/// Convert a relative path encoded as a URI (RFC 2396) into the object store's spelling of it
pub fn store_path_from_uri(path: &str) -> Result<Option<Path>, anyhow::Error> {
    match Url::parse(path) {
        Ok(_) => Ok(None),
        Err(_) => Ok(Some(Path::from_url_path(path)?)),
    }
}

pub fn join_store_path(base: &Path, relative: &Path) -> Path {
    relative
        .parts()
        .fold(base.clone(), |path, part| path.join(part))
}

pub fn build_store(
    url: &Url,
) -> Result<(Arc<dyn ObjectStore>, Path, ObjectStoreScheme), anyhow::Error> {
    let (scheme, path) = ObjectStoreScheme::parse(url).with_context(|| "Not an ObjectStore URL")?;
    // ensure that cloud providers use the env to utilize their credential providers instead of relying on credentials in the URL
    let store: Arc<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::AmazonS3 => Arc::new(
            object_store::aws::AmazonS3Builder::from_env()
                .with_url(url.to_string())
                .build()
                .context("Could not build S3 store")?,
        ),
        ObjectStoreScheme::MicrosoftAzure => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_url(url.to_string())
                .build()
                .context("Could not build Azure store")?,
        ),
        ObjectStoreScheme::GoogleCloudStorage => Arc::new(
            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_url(url.to_string())
                .build()
                .context("Could not build GCP store")?,
        ),
        _ => {
            let (boxed, _) = object_store::parse_url(url).context("Could not build store")?;
            Arc::from(boxed)
        }
    };
    Ok((store, path, scheme))
}

/// The container or bucket a URL denotes, however that URL spells it. Names are lowercased.
#[derive(Debug, PartialEq, Eq)]
pub enum StoreBase {
    Azure { account: String, container: String },
}

pub struct StoreLocation {
    pub base: StoreBase,
    /// Trailing slash when non-empty, empty at the container root.
    pub path: String,
}

pub fn store_location(url: &Url) -> Result<StoreLocation, anyhow::Error> {
    match ObjectStoreScheme::parse(url) {
        Ok((ObjectStoreScheme::MicrosoftAzure, _)) => azure_location(url),
        _ => Err(anyhow::anyhow!("'{url}' is not an azure url")),
    }
}

/// Every Azure spelling: `abfs[s]`, `az`, `adl`, `azure`, and the `https` endpoint forms.
fn azure_location(url: &Url) -> Result<StoreLocation, anyhow::Error> {
    use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};

    let host = url.host_str().context("azure url has no host")?;

    let (account, container, path) = match (url.username(), host.split_once('.')) {
        // abfss://container@account.host/path
        (container, Some((account, _))) if !container.is_empty() => {
            Ok((account.to_string(), container.to_string(), url.path()))
        }
        // https://account.host/container/path, and Snowflake's azure://account.host/container/path
        ("", Some((account, _))) => match url.path().trim_start_matches('/').split_once('/') {
            Some((container, path)) => Ok((account.to_string(), container.to_string(), path)),
            None => Ok((
                account.to_string(),
                url.path().trim_matches('/').to_string(),
                "",
            )),
        },
        // az://container/path
        ("", None) => MicrosoftAzureBuilder::from_env()
            .get_config_value(&AzureConfigKey::AccountName)
            .context("url carries no storage account and AZURE_STORAGE_ACCOUNT_NAME is unset")
            .map(|account| (account, host.to_string(), url.path())),
        _ => Err(anyhow::anyhow!("unrecognised azure url '{url}'")),
    }?;

    let path = path.trim_matches('/');
    Ok(StoreLocation {
        base: StoreBase::Azure {
            account: account.to_lowercase(),
            container: container.to_lowercase(),
        },
        path: match path.is_empty() {
            true => String::new(),
            false => format!("{path}/"),
        },
    })
}

/// Workaround for delta-io/delta-kernel-rs#2209.
pub fn kernel_url(url: &Url) -> Result<Url, anyhow::Error> {
    let (scheme, base) = ObjectStoreScheme::parse(url).context("Not an ObjectStore URL")?;
    if Path::from_url_path(url.path())? == base {
        return Ok(url.clone());
    }
    let native = match scheme {
        ObjectStoreScheme::MicrosoftAzure => "az",
        ObjectStoreScheme::AmazonS3 => "s3",
        ObjectStoreScheme::GoogleCloudStorage => "gs",
        _ => return Ok(url.clone()),
    };
    // `parse` strips the leading segment; it is the container or bucket by construction.
    let container = url
        .path()
        .trim_start_matches('/')
        .split('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .context("object store URL has no container or bucket segment")?;
    Url::parse(&format!("{native}://{container}/{}", base.as_ref()))
        .context("Could not rebuild the table URL for delta-kernel")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_path(uri: &str) -> Option<String> {
        store_path_from_uri(uri).unwrap().map(String::from)
    }

    fn location(uri: &str) -> StoreLocation {
        store_location(&Url::parse(uri).unwrap()).unwrap()
    }

    fn azure(account: &str, container: &str) -> StoreBase {
        StoreBase::Azure {
            account: account.to_string(),
            container: container.to_string(),
        }
    }

    #[test]
    fn azure_location_agrees_across_spellings() {
        for uri in [
            "abfss://cont@acct.dfs.core.windows.net/a/b",
            "abfs://cont@acct.blob.core.windows.net/a/b",
            "https://acct.dfs.core.windows.net/cont/a/b",
            "https://acct.blob.core.windows.net/cont/a/b",
            // the spelling Snowflake reports for an external volume
            "azure://acct.blob.core.windows.net/cont/a/b",
            // lowercased
            "https://ACCT.blob.core.windows.net/CONT/a/b",
        ] {
            let got = location(uri);
            assert_eq!(got.base, azure("acct", "cont"), "disagreed for {uri}");
            assert_eq!(got.path, "a/b/", "disagreed for {uri}");
        }
    }

    #[test]
    fn azure_location_at_the_container_root_has_no_path() {
        for uri in [
            "azure://acct.blob.core.windows.net/cont/",
            "azure://acct.blob.core.windows.net/cont",
        ] {
            let got = location(uri);
            assert_eq!(got.base, azure("acct", "cont"));
            assert_eq!(got.path, "", "disagreed for {uri}");
        }
    }

    #[test]
    fn azure_location_path_subtraction_yields_the_base_location() {
        let volume = location("azure://acct.blob.core.windows.net/cont/");
        let table = location("https://acct.blob.core.windows.net/cont/one/two/tbl");
        assert_eq!(table.path.strip_prefix(&volume.path), Some("one/two/tbl/"));

        let scoped = location("azure://acct.blob.core.windows.net/cont/one/");
        assert_eq!(table.path.strip_prefix(&scoped.path), Some("two/tbl/"));

        // a partial segment is not a prefix
        let sibling = location("azure://acct.blob.core.windows.net/cont/on/");
        assert_eq!(table.path.strip_prefix(&sibling.path), None);
    }

    #[test]
    fn store_location_rejects_other_stores() {
        for uri in [
            "s3://bucket/a/b",
            "gs://bucket/a/b",
            "https://example.com/a/b",
            "file:///tmp/a/b",
        ] {
            assert!(
                store_location(&Url::parse(uri).unwrap()).is_err(),
                "accepted {uri}"
            );
        }
    }

    #[test]
    fn kernel_url_path_matches_the_store_root() {
        for uri in [
            "https://acct.blob.core.windows.net/cont/a/b",
            "https://acct.dfs.core.windows.net/cont/a/b",
            "https://s3.us-east-1.amazonaws.com/bucket/a/b",
            "https://bucket.s3.amazonaws.com/a/b",
            "az://cont/a/b",
            "abfss://cont@acct.dfs.core.windows.net/a/b",
            "s3://bucket/a/b",
            "gs://bucket/a/b",
        ] {
            let url = Url::parse(uri).unwrap();
            let (_, base) = ObjectStoreScheme::parse(&url).unwrap();
            let rebuilt = kernel_url(&url).unwrap();
            assert_eq!(
                Path::from_url_path(rebuilt.path()).unwrap(),
                base,
                "kernel path disagrees with the store root for {uri}"
            );
        }
    }

    #[test]
    fn kernel_url_rewrites_only_the_https_forms() {
        let unchanged = "az://cont/a/b";
        let url = Url::parse(unchanged).unwrap();
        assert_eq!(kernel_url(&url).unwrap().as_str(), unchanged);

        let url = Url::parse("https://acct.blob.core.windows.net/cont/a/b").unwrap();
        assert_eq!(kernel_url(&url).unwrap().as_str(), "az://cont/a/b");
    }

    #[test]
    fn kernel_url_handles_a_table_at_the_container_root() {
        let url = Url::parse("https://acct.blob.core.windows.net/cont").unwrap();
        let rebuilt = kernel_url(&url).unwrap();
        assert_eq!(rebuilt.as_str(), "az://cont/");
        assert_eq!(
            Path::from_url_path(rebuilt.path()).unwrap(),
            Path::from_url_path("").unwrap()
        );
    }

    #[test]
    fn store_path_from_uri_decodes_uri_encoding() {
        // a partition value of `100%` is `dt=100%25` as an object key and `dt=100%2525` as a URI
        assert_eq!(
            store_path("dt=100%2525/part-0.parquet").as_deref(),
            Some("dt=100%25/part-0.parquet")
        );
        assert_eq!(
            store_path("dt=2026-01-01%2000%3A00%3A00/p.parquet").as_deref(),
            Some("dt=2026-01-01 00:00:00/p.parquet")
        );
    }

    #[test]
    fn store_path_from_uri_has_no_answer_for_an_absolute_uri() {
        assert_eq!(store_path("s3://other-bucket/t/a.parquet"), None);
    }

    #[test]
    fn join_store_path_keeps_the_relative_path_nested() {
        let base = Path::from("tables/users");
        let relative = store_path_from_uri("dt=100%2525/part-0.parquet")
            .unwrap()
            .unwrap();
        // Handing the relative path to `Path::join` as a string would encode both the separator and
        // the percent, giving one segment named `dt=100%2525%2Fpart-0.parquet`.
        assert_eq!(
            String::from(join_store_path(&base, &relative)),
            "tables/users/dt=100%25/part-0.parquet"
        );
        assert_eq!(
            String::from(base.clone().join("dt=100%25/part-0.parquet")),
            "tables/users/dt=100%2525%2Fpart-0.parquet",
            "the broken form this exists to replace"
        );
    }

    #[test]
    fn join_store_path_handles_a_file_at_the_table_root() {
        let base = Path::from("tables/users");
        let relative = store_path_from_uri("part-0.parquet").unwrap().unwrap();
        assert_eq!(
            String::from(join_store_path(&base, &relative)),
            "tables/users/part-0.parquet"
        );
    }

    #[test]
    fn test_base_or_relative_path() {
        let cd = std::env::current_dir().unwrap();
        let base_path = format!("file://{}/home/data", cd.display());
        let no_file_base = format!("{}/home/data", cd.display());
        let cwd_root = format!("file://{}/", cd.display());
        let cwd_nested = format!("file://{}/data/dir", cd.display());
        let cwd_rel = format!("file://{}/rel/path", cd.display());
        let tests = [
            ("file:///home/data", "file:///home/data", true),
            ("home/data", base_path.as_str(), true),
            (no_file_base.as_str(), base_path.as_str(), true),
            ("file://home/data", base_path.as_str(), true),
            // bare `file://` (no path) is the working directory, like a bare shell
            // path; v0.19.0 resolved it to filesystem root instead (the regression).
            ("file://", cwd_root.as_str(), true),
            // a relative remainder (no leading slash) resolves under the cwd
            ("file://data/dir", cwd_nested.as_str(), true),
            // a leading slash after `file://` is absolute stays at root, not cwd
            ("file:///data/dir", "file:///data/dir", true),
            ("file:///", "file:///", true),
            // plain filesystem strings (no scheme)
            ("", cwd_root.as_str(), true),
            (".", cwd_root.as_str(), true),
            ("rel/path", cwd_rel.as_str(), true),
            ("/abs", "file:///abs", true),
            // cloud + absolute schemes pass through untouched (no working-dir rewrite)
            ("az://account/container", "az://account/container", true),
            ("s3://bucket/prefix", "s3://bucket/prefix", true),
        ];
        for (path, expected, is_ok) in tests.iter() {
            let result = base_or_relative_path(path);
            assert_eq!(result.is_ok(), *is_ok);
            if *is_ok {
                assert_eq!(result.unwrap().as_str(), *expected);
            }
        }
    }

    #[test]
    fn test_join_into_url() {
        // The contract: base + relative join with exactly one separator; a leading slash
        // on the relative and a missing/extra trailing slash on the base are all forgiven.
        let cases = [
            ("az://base", "one", "az://base/one"),
            ("az://base/", "one", "az://base/one"),
            ("az://base", "/one", "az://base/one"),
            ("az://base/", "/one", "az://base/one"),
            ("az://base/one", "two", "az://base/one/two"),
            (
                "az://account/container",
                "dir/file.parquet",
                "az://account/container/dir/file.parquet",
            ),
            (
                "s3://bucket/prefix/",
                "dir/file.parquet",
                "s3://bucket/prefix/dir/file.parquet",
            ),
            // an absolute file base stays absolute
            (
                "file:///abs/base",
                "file.parquet",
                "file:///abs/base/file.parquet",
            ),
        ];
        for (base, relative, expected) in cases {
            assert_eq!(
                join_into_url(base, relative).unwrap().as_str(),
                expected,
                "join {base} + {relative}"
            );
        }

        // local: a relative/bare `file://` base is lifted to the working directory first
        let cd = std::env::current_dir().unwrap();
        assert_eq!(
            join_into_url("file://", "dir/file.parquet")
                .unwrap()
                .as_str(),
            format!("file://{}/dir/file.parquet", cd.display())
        );
        assert_eq!(
            join_into_url("data/dir", "file.parquet").unwrap().as_str(),
            format!("file://{}/data/dir/file.parquet", cd.display())
        );
    }
}
