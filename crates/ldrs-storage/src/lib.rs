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

pub fn build_store(
    url: &Url,
) -> Result<(Arc<dyn ObjectStore>, Path, ObjectStoreScheme), anyhow::Error> {
    let (scheme, path) = ObjectStoreScheme::parse(url).with_context(|| "Not an ObjectStore URL")?;
    // ensure that cloud providers use the env to utilize their credential providers instead of relying on credentials in the URL
    let store: Arc<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::MicrosoftAzure => Arc::new(
            object_store::azure::MicrosoftAzureBuilder::from_env()
                .with_url(url.to_string())
                .build()
                .context("Could not build Azure store")?,
        ),
        _ => {
            let (boxed, _) = object_store::parse_url(url).context("Could not build store")?;
            Arc::from(boxed)
        }
    };
    Ok((store, path, scheme))
}

#[cfg(test)]
mod tests {
    use super::*;

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
