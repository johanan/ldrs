use std::sync::Arc;

use anyhow::Context;
use object_store::{path::Path, ObjectStore, ObjectStoreScheme};
use url::{ParseError, Url};

pub fn ensure_trailing_slash(url: &str) -> String {
    if url.ends_with('/') {
        url.to_string()
    } else {
        format!("{}/", url)
    }
}

pub fn base_or_relative_path(path: &str) -> Result<Url, anyhow::Error> {
    let try_parse = Url::parse(path);
    match try_parse {
        Ok(url) if url.scheme() == "file" && url.host().is_some() => {
            // This is file://something (two slashes), treat as relative path
            // Extract the path part after file://
            let relative_path = &path[7..]; // Skip "file://"
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
        let tests = [
            ("file:///home/data", "file:///home/data", true),
            ("home/data", base_path.as_str(), true),
            (no_file_base.as_str(), base_path.as_str(), true),
            ("file://home/data", base_path.as_str(), true),
        ];
        for (path, expected, is_ok) in tests.iter() {
            let result = base_or_relative_path(path);
            assert_eq!(result.is_ok(), *is_ok);
            if *is_ok {
                assert_eq!(result.unwrap().as_str(), *expected);
            }
        }
    }
}
