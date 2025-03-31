mod azure;

use std::sync::Arc;

use anyhow::Context;
use azure::AzureUrl;
use object_store::{ObjectStore, ObjectStoreScheme};
use url::{ParseError, Url};

pub fn base_or_relative_path(path: &str) -> Result<Url, anyhow::Error> {
    let try_parse = Url::parse(path);
    match try_parse {
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

pub enum StorageProvider {
    Azure(azure::AzureUrl),
    Local(Url, object_store::path::Path),
}

impl StorageProvider {
    pub fn try_from_url<T: Into<Url>>(url: T) -> Result<StorageProvider, anyhow::Error> {
        let url = url.into();
        let (scheme, path) =
            ObjectStoreScheme::parse(&url).with_context(|| "Not an ObjectStore URL")?;

        match scheme {
            ObjectStoreScheme::MicrosoftAzure => {
                Ok(StorageProvider::Azure(AzureUrl::try_from_url(url)?))
            }
            ObjectStoreScheme::Local => Ok(StorageProvider::Local(url, path)),
            _ => Err(anyhow::Error::msg("Not an Azure URL")),
        }
    }

    pub fn try_from_string(path: &str) -> Result<StorageProvider, anyhow::Error> {
        let url = base_or_relative_path(path)?;
        Self::try_from_url(url)
    }

    pub fn get_store_and_path(
        &self,
    ) -> Result<(Arc<dyn ObjectStore>, object_store::path::Path), anyhow::Error> {
        match self {
            StorageProvider::Azure(azure) => azure.get_store_and_path(),
            StorageProvider::Local(_, path) => {
                let store = object_store::local::LocalFileSystem::new();
                Ok((Arc::new(store), path.clone()))
            }
        }
    }

    /// Returns the URI and options for the delta table
    /// Delta table creates its own object store so we need to give it different options
    /// and uri than if we were creating the store ourselves
    pub fn get_delta_uri_options(&self) -> (String, std::collections::HashMap<String, String>) {
        match self {
            StorageProvider::Azure(azure) => azure.get_delta_uri_options(),
            StorageProvider::Local(url, _) => {
                let options = std::collections::HashMap::new();
                (url.to_string(), options)
            }
        }
    }

    /// Returns the URL of the storage provider
    /// that was used to create the object store
    pub fn get_url(&self) -> Url {
        match self {
            StorageProvider::Azure(azure) => azure.get_url(),
            StorageProvider::Local(url, _) => url.clone(),
        }
    }
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
    fn test_parse_storage_url() {
        let local_cases = ["file:///home/data/file.parquet", "home/data/file.parquet"];
        for case in local_cases.iter() {
            let url = base_or_relative_path(case).unwrap();
            let result = StorageProvider::try_from_url(url);
            assert!(result.is_ok());
            match result.unwrap() {
                StorageProvider::Local(_, _) => (),
                _ => panic!("Expected Local storage provider"),
            }
        }

        let azure_cases = [
            "https://myaccount.blob.core.windows.net/mycontainer/myblob",
            "https://myaccount.blob.core.windows.net/mycontainer",
            "https://myaccount.blob.core.windows.net/mycontainer/",
        ];
        for case in azure_cases.iter() {
            let url = Url::parse(case).unwrap();
            let result = StorageProvider::try_from_url(url);
            assert!(result.is_ok());
            match result.unwrap() {
                StorageProvider::Azure(_) => (),
                _ => panic!("Expected Azure storage provider"),
            }
        }
    }
}
