use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use object_store::azure::MicrosoftAzureBuilder;
use url::Url;

pub struct AzureUrl {
    storage_account: String,
    container: String,
    path: String,
    url: Url,
}

impl TryFrom<Url> for AzureUrl {
    type Error = anyhow::Error;

    fn try_from(url: Url) -> Result<Self, anyhow::Error> {
        let host = url
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("No host found"))?;
        let path = match url.path() {
            "/" => Err(anyhow::anyhow!("No path found")),
            p => Ok(p),
        }?;

        let clean_path = path.strip_prefix('/').unwrap_or(path);
        let (container, blob_path) = match clean_path.split_once('/') {
            // If we have a slash, split at the first one
            Some((container, rest)) => (container.to_string(), rest.to_string()),
            // If no slash, the whole path is the container and there's no blob path
            None => (clean_path.to_string(), String::new()),
        };

        match url.scheme() {
            "https" | "azure" | "az" => match host.split_once('.') {
                Some((account, _)) => Ok(AzureUrl {
                    storage_account: account.to_string(),
                    container,
                    path: blob_path,
                    url,
                }),
                None => Ok(AzureUrl {
                    storage_account: host.to_string(),
                    container,
                    path: blob_path,
                    url,
                }),
            },
            _ => Err(anyhow::anyhow!("Invalid scheme")),
        }
    }
}

impl<'a> AzureUrl {
    pub fn try_from_url<T: Into<Url>>(url: T) -> Result<Self, anyhow::Error> {
        Self::try_from(url.into())
    }

    pub fn get_store_and_path(
        &self,
    ) -> Result<(Arc<dyn object_store::ObjectStore>, object_store::path::Path), anyhow::Error> {
        let store = MicrosoftAzureBuilder::from_env()
            .with_url(self.url.clone())
            .build()
            .with_context(|| "Could not build Azure store")?;
        let path = object_store::path::Path::from_url_path(self.path.clone())
            .with_context(|| "Could not build path")?;
        Ok((Arc::new(store), path))
    }

    pub fn get_delta_uri_options(&self) -> (String, HashMap<String, String>) {
        (format!("azure://{}/{}", self.container, self.path), HashMap::from([
            ("azure_storage_account_name".to_string(), self.storage_account.clone())
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_azure_url() {
        let test_cases = [
            (
                "https://myaccount.blob.core.windows.net/mycontainer/myblob",
                "myaccount",
                "mycontainer",
                "myblob",
            ),
            (
                "https://myaccount.blob.core.windows.net/mycontainer/ensure/this/works",
                "myaccount",
                "mycontainer",
                "ensure/this/works",
            ),
            (
                "https://myaccount.blob.core.windows.net/mycontainer/",
                "myaccount",
                "mycontainer",
                "",
            ),
        ];
        for (url, account, container, path) in test_cases.iter() {
            let url = Url::parse(url).expect("Could not parse URL");
            let azure_url = AzureUrl::try_from_url(url).expect("Could not parse Azure URL");
            assert_eq!(azure_url.storage_account, *account);
            assert_eq!(azure_url.container, *container);
            assert_eq!(azure_url.path, *path);
        }
    }
}
