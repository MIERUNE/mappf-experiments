//! Trusted mapping from management resource IDs to delivery object paths.

use std::collections::{HashMap, HashSet};

use anyhow::{Context as _, ensure};
use object_store::{ObjectStoreExt as _, parse_url_opts};
use serde::Deserialize;
use tokio::time::timeout;
use url::Url;

use crate::{
    mutation::{AccountId, LocalResourceId},
    style::StyleObjectPath,
};

const SCHEMA_VERSION: u32 = 1;
const MAX_CATALOG_BYTES: u64 = 4 * 1024 * 1024;
const MAX_STYLES: usize = 10_000;

/// Startup snapshot resolving authorized style IDs to trusted storage paths.
#[derive(Clone, Default)]
pub struct StyleCatalog {
    styles: HashMap<(AccountId, LocalResourceId), StyleObjectPath>,
}

impl StyleCatalog {
    /// Loads one complete catalog object from a trusted object-store URL.
    pub async fn from_url<I, K, V>(url: &Url, options: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        crate::store_policy::ensure_location_only(url, "style catalog URL")?;
        let (store, location) =
            parse_url_opts(url, options).context("configure style catalog store")?;
        let result = timeout(crate::store_policy::OPERATION_TIMEOUT, store.get(&location))
            .await
            .context("style catalog read timed out")?
            .context("read style catalog")?;
        ensure!(
            result.meta.size <= MAX_CATALOG_BYTES,
            "style catalog exceeds {MAX_CATALOG_BYTES} bytes"
        );
        let body = timeout(crate::store_policy::OPERATION_TIMEOUT, result.bytes())
            .await
            .context("style catalog body timed out")?
            .context("collect style catalog")?;
        Self::parse(&body)
    }

    /// Parses one complete startup snapshot.
    pub fn parse(body: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            body.len() as u64 <= MAX_CATALOG_BYTES,
            "style catalog exceeds {MAX_CATALOG_BYTES} bytes"
        );
        let wire: CatalogWire = serde_json::from_slice(body).context("decode style catalog")?;
        ensure!(
            wire.schema_version == SCHEMA_VERSION,
            "unsupported style catalog schema_version {}",
            wire.schema_version
        );
        ensure!(
            wire.styles.len() <= MAX_STYLES,
            "style catalog exceeds {MAX_STYLES} entries"
        );

        let mut styles = HashMap::with_capacity(wire.styles.len());
        let mut locations = HashSet::with_capacity(wire.styles.len());
        for entry in wire.styles {
            let account = AccountId::try_new(entry.account_id)?;
            let style = LocalResourceId::try_new(entry.style_id)?;
            let location = StyleObjectPath::try_new(&entry.object_path)?;
            ensure!(
                locations.insert(location.as_ref().to_string()),
                "style catalog maps more than one resource to {}",
                location.as_ref()
            );
            ensure!(
                styles.insert((account, style), location).is_none(),
                "style catalog contains a duplicate account/style entry"
            );
        }
        Ok(Self { styles })
    }

    pub fn resolve(
        &self,
        account: &AccountId,
        style: &LocalResourceId,
    ) -> Option<&StyleObjectPath> {
        self.styles.get(&(account.clone(), style.clone()))
    }

    pub fn len(&self) -> usize {
        self.styles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.styles.is_empty()
    }

    pub fn locations(&self) -> impl Iterator<Item = &StyleObjectPath> {
        self.styles.values()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogWire {
    schema_version: u32,
    styles: Vec<StyleEntryWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StyleEntryWire {
    account_id: String,
    style_id: String,
    object_path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_only_explicit_catalog_entries() {
        let catalog = StyleCatalog::parse(
            br#"{
                "schema_version": 1,
                "styles": [{
                    "account_id": "example",
                    "style_id": "basic",
                    "object_path": "styles/delivery/basic/style.json"
                }]
            }"#,
        )
        .unwrap();
        let account = AccountId::try_new("example").unwrap();
        let basic = LocalResourceId::try_new("basic").unwrap();
        let unknown = LocalResourceId::try_new("unknown").unwrap();

        assert_eq!(catalog.len(), 1);
        assert_eq!(
            catalog.resolve(&account, &basic).unwrap().as_ref(),
            "styles/delivery/basic/style.json"
        );
        assert!(catalog.resolve(&account, &unknown).is_none());
    }

    #[test]
    fn resolves_flat_style_object_layout() {
        let catalog = StyleCatalog::parse(
            br#"{
                "schema_version": 1,
                "styles": [{
                    "account_id": "example",
                    "style_id": "basic",
                    "object_path": "styles/delivery/basic.json"
                }]
            }"#,
        )
        .unwrap();

        let location = catalog
            .resolve(
                &AccountId::try_new("example").unwrap(),
                &LocalResourceId::try_new("basic").unwrap(),
            )
            .unwrap();
        assert_eq!(location.as_ref(), "styles/delivery/basic.json");
        assert_eq!(location.delivery_style_id(), "delivery/basic");
    }

    #[test]
    fn rejects_aliases_duplicates_and_untrusted_paths() {
        for body in [
            br#"{"schema_version":1,"styles":[{"account_id":"a","style_id":"one","object_path":"styles/shared/style.json"},{"account_id":"b","style_id":"two","object_path":"styles/shared/style.json"}]}"#.as_slice(),
            br#"{"schema_version":1,"styles":[{"account_id":"a","style_id":"one","object_path":"outside/style.json"}]}"#.as_slice(),
            br#"{"schema_version":2,"styles":[]}"#.as_slice(),
        ] {
            assert!(StyleCatalog::parse(body).is_err());
        }
    }
}
