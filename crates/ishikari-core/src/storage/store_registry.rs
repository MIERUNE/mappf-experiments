//! Shared registry of object stores.
//!
//! Object stores (and their reqwest connection pools and credential caches) are
//! built once per scheme + authority (bucket/host) and reused across every
//! tileset and provider read to that backend, instead of being rebuilt per
//! request. A single registry is shared by the tile storage layer and the
//! provider fetch layer, so e.g. tiles and styles in the same bucket share one
//! store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use object_store::{ObjectStore, parse_url_opts, path::Path as ObjectPath};
use url::Url;

/// Caches object stores keyed by scheme + authority.
#[derive(Default)]
pub struct ObjectStoreRegistry {
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ObjectStoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves a URL to a reused object store plus the object path within it.
    /// The store is built once per scheme + authority and cached; the path is
    /// derived from the URL so different prefixes on one bucket share a store.
    pub fn resolve(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let key = store_key(url);
        let store = {
            let mut stores = self.stores.lock().expect("object store registry poisoned");
            if let Some(store) = stores.get(&key) {
                store.clone()
            } else {
                // The HTTP backend refuses plain-text HTTP by default, but
                // `http://` is an accepted provider-template scheme (local and
                // dev upstreams). The URL scheme already states the intent, so
                // enable it here instead of requiring an ALLOW_HTTP env var.
                let allow_http = (url.scheme() == "http")
                    .then_some(("allow_http".to_string(), "true".to_string()));
                let options = std::env::vars().chain(allow_http);
                let (store, _path) = parse_url_opts(url, options)
                    .with_context(|| format!("failed to parse object store URL {url}"))?;
                let store: Arc<dyn ObjectStore> = store.into();
                stores.insert(key, store.clone());
                store
            }
        };
        let path = ObjectPath::from_url_path(url.path())
            .with_context(|| format!("invalid object path in URL {url}"))?;
        Ok((store, path))
    }
}

/// Identifies the object store backing a URL by scheme + authority
/// (bucket/host), independent of the object path.
fn store_key(url: &Url) -> String {
    format!("{}://{}", url.scheme(), url.authority())
}

#[cfg(test)]
mod tests {
    use super::store_key;
    use url::Url;

    #[test]
    fn store_key_is_path_independent() {
        let a = Url::parse("gs://bucket/styles/x/style.json").unwrap();
        let b = Url::parse("gs://bucket/japan.pmtiles").unwrap();
        // Same bucket, different paths -> one store.
        assert_eq!(store_key(&a), store_key(&b));
        assert_eq!(store_key(&a), "gs://bucket");

        let other = Url::parse("gs://other-bucket/x").unwrap();
        assert_ne!(store_key(&a), store_key(&other));

        let http = Url::parse("https://host.example/a/b").unwrap();
        assert_eq!(store_key(&http), "https://host.example");
    }
}
