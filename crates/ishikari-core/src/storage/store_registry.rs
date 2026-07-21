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

use anyhow::Result;
use mmpf_common::sync::lock_unpoisoned;
use object_store::{ObjectStore, parse_url_opts, path::Path as ObjectPath};
use url::Url;

/// Caches object stores keyed by scheme + authority.
pub struct ObjectStoreRegistry {
    options: Arc<[(String, String)]>,
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ObjectStoreRegistry {
    /// Creates a registry with object-store configuration supplied by the
    /// process entry point.
    ///
    /// The library deliberately does not read the process environment. A
    /// production server may pass `std::env::vars()`, while tests and embedded
    /// callers can supply a deterministic, restricted set of options.
    pub fn new<I, K, V>(options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            options: options
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect::<Vec<_>>()
                .into(),
            stores: Mutex::new(HashMap::new()),
        }
    }

    /// Creates a deterministic registry without ambient configuration.
    pub fn without_options() -> Self {
        Self::new(std::iter::empty::<(String, String)>())
    }

    /// Resolves a URL to a reused object store plus the object path within it.
    /// The store is built once per scheme + authority and cached; the path is
    /// derived from the URL so different prefixes on one bucket share a store.
    pub fn resolve(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let key = store_key(url);
        let store = {
            // Recover a poisoned lock rather than cascading a panic across every
            // subsequent resolve: the guarded map is independently valid (each
            // entry is a self-contained cached store), matching the crate-wide
            // `lock_unpoisoned` policy used elsewhere.
            let mut stores = lock_unpoisoned(&self.stores);
            if let Some(store) = stores.get(&key) {
                store.clone()
            } else {
                let store = self.build_store(url)?;
                stores.insert(key, store.clone());
                store
            }
        };
        let source_label = redacted_source_label(url);
        let path = ObjectPath::from_url_path(url.path())
            .map_err(|_| anyhow::anyhow!("invalid object path for {source_label}"))?;
        Ok((store, path))
    }

    /// Builds the backing store for a URL. The in-memory and local-filesystem
    /// stores are always available; cloud backends (`gs://`, `s3://`,
    /// `http(s)://`) require the `cloud` feature.
    fn build_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        match url.scheme() {
            "file" | "memory" => {
                let source_label = redacted_source_label(url);
                let (store, _path) = parse_url_opts(url, self.options.iter().cloned())
                    .map_err(|_| anyhow::anyhow!("failed to configure {source_label}"))?;
                Ok(store.into())
            }
            _ => self.build_cloud_store(url),
        }
    }

    /// Builds a cloud object store (GCS/S3/HTTP) when the `cloud` feature is
    /// compiled in.
    #[cfg(feature = "cloud")]
    fn build_cloud_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        // The HTTP backend refuses plain-text HTTP by default, but `http://` is
        // an accepted provider-template scheme (local and dev upstreams). The
        // URL scheme already states the intent, so enable it here instead of
        // requiring an ALLOW_HTTP env var.
        let allow_http =
            (url.scheme() == "http").then_some(("allow_http".to_string(), "true".to_string()));
        let options = self.options.iter().cloned().chain(allow_http);
        let source_label = redacted_source_label(url);
        let (store, _path) = parse_url_opts(url, options)
            .map_err(|_| anyhow::anyhow!("failed to configure {source_label}"))?;
        Ok(store.into())
    }

    /// Fails clearly when a cloud URL is resolved in a build without the
    /// `cloud` feature (e.g. the simulator, which only uses in-memory and
    /// local stores).
    #[cfg(not(feature = "cloud"))]
    fn build_cloud_store(&self, url: &Url) -> Result<Arc<dyn ObjectStore>> {
        anyhow::bail!(
            "cloud object stores not compiled; enable the `cloud` feature to configure {}",
            redacted_source_label(url)
        )
    }
}

/// Returns a bounded diagnostic label that cannot retain credentials or
/// source-specific host, path, query, or fragment data.
pub(crate) fn redacted_source_label(url: &Url) -> String {
    format!("{}://<redacted>", url.scheme())
}

/// Identifies the object store backing a URL by scheme + authority
/// (bucket/host), independent of the object path.
fn store_key(url: &Url) -> String {
    format!("{}://{}", url.scheme(), url.authority())
}

#[cfg(test)]
mod tests {
    use super::{ObjectStoreRegistry, redacted_source_label, store_key};
    use url::Url;

    #[test]
    fn diagnostic_source_label_retains_only_the_scheme() {
        let url = Url::parse(
            "https://alice:super-secret@private.example/signed/path?token=hidden#fragment",
        )
        .unwrap();

        let label = redacted_source_label(&url);

        assert_eq!(label, "https://<redacted>");
        for secret in [
            "alice",
            "super-secret",
            "private.example",
            "signed",
            "hidden",
            "fragment",
        ] {
            assert!(!label.contains(secret), "source label leaked {secret:?}");
        }
    }

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

    // Only meaningful with the cloud backends compiled: the injected option is
    // parsed by the cloud store constructor. Without `cloud`, resolving an
    // `https://` URL errors from the feature gate, not from option parsing, so
    // the test would pass without exercising constructor option-injection.
    #[cfg(feature = "cloud")]
    #[test]
    fn object_store_errors_do_not_echo_source_secrets() {
        let registry = ObjectStoreRegistry::new([("allow_invalid_certificates", "not-a-bool")]);
        let url = Url::parse(
            "https://alice:super-secret@host.example/object?token=signed-secret#fragment",
        )
        .unwrap();

        let error = registry
            .resolve(&url)
            .expect_err("invalid option must fail");
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains("https://<redacted>"));
        for secret in ["alice", "super-secret", "host.example", "signed-secret"] {
            assert!(!diagnostic.contains(secret), "error leaked {secret:?}");
        }
    }

    #[cfg(not(feature = "cloud"))]
    #[test]
    fn feature_gate_error_does_not_echo_source_secrets() {
        let registry = ObjectStoreRegistry::without_options();
        let url = Url::parse(
            "https://alice:super-secret@host.example/object?token=signed-secret#fragment",
        )
        .unwrap();

        let error = registry
            .resolve(&url)
            .expect_err("cloud feature must be required");
        let diagnostic = format!("{error:#}");
        assert!(diagnostic.contains("https://<redacted>"));
        for secret in ["alice", "super-secret", "host.example", "signed-secret"] {
            assert!(!diagnostic.contains(secret), "error leaked {secret:?}");
        }
    }
}
