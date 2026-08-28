use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::{Context, bail};
use url::Url;

const CURRENT_OBJECT: &str = "current.json";
const MAX_REGISTRIES: usize = 128;
const MAX_REGISTRY_ID_BYTES: usize = 64;

#[derive(Clone, PartialEq, Eq)]
pub struct RegistryCatalog {
    entries: Arc<BTreeMap<String, RegistryConfig>>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct RegistryConfig {
    pub(super) current_url: Url,
}

impl RegistryCatalog {
    pub fn empty() -> Self {
        Self {
            entries: Arc::new(BTreeMap::new()),
        }
    }

    /// Parses `registry_id=auth-root;...`. An empty string disables auth.
    pub fn parse(spec: &str) -> anyhow::Result<Self> {
        if spec.trim().is_empty() {
            return Ok(Self::empty());
        }

        let mut entries = BTreeMap::new();
        for raw_entry in spec.split(';') {
            let (raw_id, raw_root) = raw_entry
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("auth registry entry must be registry_id=URL"))?;
            let registry_id = raw_id.trim();
            validate_registry_id(registry_id)?;
            let auth_root = parse_auth_root(raw_root.trim())?;
            let current_url = auth_root
                .join(CURRENT_OBJECT)
                .context("resolve auth registry current.json")?;
            if entries
                .insert(registry_id.to_string(), RegistryConfig { current_url })
                .is_some()
            {
                bail!("duplicate auth registry id {registry_id:?}");
            }
            if entries.len() > MAX_REGISTRIES {
                bail!("too many auth registries; maximum is {MAX_REGISTRIES}");
            }
        }
        Ok(Self {
            entries: Arc::new(entries),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn registry_ids(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    pub(super) fn get(&self, registry_id: &str) -> Option<&RegistryConfig> {
        self.entries.get(registry_id)
    }
}

impl fmt::Debug for RegistryCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryCatalog")
            .field("registry_ids", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn parse_auth_root(raw: &str) -> anyhow::Result<Url> {
    let url = Url::parse(raw).context("parse auth registry URL")?;
    if !matches!(
        url.scheme(),
        "file" | "memory" | "gs" | "s3" | "http" | "https"
    ) {
        bail!(
            "auth registry URL scheme {:?} is not supported",
            url.scheme()
        );
    }
    if url.cannot_be_a_base() || !url.path().ends_with('/') {
        bail!("auth registry URL must be an absolute directory URL ending in `/`");
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        bail!("auth registry URL must not contain credentials, query, or fragment");
    }
    Ok(url)
}

pub(super) fn validate_registry_id(registry_id: &str) -> anyhow::Result<()> {
    if registry_id.is_empty() || registry_id.len() > MAX_REGISTRY_ID_BYTES {
        bail!("auth registry id must contain 1..={MAX_REGISTRY_ID_BYTES} bytes");
    }
    if !registry_id.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
    }) {
        bail!("auth registry id must use lowercase ASCII letters, digits, `-`, or `_`");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_catalog_rejects_ambiguous_or_secret_urls() {
        assert!(RegistryCatalog::parse("A=gs://bucket/auth/").is_err());
        assert!(RegistryCatalog::parse("a=gs://bucket/auth").is_err());
        assert!(RegistryCatalog::parse("a=https://user:secret@example/auth/").is_err());
        assert!(RegistryCatalog::parse("a=gs://bucket/a/;a=gs://bucket/b/").is_err());
    }
}
