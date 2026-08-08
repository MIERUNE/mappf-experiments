//! Conditional object-store writes required by management-plane publication.
//!
//! A writable backend is not sufficient: Abashiri needs create-only writes and
//! version-conditional replacement so concurrent publishers cannot silently
//! overwrite one another. The operator command in this module proves those
//! semantics against the configured backend before writer routes are enabled.

use std::{fmt::Display, sync::Arc};

use anyhow::{Context as _, bail, ensure};
use bytes::Bytes;
use object_store::{
    Attribute, AttributeValue, Attributes, Error as ObjectStoreError, GetResult, ObjectStore,
    ObjectStoreExt as _, PutMode, PutOptions, PutResult, UpdateVersion, parse_url_opts,
    path::Path as ObjectPath,
};
use url::Url;

use mmpf_http::request_id::RequestId;

const PROBE_PREFIX: &str = ".abashiri-capability-check";
const PROBE_METADATA_KEY: &str = "mmpf-capability-check";

/// Result of a successful backend capability check.
pub struct CheckOutcome {
    location: ObjectPath,
    cleaned_up: bool,
}

impl CheckOutcome {
    pub fn location(&self) -> &ObjectPath {
        &self.location
    }

    pub fn cleaned_up(&self) -> bool {
        self.cleaned_up
    }
}

/// A store rooted below a caller-selected object prefix.
#[derive(Clone)]
pub(crate) struct ConditionalStore {
    store: Arc<dyn ObjectStore>,
    root: ObjectPath,
}

impl ConditionalStore {
    pub(crate) fn from_url<I, K, V>(root: &Url, options: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        crate::store_policy::ensure_location_only(root, "storage root")?;
        let (store, root) =
            parse_url_opts(root, options).context("configure object-store backend")?;
        Ok(Self {
            store: store.into(),
            root,
        })
    }

    pub(crate) fn from_store(store: Arc<dyn ObjectStore>, root: ObjectPath) -> Self {
        Self { store, root }
    }

    pub(crate) fn location(&self, relative: &str) -> anyhow::Result<ObjectPath> {
        let relative = ObjectPath::parse(relative).context("parse relative object path")?;
        ensure!(
            !relative.is_root(),
            "relative object path must not be empty"
        );
        let mut location = self.root.clone();
        location.extend(&relative);
        Ok(location)
    }

    pub(crate) async fn create(
        &self,
        location: &ObjectPath,
        body: Bytes,
    ) -> object_store::Result<PutResult> {
        self.create_with_attributes(location, body, Attributes::new())
            .await
    }

    pub(crate) async fn create_with_attributes(
        &self,
        location: &ObjectPath,
        body: Bytes,
        attributes: Attributes,
    ) -> object_store::Result<PutResult> {
        self.store
            .put_opts(
                location,
                body.into(),
                PutOptions {
                    mode: PutMode::Create,
                    attributes,
                    ..PutOptions::default()
                },
            )
            .await
    }

    pub(crate) async fn update(
        &self,
        location: &ObjectPath,
        expected: UpdateVersion,
        body: Bytes,
    ) -> object_store::Result<PutResult> {
        self.update_with_attributes(location, expected, body, Attributes::new())
            .await
    }

    pub(crate) async fn update_with_attributes(
        &self,
        location: &ObjectPath,
        expected: UpdateVersion,
        body: Bytes,
        attributes: Attributes,
    ) -> object_store::Result<PutResult> {
        self.store
            .put_opts(
                location,
                body.into(),
                PutOptions {
                    mode: PutMode::Update(expected),
                    attributes,
                    ..PutOptions::default()
                },
            )
            .await
    }

    pub(crate) async fn get(&self, location: &ObjectPath) -> object_store::Result<GetResult> {
        self.store.get(location).await
    }

    pub(crate) async fn read(&self, location: &ObjectPath) -> object_store::Result<Bytes> {
        self.store.get(location).await?.bytes().await
    }
}

/// Proves the write preconditions needed by Abashiri on one actual backend.
///
/// By default the uniquely named probe remains below [`PROBE_PREFIX`] for an
/// object-lifecycle rule to expire. `cleanup` is an explicit diagnostic option,
/// not a capability required of the production publisher identity.
pub async fn check_backend<I, K, V>(
    root: &Url,
    options: I,
    cleanup: bool,
) -> anyhow::Result<CheckOutcome>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let conditional = ConditionalStore::from_url(root, options)?;
    let probe_id = RequestId::new_random();
    let location = conditional.location(&format!("{PROBE_PREFIX}/{probe_id}"))?;

    // Cleanup is authorized only after this process successfully creates the
    // object. If the create reports a collision, a pre-existing object is not
    // ours to delete.
    let first = conditional
        .create(&location, Bytes::from_static(b"abashiri-cas-v1"))
        .await
        .with_context(|| format!("create capability probe object {location}"))?;

    let result = run_owned_probe(&conditional, &location, first)
        .await
        .with_context(|| format!("check capability probe object {location}"));
    if !cleanup {
        result?;
        return Ok(CheckOutcome {
            location,
            cleaned_up: false,
        });
    }

    let cleanup = conditional.store.delete(&location).await;

    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(CheckOutcome {
            location,
            cleaned_up: true,
        }),
        (Ok(()), Err(error)) => {
            Err(error).with_context(|| format!("delete capability probe object {location}"))
        }
        (Err(probe_error), Ok(())) => Err(probe_error),
        (Err(probe_error), Err(cleanup_error)) => Err(probe_error).context(format!(
            "additionally failed to delete capability probe object {location}: {cleanup_error}"
        )),
    }
}

async fn run_owned_probe(
    conditional: &ConditionalStore,
    location: &ObjectPath,
    first: PutResult,
) -> anyhow::Result<()> {
    require_error(
        conditional
            .create(location, Bytes::from_static(b"unexpected-overwrite"))
            .await,
        |error| matches!(error, ObjectStoreError::AlreadyExists { .. }),
        "duplicate create was not rejected with AlreadyExists",
    )?;

    let first_version: UpdateVersion = first.into();
    ensure!(
        first_version.e_tag.is_some() || first_version.version.is_some(),
        "backend returned no version or ETag for a created object"
    );

    let second = conditional
        .update_with_attributes(
            location,
            first_version.clone(),
            Bytes::from_static(b"abashiri-cas-v2"),
            probe_attributes(),
        )
        .await
        .with_context(|| format!("conditionally update capability probe object {location}"))?;
    let second_version: UpdateVersion = second.into();
    ensure!(
        second_version.e_tag.is_some() || second_version.version.is_some(),
        "backend returned no version or ETag for an updated object"
    );

    require_error(
        conditional
            .update(
                location,
                first_version,
                Bytes::from_static(b"stale-writer-must-not-win"),
            )
            .await,
        |error| matches!(error, ObjectStoreError::Precondition { .. }),
        "stale conditional update was not rejected with Precondition",
    )?;

    let stored = conditional
        .get(location)
        .await
        .with_context(|| format!("read capability probe object {location}"))?;
    ensure!(
        stored
            .attributes
            .get(&Attribute::ContentType)
            .is_some_and(|value| value.as_ref() == "application/json")
            && stored
                .attributes
                .get(&Attribute::CacheControl)
                .is_some_and(|value| value.as_ref() == "no-store")
            && stored
                .attributes
                .get(&Attribute::Metadata(PROBE_METADATA_KEY.into()))
                .is_some_and(|value| value.as_ref() == "v1"),
        "backend did not preserve required object attributes"
    );
    let body = stored
        .bytes()
        .await
        .with_context(|| format!("collect capability probe object {location}"))?;
    ensure!(
        body == Bytes::from_static(b"abashiri-cas-v2"),
        "conditional update did not leave the expected object body"
    );
    Ok(())
}

fn probe_attributes() -> Attributes {
    [
        (
            Attribute::ContentType,
            AttributeValue::from("application/json"),
        ),
        (Attribute::CacheControl, AttributeValue::from("no-store")),
        (
            Attribute::Metadata(PROBE_METADATA_KEY.into()),
            AttributeValue::from("v1"),
        ),
    ]
    .into_iter()
    .collect()
}

fn require_error<T, F>(
    result: object_store::Result<T>,
    expected: F,
    message: impl Display,
) -> anyhow::Result<()>
where
    F: FnOnce(&ObjectStoreError) -> bool,
{
    match result {
        Err(error) if expected(&error) => Ok(()),
        Err(error) => bail!("{message}: {error}"),
        Ok(_) => bail!("{message}: operation unexpectedly succeeded"),
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn conditional_writer_rejects_duplicate_and_stale_writes() {
        let store = ConditionalStore {
            store: Arc::new(InMemory::new()),
            root: ObjectPath::from("management"),
        };
        let location = store.location("registry/current.json").unwrap();

        let first = store
            .create(&location, Bytes::from_static(b"first"))
            .await
            .unwrap();
        let first_version: UpdateVersion = first.into();
        assert!(matches!(
            store
                .create(&location, Bytes::from_static(b"duplicate"))
                .await,
            Err(ObjectStoreError::AlreadyExists { .. })
        ));
        store
            .update(
                &location,
                first_version.clone(),
                Bytes::from_static(b"second"),
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .update(&location, first_version, Bytes::from_static(b"stale"))
                .await,
            Err(ObjectStoreError::Precondition { .. })
        ));
        assert_eq!(
            store
                .store
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"second")
        );
    }

    #[tokio::test]
    async fn capability_check_retains_probe_without_delete_permission() {
        let outcome = check_backend(
            &Url::parse("memory:///control").unwrap(),
            Vec::<(String, String)>::new(),
            false,
        )
        .await
        .unwrap();
        assert!(!outcome.cleaned_up());
        assert!(
            outcome
                .location()
                .as_ref()
                .starts_with("control/.abashiri-capability-check/")
        );
    }

    #[tokio::test]
    async fn capability_check_can_explicitly_clean_up() {
        let outcome = check_backend(
            &Url::parse("memory:///control").unwrap(),
            Vec::<(String, String)>::new(),
            true,
        )
        .await
        .unwrap();
        assert!(outcome.cleaned_up());
    }

    #[test]
    fn root_and_relative_paths_are_composed_without_string_concatenation() {
        let store = ConditionalStore {
            store: Arc::new(InMemory::new()),
            root: ObjectPath::from("control"),
        };
        assert_eq!(
            store.location("registry/current.json").unwrap().as_ref(),
            "control/registry/current.json"
        );
        assert!(store.location("../escape").is_err());
        assert!(store.location("").is_err());
    }

    #[test]
    fn storage_root_rejects_query_and_fragment() {
        for url in [
            "memory:///control?credential=secret",
            "memory:///control#fragment",
            "gs://user:secret@example/control",
        ] {
            assert!(
                ConditionalStore::from_url(
                    &Url::parse(url).unwrap(),
                    Vec::<(String, String)>::new()
                )
                .is_err()
            );
        }
    }
}
