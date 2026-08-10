//! Conditional publication of mutable MapLibre style documents.
//!
//! The HTTP API and account-to-delivery-path catalog remain deliberately
//! outside this module. A trusted caller supplies the catalog-resolved object
//! path. This module binds the resulting object to the mutation journal,
//! preserves exact style bytes, and recovers idempotently when state committed
//! but the journal completion did not.

use std::{borrow::Cow, sync::Arc};

use anyhow::{Context as _, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt as _;
use object_store::{Attribute, AttributeValue, Attributes, ObjectStore, path::Path as ObjectPath};
use serde_json::Value;
use thiserror::Error;
use url::Url;

use mmpf_http::{request_id::RequestId, style_key::StyleKey};

use crate::{
    catalog::StyleCatalog,
    mutation::{
        AccountId, Actor, Execution, LocalResourceId, MutationAction, MutationIntent,
        MutationJournal, MutationRequest, ResourceKind, ResourceTarget, StateCommit,
        VersionEvidence, digest_hex,
    },
    storage::ConditionalStore,
};

pub const MAX_STYLE_BYTES: usize = 2 * 1024 * 1024;
const STYLE_DIGEST_DOMAIN: &[u8] = b"abashiri-style-content-v1\0";
const MUTATION_METADATA: &str = "mmpf-mutation-reference";
const STYLE_CACHE_CONTROL: &str =
    "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400";

/// Client-visible concurrency conflicts from style publication.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StylePublishConflict {
    #[error("style already exists")]
    AlreadyExists,
    #[error("style replacement precondition did not match")]
    PreconditionFailed,
    #[error("style replacement target does not exist")]
    NotFound,
}

/// Catalog-resolved object path for one mutable style document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleObjectPath {
    path: ObjectPath,
    delivery_style_id: String,
}

impl StyleObjectPath {
    pub fn try_new(relative: &str) -> anyhow::Result<Self> {
        let path = ObjectPath::parse(relative).context("parse style object path")?;
        let style_path = path
            .as_ref()
            .strip_prefix("styles/")
            .context("style object path must be under styles/")?;
        let delivery_style_id = style_path
            .strip_suffix("/style.json")
            .or_else(|| style_path.strip_suffix(".json"))
            .context(
                "style object path must match styles/{namespace}/{style_id}/style.json or styles/{namespace}/{style_id}.json",
            )?;
        StyleKey::parse(delivery_style_id)
            .context("style object path must contain a canonical style key")?;
        let delivery_style_id = delivery_style_id.to_owned();
        Ok(Self {
            path,
            delivery_style_id,
        })
    }

    /// Logical delivery style ID addressed by Biei and Ishikari refresh hints.
    pub fn delivery_style_id(&self) -> &str {
        &self.delivery_style_id
    }
}

impl AsRef<str> for StyleObjectPath {
    fn as_ref(&self) -> &str {
        self.path.as_ref()
    }
}

/// Required state of the style before publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StylePrecondition {
    MustNotExist,
    MustMatch(VersionEvidence),
}

/// Validated style publication request.
pub struct PublishStyleRequest {
    idempotency_key: String,
    actor: Actor,
    target: ResourceTarget,
    location: StyleObjectPath,
    precondition: StylePrecondition,
    body: Bytes,
    content_sha256: String,
    request_id: RequestId,
}

impl PublishStyleRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        idempotency_key: impl Into<String>,
        actor: Actor,
        account_id: AccountId,
        style_id: LocalResourceId,
        location: StyleObjectPath,
        precondition: StylePrecondition,
        body: Bytes,
        request_id: RequestId,
    ) -> anyhow::Result<Self> {
        validate_style(&body)?;
        let content_sha256 = digest_hex(STYLE_DIGEST_DOMAIN, &body);
        Ok(Self {
            idempotency_key: idempotency_key.into(),
            actor,
            target: ResourceTarget::new(account_id, ResourceKind::Style, style_id),
            location,
            precondition,
            body,
            content_sha256,
            request_id,
        })
    }
}

/// Result of a newly completed style publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedStyle {
    location: StyleObjectPath,
    content_sha256: String,
    version: VersionEvidence,
}

/// Current published style bytes and the validator required for replacement.
pub struct PublishedStyleDocument {
    body: Bytes,
    version: VersionEvidence,
}

impl PublishedStyleDocument {
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    pub fn version(&self) -> &VersionEvidence {
        &self.version
    }
}

impl PublishedStyle {
    pub fn location(&self) -> &StyleObjectPath {
        &self.location
    }

    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    pub fn version(&self) -> &VersionEvidence {
        &self.version
    }
}

/// Journal and published-state composition for style publication.
pub struct StylePublisher {
    journal: MutationJournal,
    state: ConditionalStore,
}

/// Result of one sequential reconciliation scan over the durable journal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StyleReconciliationReport {
    scanned_intents: usize,
    unfinished_intents: usize,
    completed: usize,
    already_completed: usize,
    not_committed: usize,
    superseded: usize,
    unsupported: usize,
    missing_catalog_entry: usize,
}

impl StyleReconciliationReport {
    pub fn unfinished_intents(&self) -> usize {
        self.unfinished_intents
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StyleReconciliationOutcome {
    Completed,
    AlreadyCompleted,
    NotCommitted,
    Superseded,
    Unsupported,
    MissingCatalogEntry,
}

impl StylePublisher {
    /// Configures independently authorized roots for public state and private audit data.
    pub fn from_urls<I, K, V>(
        state_root: &Url,
        journal_root: &Url,
        options: I,
    ) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        ensure_separate_remote_stores(state_root, journal_root)?;
        let options = options
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_owned(), value.into()))
            .collect::<Vec<_>>();
        Ok(Self {
            journal: MutationJournal::from_url(journal_root, options.clone())?,
            state: ConditionalStore::from_url(state_root, options)?,
        })
    }

    /// Injects distinct stores for tests or an embedding with its own IAM configuration.
    pub fn from_object_stores(
        state_store: Arc<dyn ObjectStore>,
        state_root: ObjectPath,
        journal_store: Arc<dyn ObjectStore>,
        journal_root: ObjectPath,
    ) -> Self {
        Self {
            journal: MutationJournal::from_object_store(journal_store, journal_root),
            state: ConditionalStore::from_store(state_store, state_root),
        }
    }

    /// Reads the exact currently published style and its replacement validator.
    pub async fn get(
        &self,
        location: &StyleObjectPath,
    ) -> anyhow::Result<Option<PublishedStyleDocument>> {
        let Some(result) = self.read_state_object(location, "published style").await? else {
            return Ok(None);
        };
        let version =
            VersionEvidence::from_meta(&result.meta).context("read published style version")?;
        let body = result.bytes().await.context("collect published style")?;
        validate_style(&body).context("stored style is invalid")?;
        Ok(Some(PublishedStyleDocument { body, version }))
    }

    async fn read_state_object(
        &self,
        location: &StyleObjectPath,
        noun: &str,
    ) -> anyhow::Result<Option<object_store::GetResult>> {
        let object_location = self.state.location(location.as_ref())?;
        match self.state.get(&object_location).await {
            Ok(result) => Ok(Some(result)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error).context(format!("read {noun}")),
        }
    }

    pub async fn publish(
        &self,
        request: PublishStyleRequest,
    ) -> anyhow::Result<Execution<PublishedStyle>> {
        let input_identity =
            style_input_identity(&request.location, &request.precondition, &request.body);
        let mutation = MutationRequest::try_new(
            &request.idempotency_key,
            request.actor,
            MutationAction::Publish,
            request.target,
            &input_identity,
            request.request_id,
        )?
        .with_state_locator(request.location.as_ref())?;
        let location = request.location;
        let precondition = request.precondition;
        let body = request.body;
        let content_sha256 = request.content_sha256;

        self.journal
            .execute(mutation, |intent| {
                let mutation_reference = intent.state_reference().to_string();
                async move {
                    self.commit_style(
                        &mutation_reference,
                        &location,
                        &precondition,
                        body,
                        &content_sha256,
                    )
                    .await
                }
            })
            .await
    }

    /// Completes journal records for style state that already durably names an intent.
    ///
    /// The scan never replays a state mutation. An absent object, a catalog miss,
    /// or state naming a newer intent is retained as an unfinished audit attempt.
    pub async fn reconcile_unfinished(
        &self,
        catalog: &StyleCatalog,
    ) -> anyhow::Result<StyleReconciliationReport> {
        let mut report = StyleReconciliationReport::default();
        let mut locations = self.journal.intent_locations()?;
        while let Some(meta) = locations
            .try_next()
            .await
            .context("list mutation journal intents")?
        {
            report.scanned_intents += 1;
            let Some(intent) = self.journal.unfinished_intent(&meta.location).await? else {
                continue;
            };
            report.unfinished_intents += 1;
            match self.reconcile_style_intent(catalog, &intent).await? {
                StyleReconciliationOutcome::Completed => report.completed += 1,
                StyleReconciliationOutcome::AlreadyCompleted => report.already_completed += 1,
                StyleReconciliationOutcome::NotCommitted => report.not_committed += 1,
                StyleReconciliationOutcome::Superseded => report.superseded += 1,
                StyleReconciliationOutcome::Unsupported => report.unsupported += 1,
                StyleReconciliationOutcome::MissingCatalogEntry => {
                    report.missing_catalog_entry += 1;
                }
            }
        }
        Ok(report)
    }

    async fn reconcile_style_intent(
        &self,
        catalog: &StyleCatalog,
        intent: &MutationIntent,
    ) -> anyhow::Result<StyleReconciliationOutcome> {
        if intent.action() != MutationAction::Publish
            || intent.target().kind() != ResourceKind::Style
        {
            return Ok(StyleReconciliationOutcome::Unsupported);
        }
        let persisted_location = intent
            .state_locator()
            .map(StyleObjectPath::try_new)
            .transpose()
            .context("decode reconciled style state locator")?;
        let location = match persisted_location
            .as_ref()
            .or_else(|| catalog.resolve(intent.target().account_id(), intent.target().local_id()))
        {
            Some(location) => location,
            None => return Ok(StyleReconciliationOutcome::MissingCatalogEntry),
        };
        let Some(current) = self
            .read_state_object(location, "style state during reconciliation")
            .await?
        else {
            return Ok(StyleReconciliationOutcome::NotCommitted);
        };
        if !current
            .attributes
            .get(&mutation_attribute())
            .is_some_and(|value| value.as_ref() == intent.state_reference())
        {
            return Ok(StyleReconciliationOutcome::Superseded);
        }
        let version = VersionEvidence::from_meta(&current.meta)
            .context("read style version during reconciliation")?;
        let (committed, _) = style_commit_from_current(current, location, version).await?;
        Ok(
            match self.journal.complete_intent(intent, committed).await? {
                Execution::Committed(_) => StyleReconciliationOutcome::Completed,
                Execution::AlreadyCompleted(_) => StyleReconciliationOutcome::AlreadyCompleted,
            },
        )
    }

    async fn commit_style(
        &self,
        mutation_reference: &str,
        location: &StyleObjectPath,
        precondition: &StylePrecondition,
        body: Bytes,
        content_sha256: &str,
    ) -> anyhow::Result<StateCommit<PublishedStyle>> {
        let object_location = self.state.location(location.as_ref())?;
        let current = self
            .read_state_object(location, "current style object")
            .await?;

        if let Some(current) = current {
            let version =
                VersionEvidence::from_meta(&current.meta).context("read current style version")?;
            let existing_mutation = current
                .attributes
                .get(&mutation_attribute())
                .map(AsRef::as_ref);
            if existing_mutation == Some(mutation_reference) {
                return existing_style_commit(current, location, content_sha256, version).await;
            }

            let StylePrecondition::MustMatch(expected) = precondition else {
                return Err(StylePublishConflict::AlreadyExists.into());
            };
            if version != *expected {
                return Err(StylePublishConflict::PreconditionFailed.into());
            }
            let result = self
                .state
                .update_with_attributes(
                    &object_location,
                    expected.as_update_version(),
                    body,
                    style_attributes(mutation_reference),
                )
                .await;
            return match result {
                Ok(result) => style_commit_from_put(location, content_sha256, result),
                Err(object_store::Error::Precondition { .. }) => {
                    self.resolve_write_race(
                        &object_location,
                        mutation_reference,
                        location,
                        precondition,
                        content_sha256,
                    )
                    .await
                }
                Err(error) => Err(error).context("conditionally replace style object"),
            };
        }

        if !matches!(precondition, StylePrecondition::MustNotExist) {
            return Err(StylePublishConflict::NotFound.into());
        }
        let result = self
            .state
            .create_with_attributes(&object_location, body, style_attributes(mutation_reference))
            .await;
        match result {
            Ok(result) => style_commit_from_put(location, content_sha256, result),
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.resolve_write_race(
                    &object_location,
                    mutation_reference,
                    location,
                    precondition,
                    content_sha256,
                )
                .await
            }
            Err(error) => Err(error).context("create style object"),
        }
    }

    async fn resolve_write_race(
        &self,
        object_location: &ObjectPath,
        mutation_reference: &str,
        location: &StyleObjectPath,
        precondition: &StylePrecondition,
        content_sha256: &str,
    ) -> anyhow::Result<StateCommit<PublishedStyle>> {
        let current = match self.state.get(object_location).await {
            Ok(current) => current,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(match precondition {
                    StylePrecondition::MustNotExist => StylePublishConflict::AlreadyExists,
                    StylePrecondition::MustMatch(_) => StylePublishConflict::NotFound,
                }
                .into());
            }
            Err(error) => return Err(error).context("read style after conditional write conflict"),
        };
        let version = VersionEvidence::from_meta(&current.meta)
            .context("read style version after conditional write conflict")?;
        if current
            .attributes
            .get(&mutation_attribute())
            .is_some_and(|value| value.as_ref() == mutation_reference)
        {
            return existing_style_commit(current, location, content_sha256, version).await;
        }
        Err(match precondition {
            StylePrecondition::MustNotExist => StylePublishConflict::AlreadyExists,
            StylePrecondition::MustMatch(_) => StylePublishConflict::PreconditionFailed,
        }
        .into())
    }
}

fn ensure_separate_remote_stores(state_root: &Url, journal_root: &Url) -> anyhow::Result<()> {
    let local_development = |url: &Url| matches!(url.scheme(), "file" | "memory");
    if !local_development(state_root) && !local_development(journal_root) {
        ensure!(
            state_root.scheme() != journal_root.scheme()
                || state_root.host_str() != journal_root.host_str()
                || state_root.port_or_known_default() != journal_root.port_or_known_default(),
            "published state and mutation journal must use different object-store authorities; prefixes in one bucket are not a portable IAM boundary"
        );
    }
    Ok(())
}

async fn existing_style_commit(
    current: object_store::GetResult,
    location: &StyleObjectPath,
    content_sha256: &str,
    version: VersionEvidence,
) -> anyhow::Result<StateCommit<PublishedStyle>> {
    let (committed, observed_sha256) =
        style_commit_from_current(current, location, version).await?;
    ensure!(
        observed_sha256 == content_sha256,
        "style object for this mutation contains different content"
    );
    Ok(committed)
}

async fn style_commit_from_current(
    current: object_store::GetResult,
    location: &StyleObjectPath,
    version: VersionEvidence,
) -> anyhow::Result<(StateCommit<PublishedStyle>, String)> {
    ensure!(
        current
            .attributes
            .get(&Attribute::ContentType)
            .is_some_and(|value| value.as_ref() == "application/json")
            && current
                .attributes
                .get(&Attribute::CacheControl)
                .is_some_and(|value| value.as_ref() == STYLE_CACHE_CONTROL),
        "style object for this mutation has invalid response attributes"
    );
    ensure!(
        current.meta.size <= MAX_STYLE_BYTES as u64,
        "style object for this mutation is too large"
    );
    let current_body = current
        .bytes()
        .await
        .context("collect current style object")?;
    validate_style(&current_body).context("style object for this mutation is invalid")?;
    let content_sha256 = digest_hex(STYLE_DIGEST_DOMAIN, &current_body);
    let committed = style_commit(location.clone(), content_sha256.clone(), version);
    Ok((committed, content_sha256))
}

fn style_commit_from_put(
    location: &StyleObjectPath,
    content_sha256: &str,
    result: object_store::PutResult,
) -> anyhow::Result<StateCommit<PublishedStyle>> {
    let version = VersionEvidence::try_from_put(result)?;
    Ok(style_commit(
        location.clone(),
        content_sha256.to_string(),
        version,
    ))
}

fn style_commit(
    location: StyleObjectPath,
    content_sha256: String,
    version: VersionEvidence,
) -> StateCommit<PublishedStyle> {
    let state_identity = format!(
        "{}\0{}\0{}",
        location.as_ref(),
        content_sha256,
        version_identity(&version)
    );
    let response = PublishedStyle {
        location,
        content_sha256,
        version: version.clone(),
    };
    StateCommit::new(version, response, state_identity.as_bytes())
}

fn version_identity(version: &VersionEvidence) -> String {
    let update = version.as_update_version();
    format!(
        "{}\0{}",
        update.e_tag.as_deref().unwrap_or_default(),
        update.version.as_deref().unwrap_or_default()
    )
}

fn style_attributes(mutation_reference: &str) -> Attributes {
    [
        (
            Attribute::ContentType,
            AttributeValue::from("application/json"),
        ),
        (
            Attribute::CacheControl,
            AttributeValue::from(STYLE_CACHE_CONTROL),
        ),
        (
            mutation_attribute(),
            AttributeValue::from(mutation_reference.to_string()),
        ),
    ]
    .into_iter()
    .collect()
}

fn mutation_attribute() -> Attribute {
    Attribute::Metadata(Cow::Borrowed(MUTATION_METADATA))
}

fn style_input_identity(
    location: &StyleObjectPath,
    precondition: &StylePrecondition,
    body: &[u8],
) -> Vec<u8> {
    let precondition = match precondition {
        StylePrecondition::MustNotExist => "create".to_string(),
        StylePrecondition::MustMatch(version) => {
            format!("replace\0{}", version_identity(version))
        }
    };
    let mut identity =
        Vec::with_capacity(location.as_ref().len() + precondition.len() + body.len() + 2);
    identity.extend_from_slice(location.as_ref().as_bytes());
    identity.push(0);
    identity.extend_from_slice(precondition.as_bytes());
    identity.push(0);
    identity.extend_from_slice(body);
    identity
}

fn validate_style(body: &[u8]) -> anyhow::Result<()> {
    ensure!(!body.is_empty(), "style document must not be empty");
    ensure!(
        body.len() <= MAX_STYLE_BYTES,
        "style document exceeds {MAX_STYLE_BYTES} bytes"
    );
    let style: Value = serde_json::from_slice(body).context("decode style JSON")?;
    ensure!(style.is_object(), "style document must be a JSON object");
    ensure!(
        style.get("version").and_then(Value::as_u64) == Some(8),
        "style document must declare version 8"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use object_store::{ObjectStoreExt as _, memory::InMemory};
    use tokio::sync::Barrier;

    use super::*;
    use crate::mutation::{ActorKind, Execution, mutation_key_sha256};

    fn publisher() -> StylePublisher {
        StylePublisher::from_object_stores(
            Arc::new(InMemory::new()),
            ObjectPath::from("state"),
            Arc::new(InMemory::new()),
            ObjectPath::from("audit"),
        )
    }

    fn actor() -> Actor {
        Actor::try_new(ActorKind::Workload, "test", "publisher").unwrap()
    }

    fn catalog() -> StyleCatalog {
        StyleCatalog::parse(
            br#"{"schema_version":1,"styles":[{"account_id":"example","style_id":"basic","object_path":"styles/demo/basic/style.json"}]}"#,
        )
        .unwrap()
    }

    fn request(
        key: &str,
        precondition: StylePrecondition,
        body: &'static [u8],
    ) -> PublishStyleRequest {
        PublishStyleRequest::try_new(
            key,
            actor(),
            AccountId::try_new("example").unwrap(),
            LocalResourceId::try_new("basic").unwrap(),
            StyleObjectPath::try_new("styles/demo/basic/style.json").unwrap(),
            precondition,
            Bytes::from_static(body),
            RequestId::new_random(),
        )
        .unwrap()
    }

    fn mutation_for(request: &PublishStyleRequest) -> MutationRequest {
        MutationRequest::try_new(
            &request.idempotency_key,
            request.actor.clone(),
            MutationAction::Publish,
            request.target.clone(),
            &style_input_identity(&request.location, &request.precondition, &request.body),
            request.request_id.clone(),
        )
        .and_then(|mutation| mutation.with_state_locator(request.location.as_ref()))
        .unwrap()
    }

    async fn publish_after_intent_barrier(
        publisher: &StylePublisher,
        request: PublishStyleRequest,
        barrier: Arc<Barrier>,
    ) -> anyhow::Result<Execution<PublishedStyle>> {
        let mutation = mutation_for(&request);
        let location = request.location;
        let precondition = request.precondition;
        let body = request.body;
        let content_sha256 = request.content_sha256;
        publisher
            .journal
            .execute(mutation, |intent| {
                let mutation_reference = intent.state_reference().to_string();
                async move {
                    barrier.wait().await;
                    publisher
                        .commit_style(
                            &mutation_reference,
                            &location,
                            &precondition,
                            body,
                            &content_sha256,
                        )
                        .await
                }
            })
            .await
    }

    #[tokio::test]
    async fn publishes_exact_style_with_cache_and_mutation_metadata() {
        let publisher = publisher();
        let body = br#"{"version":8,"sources":{},"layers":[]}"#;
        let outcome = publisher
            .publish(request("style-1", StylePrecondition::MustNotExist, body))
            .await
            .unwrap();
        let Execution::Committed(published) = outcome else {
            panic!("first publication unexpectedly reported a retry");
        };
        assert_eq!(
            published.location().as_ref(),
            "styles/demo/basic/style.json"
        );

        let location = publisher
            .state
            .location("styles/demo/basic/style.json")
            .unwrap();
        let stored = publisher.state.get(&location).await.unwrap();
        assert_eq!(
            stored
                .attributes
                .get(&Attribute::ContentType)
                .unwrap()
                .as_ref(),
            "application/json"
        );
        assert_eq!(
            stored
                .attributes
                .get(&Attribute::CacheControl)
                .unwrap()
                .as_ref(),
            STYLE_CACHE_CONTROL
        );
        let mutation_reference = stored
            .attributes
            .get(&mutation_attribute())
            .unwrap()
            .as_ref();
        assert_eq!(mutation_reference.len(), 32);
        assert_ne!(mutation_reference, mutation_key_sha256("style-1").unwrap());
        assert_eq!(stored.bytes().await.unwrap(), body.as_slice());

        let published = publisher
            .get(&StyleObjectPath::try_new("styles/demo/basic/style.json").unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(published.body(), body.as_slice());
        assert!(published.version().to_entity_tag().is_ok());
    }

    #[tokio::test]
    async fn completed_retry_does_not_republish_style() {
        let publisher = publisher();
        let body = br#"{"version":8,"sources":{},"layers":[]}"#;
        publisher
            .publish(request("style-2", StylePrecondition::MustNotExist, body))
            .await
            .unwrap();
        let retry = publisher
            .publish(request("style-2", StylePrecondition::MustNotExist, body))
            .await
            .unwrap();
        assert!(matches!(retry, Execution::AlreadyCompleted(_)));
    }

    #[tokio::test]
    async fn retry_after_failure_before_state_write_reuses_the_key() {
        let publisher = publisher();
        let publication = request(
            "style-prewrite-failure",
            StylePrecondition::MustNotExist,
            br#"{"version":8,"sources":{},"layers":[]}"#,
        );
        let interrupted: anyhow::Result<Execution<PublishedStyle>> = publisher
            .journal
            .execute(mutation_for(&publication), |_| async {
                anyhow::bail!("simulated failure before state write")
            })
            .await;
        assert!(interrupted.is_err());

        let retry = publisher.publish(publication).await.unwrap();
        assert!(matches!(retry, Execution::Committed(_)));
    }

    #[tokio::test]
    async fn concurrent_identical_publications_converge_on_one_commit() {
        let publisher = publisher();
        let barrier = Arc::new(Barrier::new(2));
        let body = br#"{"version":8,"sources":{},"layers":[]}"#;
        let left = publish_after_intent_barrier(
            &publisher,
            request("style-concurrent", StylePrecondition::MustNotExist, body),
            Arc::clone(&barrier),
        );
        let right = publish_after_intent_barrier(
            &publisher,
            request("style-concurrent", StylePrecondition::MustNotExist, body),
            barrier,
        );

        let (left, right) = tokio::join!(left, right);
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Execution::Committed(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Execution::AlreadyCompleted(_)))
                .count(),
            1
        );

        let current = publisher
            .get(&StyleObjectPath::try_new("styles/demo/basic/style.json").unwrap())
            .await
            .unwrap()
            .unwrap();
        let barrier = Arc::new(Barrier::new(2));
        let replacement = br#"{"version":8,"name":"replacement","sources":{},"layers":[]}"#;
        let left = publish_after_intent_barrier(
            &publisher,
            request(
                "style-concurrent-replacement",
                StylePrecondition::MustMatch(current.version().clone()),
                replacement,
            ),
            Arc::clone(&barrier),
        );
        let right = publish_after_intent_barrier(
            &publisher,
            request(
                "style-concurrent-replacement",
                StylePrecondition::MustMatch(current.version().clone()),
                replacement,
            ),
            barrier,
        );

        let (left, right) = tokio::join!(left, right);
        let outcomes = [left.unwrap(), right.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Execution::Committed(_)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Execution::AlreadyCompleted(_)))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn replacement_requires_the_current_style_version() {
        let publisher = publisher();
        let first = publisher
            .publish(request(
                "style-old",
                StylePrecondition::MustNotExist,
                br#"{"version":8,"name":"old","sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap();
        let Execution::Committed(first) = first else {
            panic!("first publication unexpectedly reported a retry");
        };
        publisher
            .publish(request(
                "style-new",
                StylePrecondition::MustMatch(first.version().clone()),
                br#"{"version":8,"name":"new","sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap();

        let stale = publisher
            .publish(request(
                "style-stale",
                StylePrecondition::MustMatch(first.version().clone()),
                br#"{"version":8,"name":"stale","sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap_err();
        assert!(format!("{stale:#}").contains("style replacement precondition did not match"));

        let location = publisher
            .state
            .location("styles/demo/basic/style.json")
            .unwrap();
        let stored = publisher.state.get(&location).await.unwrap();
        assert_eq!(
            stored.bytes().await.unwrap(),
            br#"{"version":8,"name":"new","sources":{},"layers":[]}"#.as_slice()
        );
    }

    #[tokio::test]
    async fn url_constructor_keeps_state_and_journal_independent() {
        let publisher = StylePublisher::from_urls(
            &Url::parse("memory:///state").unwrap(),
            &Url::parse("memory:///audit").unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();
        let body = br#"{"version":8,"sources":{},"layers":[]}"#;
        publisher
            .publish(request("style-url", StylePrecondition::MustNotExist, body))
            .await
            .unwrap();
        let retry = publisher
            .publish(request("style-url", StylePrecondition::MustNotExist, body))
            .await
            .unwrap();
        assert!(matches!(retry, Execution::AlreadyCompleted(_)));
    }

    #[test]
    fn url_constructor_rejects_one_remote_authority_for_state_and_journal() {
        let Err(error) = StylePublisher::from_urls(
            &Url::parse("gs://shared-bucket/state").unwrap(),
            &Url::parse("gs://shared-bucket/audit").unwrap(),
            Vec::<(String, String)>::new(),
        ) else {
            panic!("one remote authority unexpectedly accepted both trust domains");
        };
        assert!(format!("{error:#}").contains("different object-store authorities"));
    }

    #[tokio::test]
    async fn state_store_contains_no_mutation_journal_objects() {
        let state = Arc::new(InMemory::new());
        let journal = Arc::new(InMemory::new());
        let publisher = StylePublisher::from_object_stores(
            state.clone(),
            ObjectPath::from("state"),
            journal.clone(),
            ObjectPath::from("audit"),
        );
        publisher
            .publish(request(
                "isolated-journal",
                StylePrecondition::MustNotExist,
                br#"{"version":8,"sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap();

        let digest = mutation_key_sha256("isolated-journal").unwrap();
        let intent = ObjectPath::from(format!(
            "audit/journal/example/mutations/{digest}/intent.json"
        ));
        let completion = ObjectPath::from(format!(
            "audit/journal/example/mutations/{digest}/completion.json"
        ));
        assert!(journal.head(&intent).await.is_ok());
        assert!(journal.head(&completion).await.is_ok());
        assert!(matches!(
            state.head(&intent).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(
            state
                .head(&ObjectPath::from("state/styles/demo/basic/style.json"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn resource_commit_recovers_before_journal_completion() {
        let publisher = publisher();
        let initial = request(
            "style-3",
            StylePrecondition::MustNotExist,
            br#"{"version":8,"name":"recover","sources":{},"layers":[]}"#,
        );
        let mutation = mutation_for(&initial);
        let location = initial.location.clone();
        let precondition = initial.precondition.clone();
        let body = initial.body.clone();
        let content_sha256 = initial.content_sha256.clone();
        let publisher_ref = &publisher;
        let interrupted: anyhow::Result<Execution<PublishedStyle>> = publisher_ref
            .journal
            .execute(mutation, |intent| {
                let mutation_reference = intent.state_reference().to_string();
                async move {
                    publisher_ref
                        .commit_style(
                            &mutation_reference,
                            &location,
                            &precondition,
                            body,
                            &content_sha256,
                        )
                        .await?;
                    anyhow::bail!("simulated completion interruption")
                }
            })
            .await;
        assert!(
            format!("{:#}", interrupted.unwrap_err()).contains("simulated completion interruption")
        );

        let object_location = publisher
            .state
            .location("styles/demo/basic/style.json")
            .unwrap();
        let stored = publisher.state.get(&object_location).await.unwrap();
        let stored_version =
            VersionEvidence::try_new(stored.meta.e_tag, stored.meta.version).unwrap();

        let recovered = publisher
            .publish(request(
                "style-3",
                StylePrecondition::MustNotExist,
                br#"{"version":8,"name":"recover","sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap();
        let Execution::Committed(recovered) = recovered else {
            panic!("missing completion was not recovered");
        };
        assert_eq!(
            stored_version.as_update_version(),
            recovered.version().as_update_version()
        );
    }

    #[tokio::test]
    async fn reconciliation_completes_state_committed_without_a_journal_completion() {
        let publisher = publisher();
        let initial = request(
            "style-background-recovery",
            StylePrecondition::MustNotExist,
            br#"{"version":8,"name":"recover","sources":{},"layers":[]}"#,
        );
        let mutation = mutation_for(&initial);
        let location = initial.location.clone();
        let precondition = initial.precondition.clone();
        let body = initial.body.clone();
        let content_sha256 = initial.content_sha256.clone();
        let publisher_ref = &publisher;
        let interrupted: anyhow::Result<Execution<PublishedStyle>> = publisher
            .journal
            .execute(mutation, |intent| {
                let mutation_reference = intent.state_reference().to_string();
                async move {
                    publisher_ref
                        .commit_style(
                            &mutation_reference,
                            &location,
                            &precondition,
                            body,
                            &content_sha256,
                        )
                        .await?;
                    anyhow::bail!("simulated completion interruption")
                }
            })
            .await;
        assert!(interrupted.is_err());

        // Recovery follows the path persisted with the intent, not a later
        // catalog snapshot that may have removed or remapped this style.
        let report = publisher
            .reconcile_unfinished(&StyleCatalog::default())
            .await
            .unwrap();
        assert_eq!(report.unfinished_intents, 1);
        assert_eq!(report.completed, 1);
        assert_eq!(
            publisher
                .reconcile_unfinished(&StyleCatalog::default())
                .await
                .unwrap()
                .unfinished_intents,
            0
        );

        let retry = publisher
            .publish(request(
                "style-background-recovery",
                StylePrecondition::MustNotExist,
                br#"{"version":8,"name":"recover","sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap();
        assert!(matches!(retry, Execution::AlreadyCompleted(_)));
    }

    #[tokio::test]
    async fn reconciliation_never_replays_state_that_was_not_committed() {
        let publisher = publisher();
        let initial = request(
            "style-no-state",
            StylePrecondition::MustNotExist,
            br#"{"version":8,"name":"later","sources":{},"layers":[]}"#,
        );
        let mutation = mutation_for(&initial);
        let interrupted: anyhow::Result<Execution<PublishedStyle>> = publisher
            .journal
            .execute(mutation, |_| async {
                anyhow::bail!("simulated failure before state")
            })
            .await;
        assert!(interrupted.is_err());

        let report = publisher.reconcile_unfinished(&catalog()).await.unwrap();
        assert_eq!(report.unfinished_intents, 1);
        assert_eq!(report.not_committed, 1);
        assert!(
            publisher
                .get(&StyleObjectPath::try_new("styles/demo/basic/style.json").unwrap())
                .await
                .unwrap()
                .is_none()
        );

        let retry = publisher
            .publish(request(
                "style-no-state",
                StylePrecondition::MustNotExist,
                br#"{"version":8,"name":"later","sources":{},"layers":[]}"#,
            ))
            .await
            .unwrap();
        assert!(matches!(retry, Execution::Committed(_)));
    }

    #[tokio::test]
    async fn incomplete_retry_does_not_overwrite_a_newer_mutation() {
        let publisher = publisher();
        let old_body = br#"{"version":8,"name":"old","sources":{},"layers":[]}"#;
        let initial = request(
            "style-incomplete",
            StylePrecondition::MustNotExist,
            old_body,
        );
        let mutation = mutation_for(&initial);
        let location = initial.location;
        let precondition = initial.precondition;
        let body = initial.body;
        let content_sha256 = initial.content_sha256;
        let publisher_ref = &publisher;
        let interrupted: anyhow::Result<Execution<PublishedStyle>> = publisher_ref
            .journal
            .execute(mutation, |intent| {
                let mutation_reference = intent.state_reference().to_string();
                async move {
                    publisher_ref
                        .commit_style(
                            &mutation_reference,
                            &location,
                            &precondition,
                            body,
                            &content_sha256,
                        )
                        .await?;
                    anyhow::bail!("simulated completion interruption")
                }
            })
            .await;
        assert!(interrupted.is_err());

        let object_location = publisher
            .state
            .location("styles/demo/basic/style.json")
            .unwrap();
        let current = publisher.state.get(&object_location).await.unwrap();
        let current_version =
            VersionEvidence::try_new(current.meta.e_tag, current.meta.version).unwrap();
        let new_body = br#"{"version":8,"name":"new","sources":{},"layers":[]}"#;
        publisher
            .publish(request(
                "style-superseding",
                StylePrecondition::MustMatch(current_version),
                new_body,
            ))
            .await
            .unwrap();

        let report = publisher.reconcile_unfinished(&catalog()).await.unwrap();
        assert_eq!(report.unfinished_intents, 1);
        assert_eq!(report.superseded, 1);
        assert_eq!(report.completed, 0);

        let error = publisher
            .publish(request(
                "style-incomplete",
                StylePrecondition::MustNotExist,
                old_body,
            ))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("style already exists"));
        assert_eq!(
            publisher
                .state
                .get(&object_location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            new_body.as_slice()
        );
    }

    #[test]
    fn rejects_invalid_or_oversized_styles() {
        for body in [
            Bytes::from_static(b""),
            Bytes::from_static(b"[]"),
            Bytes::from_static(br#"{"version":7}"#),
            Bytes::from_static(b"not-json"),
        ] {
            assert!(
                PublishStyleRequest::try_new(
                    "style-invalid",
                    actor(),
                    AccountId::try_new("example").unwrap(),
                    LocalResourceId::try_new("basic").unwrap(),
                    StyleObjectPath::try_new("styles/default/basic/style.json").unwrap(),
                    StylePrecondition::MustNotExist,
                    body,
                    RequestId::new_random(),
                )
                .is_err()
            );
        }

        let oversized = Bytes::from(vec![b' '; MAX_STYLE_BYTES + 1]);
        assert!(
            PublishStyleRequest::try_new(
                "style-oversized",
                actor(),
                AccountId::try_new("example").unwrap(),
                LocalResourceId::try_new("basic").unwrap(),
                StyleObjectPath::try_new("styles/default/basic/style.json").unwrap(),
                StylePrecondition::MustNotExist,
                oversized,
                RequestId::new_random(),
            )
            .is_err()
        );
    }

    #[test]
    fn style_object_path_stays_under_style_documents() {
        for (object_path, expected_id) in [
            ("styles/demo/basic/style.json", "demo/basic"),
            ("styles/demo/basic.json", "demo/basic"),
        ] {
            let path = StyleObjectPath::try_new(object_path).unwrap();
            assert_eq!(path.as_ref(), object_path);
            assert_eq!(path.delivery_style_id(), expected_id);
        }
        for invalid in [
            "basic/style.json",
            "styles/style.json",
            "styles/basic/sprite.png",
            "styles/demo/team/basic/style.json",
            "styles/demo/team/basic.json",
            "../styles/default/basic/style.json",
        ] {
            assert!(StyleObjectPath::try_new(invalid).is_err(), "{invalid}");
        }
    }

    #[tokio::test]
    async fn idempotency_key_cannot_move_the_style_object() {
        let publisher = publisher();
        let body = br#"{"version":8,"sources":{},"layers":[]}"#;
        publisher
            .publish(request("style-4", StylePrecondition::MustNotExist, body))
            .await
            .unwrap();

        let moved = PublishStyleRequest::try_new(
            "style-4",
            actor(),
            AccountId::try_new("example").unwrap(),
            LocalResourceId::try_new("basic").unwrap(),
            StyleObjectPath::try_new("styles/other/basic/style.json").unwrap(),
            StylePrecondition::MustNotExist,
            Bytes::from_static(body),
            RequestId::new_random(),
        )
        .unwrap();
        let error = publisher.publish(moved).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("idempotency key was reused for a different mutation")
        );
    }
}
