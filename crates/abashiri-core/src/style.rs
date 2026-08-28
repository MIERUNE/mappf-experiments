//! Conditional publication of mutable MapLibre style documents.
//!
//! The HTTP API remains deliberately outside this module. A trusted caller
//! supplies a canonical namespace/style identity and configured physical
//! layout. This module binds the resulting object to the mutation journal,
//! preserves exact style bytes, and recovers idempotently when state committed
//! but the journal completion did not.

use std::{borrow::Cow, collections::BTreeSet, sync::Arc};

use anyhow::{Context as _, ensure};
use bytes::Bytes;
use futures_util::TryStreamExt as _;
use object_store::{Attribute, AttributeValue, Attributes, ObjectStore, path::Path as ObjectPath};
use serde_json::Value;
use thiserror::Error;
use url::Url;

use mmpf_http::{request_id::RequestId, style_key::StyleKey};

use crate::{
    mutation::{
        AccountId, Actor, Execution, LocalResourceId, MutationAction, MutationIntent,
        MutationJournal, MutationRequest, ResourceKind, ResourceTarget, StateCommit,
        VersionEvidence, digest_hex,
    },
    storage::ConditionalStore,
};

pub const MAX_STYLE_BYTES: usize = 2 * 1024 * 1024;
const MAX_INVENTORY_OBJECTS: usize = 10_000;
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

/// Canonical object path for one mutable style document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleObjectPath {
    path: ObjectPath,
    delivery_style_id: String,
    layout: StyleObjectLayout,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StyleObjectLayout {
    #[default]
    Nested,
    Flat,
}

impl StyleObjectPath {
    pub fn for_resource(
        account: &AccountId,
        style: &LocalResourceId,
        layout: StyleObjectLayout,
    ) -> anyhow::Result<Self> {
        StyleKey::from_segments(account.as_str(), style.as_str())
            .context("management account and style must form a canonical style key")?;
        let relative = match layout {
            StyleObjectLayout::Nested => {
                format!("styles/{}/{}/style.json", account.as_str(), style.as_str())
            }
            StyleObjectLayout::Flat => {
                format!("styles/{}/{}.json", account.as_str(), style.as_str())
            }
        };
        Self::try_new(&relative)
    }

    pub fn try_new(relative: &str) -> anyhow::Result<Self> {
        let path = ObjectPath::parse(relative).context("parse style object path")?;
        let style_path = path
            .as_ref()
            .strip_prefix("styles/")
            .context("style object path must be under styles/")?;
        let (delivery_style_id, layout) = if let Some(style_id) =
            style_path.strip_suffix("/style.json")
        {
            (style_id, StyleObjectLayout::Nested)
        } else if let Some(style_id) = style_path.strip_suffix(".json") {
            (style_id, StyleObjectLayout::Flat)
        } else {
            anyhow::bail!(
                "style object path must match styles/{{namespace}}/{{style_id}}/style.json or styles/{{namespace}}/{{style_id}}.json"
            );
        };
        StyleKey::parse(delivery_style_id)
            .context("style object path must contain a canonical style key")?;
        let delivery_style_id = delivery_style_id.to_owned();
        Ok(Self {
            path,
            delivery_style_id,
            layout,
        })
    }

    /// Logical delivery style ID addressed by Biei and Ishikari refresh hints.
    pub fn delivery_style_id(&self) -> &str {
        &self.delivery_style_id
    }

    pub fn layout(&self) -> StyleObjectLayout {
        self.layout
    }
}

impl AsRef<str> for StyleObjectPath {
    fn as_ref(&self) -> &str {
        self.path.as_ref()
    }
}

/// Canonical object path for one published PMTiles archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TilesetObjectPath {
    path: ObjectPath,
    delivery_tileset_id: String,
}

impl TilesetObjectPath {
    pub fn try_new(relative: &str) -> anyhow::Result<Self> {
        let path = ObjectPath::parse(relative).context("parse tileset object path")?;
        let delivery_tileset_id = path
            .as_ref()
            .strip_prefix("tilesets/")
            .and_then(|value| value.strip_suffix(".pmtiles"))
            .context(
                "tileset object path must match tilesets/{tileset_id}.pmtiles or tilesets/{namespace}/{tileset_id}.pmtiles",
            )?;
        ensure!(
            is_delivery_tileset_id(delivery_tileset_id),
            "tileset object path must contain a canonical delivery tileset ID"
        );
        let delivery_tileset_id = delivery_tileset_id.to_owned();
        Ok(Self {
            path,
            delivery_tileset_id,
        })
    }

    pub fn delivery_tileset_id(&self) -> &str {
        &self.delivery_tileset_id
    }
}

impl AsRef<str> for TilesetObjectPath {
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

/// One canonical PMTiles archive discovered below the published-state root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedTileset {
    tileset_id: String,
    size_bytes: u64,
    updated_at: String,
}

struct StateInventoryObject {
    relative: String,
    size_bytes: u64,
    updated_at: String,
}

/// One canonical style document discovered below the published-state root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedStyleResource {
    delivery_style_id: String,
    size_bytes: u64,
    updated_at: String,
}

impl PublishedStyleResource {
    pub fn delivery_style_id(&self) -> &str {
        &self.delivery_style_id
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn updated_at(&self) -> &str {
        &self.updated_at
    }
}

impl PublishedTileset {
    pub fn tileset_id(&self) -> &str {
        &self.tileset_id
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub fn updated_at(&self) -> &str {
        &self.updated_at
    }
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

    /// Lists canonical delivery tilesets without exposing storage locations.
    pub async fn list_tilesets(&self) -> anyhow::Result<Vec<PublishedTileset>> {
        self.list_tilesets_below("tilesets").await
    }

    /// Lists canonical delivery tilesets within one validated namespace.
    pub async fn list_tilesets_in_namespace(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<PublishedTileset>> {
        validate_namespace(namespace)?;
        let prefix = format!("tilesets/{namespace}");
        self.list_tilesets_below(&prefix).await
    }

    async fn list_tilesets_below(&self, prefix: &str) -> anyhow::Result<Vec<PublishedTileset>> {
        let mut tilesets = Vec::new();
        for object in self.list_state_prefix(prefix).await? {
            let relative = format!("{prefix}/{}", object.relative);
            let Ok(location) = TilesetObjectPath::try_new(&relative) else {
                continue;
            };
            tilesets.push(PublishedTileset {
                tileset_id: location.delivery_tileset_id().to_owned(),
                size_bytes: object.size_bytes,
                updated_at: object.updated_at,
            });
        }
        tilesets.sort_unstable_by(|left, right| left.tileset_id.cmp(&right.tileset_id));
        Ok(tilesets)
    }

    /// Lists canonical delivery style documents without exposing storage locations.
    pub async fn list_styles(
        &self,
        layout: StyleObjectLayout,
    ) -> anyhow::Result<Vec<PublishedStyleResource>> {
        self.list_styles_below("styles", layout).await
    }

    /// Lists canonical style documents within one validated namespace.
    pub async fn list_styles_in_namespace(
        &self,
        namespace: &str,
        layout: StyleObjectLayout,
    ) -> anyhow::Result<Vec<PublishedStyleResource>> {
        validate_namespace(namespace)?;
        let prefix = format!("styles/{namespace}");
        self.list_styles_below(&prefix, layout).await
    }

    async fn list_styles_below(
        &self,
        prefix: &str,
        layout: StyleObjectLayout,
    ) -> anyhow::Result<Vec<PublishedStyleResource>> {
        let mut styles = Vec::new();
        for object in self.list_state_prefix(prefix).await? {
            let relative = format!("{prefix}/{}", object.relative);
            let Ok(location) = StyleObjectPath::try_new(&relative) else {
                continue;
            };
            if location.layout() != layout {
                continue;
            }
            styles.push(PublishedStyleResource {
                delivery_style_id: location.delivery_style_id().to_owned(),
                size_bytes: object.size_bytes,
                updated_at: object.updated_at,
            });
        }
        styles.sort_unstable_by(|left, right| left.delivery_style_id.cmp(&right.delivery_style_id));
        Ok(styles)
    }

    /// Discovers only the first namespace segment below style and tileset roots.
    pub async fn list_namespaces(&self) -> anyhow::Result<Vec<String>> {
        let mut namespaces = BTreeSet::new();
        for kind in ["styles", "tilesets"] {
            let prefix = self.state.location(kind)?;
            let prefix_with_separator = format!("{}/", prefix.as_ref());
            let result = tokio::time::timeout(
                crate::store_policy::OPERATION_TIMEOUT,
                self.state.list_with_delimiter(&prefix),
            )
            .await
            .context("namespace inventory timed out")?
            .context("list resource namespaces")?;
            for common_prefix in result.common_prefixes {
                let Some(namespace) = common_prefix.as_ref().strip_prefix(&prefix_with_separator)
                else {
                    continue;
                };
                let namespace = namespace.trim_end_matches('/');
                if namespace.contains('/') {
                    continue;
                }
                if validate_namespace(namespace).is_ok() && !namespaces.contains(namespace) {
                    ensure!(
                        namespaces.len() < MAX_INVENTORY_OBJECTS,
                        "resource inventory exceeds {MAX_INVENTORY_OBJECTS} namespaces"
                    );
                    namespaces.insert(namespace.to_owned());
                }
            }
        }
        Ok(namespaces.into_iter().collect())
    }

    async fn list_state_prefix(&self, prefix: &str) -> anyhow::Result<Vec<StateInventoryObject>> {
        let location = self.state.location(prefix)?;
        let location_prefix = format!("{}/", location.as_ref());
        let mut objects = self.state.list(&location);
        tokio::time::timeout(crate::store_policy::OPERATION_TIMEOUT, async {
            let mut inventory = Vec::new();
            while let Some(object) = objects.try_next().await.context("list published state")? {
                ensure!(
                    inventory.len() < MAX_INVENTORY_OBJECTS,
                    "resource inventory exceeds {MAX_INVENTORY_OBJECTS} objects below one prefix"
                );
                let Some(relative) = object.location.as_ref().strip_prefix(&location_prefix) else {
                    continue;
                };
                inventory.push(StateInventoryObject {
                    relative: relative.to_owned(),
                    size_bytes: object.size,
                    updated_at: object.last_modified.to_rfc3339(),
                });
            }
            anyhow::Ok(inventory)
        })
        .await
        .context("resource inventory timed out")?
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
    /// The scan never replays a state mutation. An absent object or state naming
    /// a newer intent is retained as an unfinished audit attempt.
    pub async fn reconcile_unfinished(
        &self,
        layout: StyleObjectLayout,
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
            match self.reconcile_style_intent(layout, &intent).await? {
                StyleReconciliationOutcome::Completed => report.completed += 1,
                StyleReconciliationOutcome::AlreadyCompleted => report.already_completed += 1,
                StyleReconciliationOutcome::NotCommitted => report.not_committed += 1,
                StyleReconciliationOutcome::Superseded => report.superseded += 1,
                StyleReconciliationOutcome::Unsupported => report.unsupported += 1,
            }
        }
        Ok(report)
    }

    async fn reconcile_style_intent(
        &self,
        layout: StyleObjectLayout,
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
        let derived_location;
        let location = if let Some(location) = persisted_location.as_ref() {
            location
        } else {
            derived_location = StyleObjectPath::for_resource(
                intent.target().account_id(),
                intent.target().local_id(),
                layout,
            )?;
            &derived_location
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

fn validate_namespace(namespace: &str) -> anyhow::Result<()> {
    StyleKey::from_segments(namespace, "resource")
        .context("resource namespace must use the canonical style namespace grammar")?;
    Ok(())
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

fn is_delivery_tileset_id(value: &str) -> bool {
    if value.is_empty() || value.len() > 256 {
        return false;
    }
    let mut segments = value.split('/');
    let first = segments.next().unwrap_or_default();
    let second = segments.next();
    if segments.next().is_some() || !is_tileset_segment(first) {
        return false;
    }
    second.is_none_or(|segment| {
        is_tileset_segment(segment) && !matches!(segment, "preview" | "preview.json")
    })
}

fn is_tileset_segment(segment: &str) -> bool {
    !segment.is_empty()
        && !matches!(segment, "." | "..")
        && segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
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

    #[tokio::test]
    async fn inventory_includes_only_canonical_delivery_resources() {
        let state = Arc::new(InMemory::new());
        for path in [
            "state/tilesets/mierune/omt.pmtiles",
            "state/tilesets/weather/rain.pmtiles",
            "state/tilesets/flat.pmtiles",
            "state/tilesets/team/nested/archive.pmtiles",
            "state/tilesets/weather/preview.pmtiles",
            "state/tilesets/weather/readme.json",
            "state/styles/carto/positron/style.json",
            "state/styles/carto/positron.json",
            "state/styles/mierune/streets.json",
            "state/styles/carto/positron/sprite.json",
            "state/styles/team/nested/extra/style.json",
        ] {
            state
                .put(
                    &ObjectPath::from(path),
                    Bytes::from_static(b"archive").into(),
                )
                .await
                .unwrap();
        }
        let publisher = StylePublisher::from_object_stores(
            state,
            ObjectPath::from("state"),
            Arc::new(InMemory::new()),
            ObjectPath::from("audit"),
        );

        let inventory = publisher.list_tilesets().await.unwrap();
        assert_eq!(
            inventory
                .iter()
                .map(PublishedTileset::tileset_id)
                .collect::<Vec<_>>(),
            ["flat", "mierune/omt", "weather/rain"]
        );
        assert!(inventory.iter().all(|tileset| tileset.size_bytes() == 7));
        assert!(
            inventory
                .iter()
                .all(|tileset| !tileset.updated_at().is_empty())
        );
        assert_eq!(
            publisher.list_namespaces().await.unwrap(),
            ["carto", "mierune", "team", "weather"]
        );
        assert_eq!(
            publisher
                .list_tilesets_in_namespace("mierune")
                .await
                .unwrap()
                .iter()
                .map(PublishedTileset::tileset_id)
                .collect::<Vec<_>>(),
            ["mierune/omt"]
        );

        let styles = publisher
            .list_styles(StyleObjectLayout::Nested)
            .await
            .unwrap();
        assert_eq!(
            styles
                .iter()
                .map(PublishedStyleResource::delivery_style_id)
                .collect::<Vec<_>>(),
            ["carto/positron"]
        );
        assert!(styles.iter().all(|style| style.size_bytes() == 7));
        assert!(styles.iter().all(|style| !style.updated_at().is_empty()));
        assert_eq!(
            publisher
                .list_styles_in_namespace("carto", StyleObjectLayout::Nested)
                .await
                .unwrap()
                .iter()
                .map(PublishedStyleResource::delivery_style_id)
                .collect::<Vec<_>>(),
            ["carto/positron"]
        );

        let flat_styles = publisher
            .list_styles(StyleObjectLayout::Flat)
            .await
            .unwrap();
        assert_eq!(
            flat_styles
                .iter()
                .map(PublishedStyleResource::delivery_style_id)
                .collect::<Vec<_>>(),
            ["carto/positron", "mierune/streets"]
        );
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

        // Recovery follows the path persisted with the intent rather than
        // re-deriving a possibly different path from current configuration.
        let report = publisher
            .reconcile_unfinished(StyleObjectLayout::Nested)
            .await
            .unwrap();
        assert_eq!(report.unfinished_intents, 1);
        assert_eq!(report.completed, 1);
        assert_eq!(
            publisher
                .reconcile_unfinished(StyleObjectLayout::Nested)
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

        let report = publisher
            .reconcile_unfinished(StyleObjectLayout::Nested)
            .await
            .unwrap();
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

        let report = publisher
            .reconcile_unfinished(StyleObjectLayout::Nested)
            .await
            .unwrap();
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
        let account = AccountId::try_new("demo").unwrap();
        let style = LocalResourceId::try_new("basic").unwrap();
        assert_eq!(
            StyleObjectPath::for_resource(&account, &style, StyleObjectLayout::Nested)
                .unwrap()
                .as_ref(),
            "styles/demo/basic/style.json"
        );
        assert_eq!(
            StyleObjectPath::for_resource(&account, &style, StyleObjectLayout::Flat)
                .unwrap()
                .as_ref(),
            "styles/demo/basic.json"
        );
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
