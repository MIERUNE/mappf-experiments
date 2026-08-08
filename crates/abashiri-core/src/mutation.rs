//! Durable mutation journal used by every future Abashiri writer route.
//!
//! The journal makes an immutable intent durable before invoking a route's
//! state commit, and makes an immutable completion durable before returning
//! success. A route-specific commit must itself be idempotent: resource state
//! records the intent's opaque `state_reference`, not its mutation key, so
//! retrying after "state committed, completion write failed" recognizes the
//! existing commit instead of applying it twice. The reference is deliberately
//! opaque — deriving it from the idempotency key would put a caller-chosen secret
//! into durable resource state, which the digest-only persistence rule below
//! exists to prevent.

use std::{
    future::Future,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use object_store::{
    Error as ObjectStoreError, ObjectMeta, ObjectStore, PutResult, UpdateVersion,
    path::Path as ObjectPath,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use mmpf_http::request_id::RequestId;

use crate::storage::ConditionalStore;

const SCHEMA_VERSION: u32 = 2;
const MAX_ACCOUNT_ID_LEN: usize = 64;
const MAX_LOCAL_ID_LEN: usize = 128;
const MAX_PRINCIPAL_PART_LEN: usize = 256;
const MAX_VERSION_PART_LEN: usize = 1_024;
const VERSION_ETAG_PREFIX: &str = "abashiri-v1.";

/// Invalid bounded identity supplied to the management domain.
#[derive(Debug, Error, Eq, PartialEq)]
#[error("{field} {reason}")]
pub struct IdentityError {
    field: &'static str,
    reason: &'static str,
}

/// A durable idempotency key was reused for a different mutation.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("idempotency key was reused for a different mutation")]
pub struct IdempotencyConflict;

/// Management account identity.
///
/// A client-supplied account is an assertion to check against the authenticated
/// principal, not the authorization scope itself.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct AccountId(String);

impl AccountId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        validate_path_id("account_id", &value, MAX_ACCOUNT_ID_LEN)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Account-scoped logical identifier used by management resources.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct LocalResourceId(String);

impl LocalResourceId {
    pub fn try_new(value: impl Into<String>) -> Result<Self, IdentityError> {
        let value = value.into();
        validate_path_id("local resource id", &value, MAX_LOCAL_ID_LEN)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Principal kind recorded in durable management audit entries.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    Workload,
}

/// Verified principal responsible for one management mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Actor {
    kind: ActorKind,
    issuer: String,
    subject: String,
}

impl Actor {
    pub fn try_new(
        kind: ActorKind,
        issuer: impl Into<String>,
        subject: impl Into<String>,
    ) -> Result<Self, IdentityError> {
        let issuer = issuer.into();
        let subject = subject.into();
        validate_bounded_text("actor issuer", &issuer, MAX_PRINCIPAL_PART_LEN)?;
        validate_bounded_text("actor subject", &subject, MAX_PRINCIPAL_PART_LEN)?;
        Ok(Self {
            kind,
            issuer,
            subject,
        })
    }

    pub fn kind(&self) -> ActorKind {
        self.kind
    }

    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// Resource family addressed by an Abashiri mutation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Style,
    Sprite,
    Tileset,
    Token,
}

/// Stable account-qualified mutation target.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceTarget {
    account_id: AccountId,
    kind: ResourceKind,
    local_id: LocalResourceId,
}

impl ResourceTarget {
    pub fn new(account_id: AccountId, kind: ResourceKind, local_id: LocalResourceId) -> Self {
        Self {
            account_id,
            kind,
            local_id,
        }
    }

    pub fn account_id(&self) -> &AccountId {
        &self.account_id
    }
}

/// Coarse operation recorded independently from its resource family.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationAction {
    Create,
    Update,
    Delete,
    Publish,
}

/// Stable mutation identity. The raw idempotency key and request body are never
/// persisted; only domain-separated SHA-256 digests are stored.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MutationRequest {
    key_sha256: String,
    actor: Actor,
    action: MutationAction,
    target: ResourceTarget,
    input_sha256: String,
    request_id: RequestId,
}

impl MutationRequest {
    pub fn try_new(
        idempotency_key: &str,
        actor: Actor,
        action: MutationAction,
        target: ResourceTarget,
        canonical_input: &[u8],
        request_id: RequestId,
    ) -> Result<Self, IdentityError> {
        // Reuse the bounded RFC-token policy already applied to request IDs,
        // while keeping this as a distinct domain value and persisting only its
        // digest.
        let key_sha256 = mutation_key_sha256(idempotency_key)?;
        Ok(Self {
            key_sha256,
            actor,
            action,
            target,
            input_sha256: digest_hex(b"abashiri-mutation-input-v1\0", canonical_input),
            request_id,
        })
    }
}

/// Returns the stable, non-secret mutation identifier used by the journal and
/// advisory refresh hints.
pub fn mutation_key_sha256(idempotency_key: &str) -> Result<String, IdentityError> {
    RequestId::try_new(idempotency_key).map_err(|_| IdentityError {
        field: "idempotency key",
        reason: "must be a 1..=128 byte RFC 7230 token",
    })?;
    Ok(digest_hex(
        b"abashiri-idempotency-key-v1\0",
        idempotency_key.as_bytes(),
    ))
}

/// Persisted immutable record written before state mutation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MutationIntent {
    schema_version: u32,
    key_sha256: String,
    state_reference: RequestId,
    actor: Actor,
    action: MutationAction,
    target: ResourceTarget,
    input_sha256: String,
    first_request_id: RequestId,
    created_at_unix_ms: u64,
}

impl MutationIntent {
    /// Opaque server-generated value persisted with committed resource state.
    pub fn state_reference(&self) -> &str {
        self.state_reference.as_str()
    }

    pub fn target(&self) -> &ResourceTarget {
        &self.target
    }

    fn matches_request(&self, request: &MutationRequest) -> bool {
        self.schema_version == SCHEMA_VERSION
            && self.key_sha256 == request.key_sha256
            && self.actor == request.actor
            && self.action == request.action
            && self.target == request.target
            && self.input_sha256 == request.input_sha256
    }
}

/// Object version proving which state generation a mutation committed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct VersionEvidence {
    e_tag: Option<String>,
    version: Option<String>,
}

impl VersionEvidence {
    pub fn try_new(e_tag: Option<String>, version: Option<String>) -> anyhow::Result<Self> {
        let evidence = Self { e_tag, version };
        ensure!(
            evidence.e_tag.is_some() || evidence.version.is_some(),
            "state object has neither an ETag nor a version"
        );
        for value in [&evidence.e_tag, &evidence.version].into_iter().flatten() {
            ensure!(
                !value.is_empty()
                    && value.len() <= MAX_VERSION_PART_LEN
                    && !value.chars().any(char::is_control),
                "state object returned invalid version evidence"
            );
        }
        Ok(evidence)
    }

    pub fn from_meta(meta: &ObjectMeta) -> anyhow::Result<Self> {
        Self::try_new(meta.e_tag.clone(), meta.version.clone())
            .context("state object returned invalid version evidence")
    }

    pub fn try_from_put(result: PutResult) -> anyhow::Result<Self> {
        Self::try_new(result.e_tag, result.version)
            .context("state write returned unusable version evidence")
    }

    pub fn as_update_version(&self) -> UpdateVersion {
        UpdateVersion {
            e_tag: self.e_tag.clone(),
            version: self.version.clone(),
        }
    }

    /// Encodes the complete backend validator as one opaque strong HTTP ETag.
    ///
    /// Carrying both fields matters for GCS, whose conditional updates require
    /// the object generation rather than its ordinary ETag.
    pub fn to_entity_tag(&self) -> anyhow::Result<String> {
        let body = serde_json::to_vec(self).context("encode version evidence")?;
        Ok(format!(
            "\"{VERSION_ETAG_PREFIX}{}\"",
            URL_SAFE_NO_PAD.encode(body)
        ))
    }

    /// Decodes an ETag previously returned by [`Self::to_entity_tag`].
    pub fn from_entity_tag(value: &str) -> anyhow::Result<Self> {
        ensure!(
            value.len() <= 4 * MAX_VERSION_PART_LEN,
            "version ETag is too long"
        );
        let encoded = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .and_then(|value| value.strip_prefix(VERSION_ETAG_PREFIX))
            .context("If-Match must contain one Abashiri version ETag")?;
        let body = URL_SAFE_NO_PAD
            .decode(encoded)
            .context("decode Abashiri version ETag")?;
        let evidence: Self =
            serde_json::from_slice(&body).context("parse Abashiri version ETag")?;
        Self::try_new(evidence.e_tag, evidence.version)
    }
}

/// Result returned by a route-specific idempotent state commit.
///
/// `response` is returned to the caller only for the execution that durably
/// records the completion. It is never serialized into the audit journal,
/// because it may contain a one-time credential. `state_identity` should be a
/// canonical, non-secret representation of the committed state.
pub struct StateCommit<T> {
    version: VersionEvidence,
    response: T,
    state_sha256: String,
}

impl<T> StateCommit<T> {
    pub fn new(version: VersionEvidence, response: T, state_identity: &[u8]) -> Self {
        Self {
            version,
            response,
            state_sha256: digest_hex(b"abashiri-committed-state-v1\0", state_identity),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct MutationCompletion {
    schema_version: u32,
    key_sha256: String,
    state_version: VersionEvidence,
    state_sha256: String,
    completed_at_unix_ms: u64,
}

impl MutationCompletion {
    fn same_result(&self, other: &Self) -> bool {
        self.schema_version == other.schema_version
            && self.key_sha256 == other.key_sha256
            && self.state_version == other.state_version
            && self.state_sha256 == other.state_sha256
    }
}

/// Redacted proof returned when an idempotency key already completed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedMutation {
    state_version: VersionEvidence,
    state_sha256: String,
}

impl CompletedMutation {
    pub fn state_version(&self) -> &VersionEvidence {
        &self.state_version
    }
}

/// Whether this call committed now or observed a prior durable completion.
#[derive(Debug)]
pub enum Execution<T> {
    Committed(T),
    AlreadyCompleted(CompletedMutation),
}

/// Durable mutation journal rooted at an object-store prefix.
pub struct MutationJournal {
    store: ConditionalStore,
}

impl MutationJournal {
    pub fn from_url<I, K, V>(root: &Url, options: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        Ok(Self {
            store: ConditionalStore::from_url(root, options)?,
        })
    }

    pub fn from_object_store(store: Arc<dyn ObjectStore>, root: ObjectPath) -> Self {
        Self {
            store: ConditionalStore::from_store(store, root),
        }
    }

    /// Execute one idempotent mutation.
    ///
    /// `commit` may run again after a partial failure or concurrent retry. It
    /// must write state that references `intent.state_reference()` and return the
    /// existing result when that same key already committed. Intent existence
    /// alone is not evidence that resource state was written; the resource
    /// writer must decide from its own conditional state.
    pub async fn execute<T, F, Fut>(
        &self,
        request: MutationRequest,
        commit: F,
    ) -> anyhow::Result<Execution<T>>
    where
        F: FnOnce(&MutationIntent) -> Fut,
        Fut: Future<Output = anyhow::Result<StateCommit<T>>>,
    {
        let paths = MutationPaths::new(&self.store, &request)?;
        let intent = self.ensure_intent(&paths.intent, request).await?;

        if let Some(completion) = self
            .read_optional::<MutationCompletion>(&paths.completion)
            .await?
        {
            ensure!(
                completion.schema_version == SCHEMA_VERSION
                    && completion.key_sha256 == intent.key_sha256,
                "stored mutation completion does not match its intent"
            );
            return Ok(Execution::AlreadyCompleted(CompletedMutation {
                state_version: completion.state_version,
                state_sha256: completion.state_sha256,
            }));
        }

        let committed = commit(&intent).await.context("commit mutation state")?;
        let completion = MutationCompletion {
            schema_version: SCHEMA_VERSION,
            key_sha256: intent.key_sha256.clone(),
            state_version: committed.version,
            state_sha256: committed.state_sha256,
            completed_at_unix_ms: unix_time_ms()?,
        };
        let body = serde_json::to_vec(&completion).context("encode mutation completion")?;

        match self
            .store
            .create(&paths.completion, Bytes::from(body))
            .await
        {
            Ok(_) => Ok(Execution::Committed(committed.response)),
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                let existing = self
                    .read_required::<MutationCompletion>(&paths.completion)
                    .await?;
                ensure!(
                    existing.same_result(&completion),
                    "concurrent mutation completion disagrees with committed result"
                );
                Ok(Execution::AlreadyCompleted(CompletedMutation {
                    state_version: existing.state_version,
                    state_sha256: existing.state_sha256,
                }))
            }
            Err(error) => Err(error).context("create mutation completion"),
        }
    }

    async fn ensure_intent(
        &self,
        location: &ObjectPath,
        request: MutationRequest,
    ) -> anyhow::Result<MutationIntent> {
        let intent = MutationIntent {
            schema_version: SCHEMA_VERSION,
            key_sha256: request.key_sha256.clone(),
            state_reference: RequestId::new_random(),
            actor: request.actor.clone(),
            action: request.action,
            target: request.target.clone(),
            input_sha256: request.input_sha256.clone(),
            first_request_id: request.request_id.clone(),
            created_at_unix_ms: unix_time_ms()?,
        };
        let body = serde_json::to_vec(&intent).context("encode mutation intent")?;

        match self.store.create(location, Bytes::from(body)).await {
            Ok(_) => Ok(intent),
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                let existing = self.read_required::<MutationIntent>(location).await?;
                if !existing.matches_request(&request) {
                    return Err(IdempotencyConflict.into());
                }
                Ok(existing)
            }
            Err(error) => Err(error).context("create mutation intent"),
        }
    }

    async fn read_optional<T: DeserializeOwned>(
        &self,
        location: &ObjectPath,
    ) -> anyhow::Result<Option<T>> {
        match self.store.read(location).await {
            Ok(body) => serde_json::from_slice(&body)
                .context("decode mutation journal object")
                .map(Some),
            Err(ObjectStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error).context("read mutation journal object"),
        }
    }

    async fn read_required<T: DeserializeOwned>(&self, location: &ObjectPath) -> anyhow::Result<T> {
        self.read_optional(location)
            .await?
            .context("mutation journal object disappeared")
    }
}

struct MutationPaths {
    intent: ObjectPath,
    completion: ObjectPath,
}

impl MutationPaths {
    fn new(store: &ConditionalStore, request: &MutationRequest) -> anyhow::Result<Self> {
        let base = format!(
            "journal/{}/mutations/{}",
            request.target.account_id.as_str(),
            request.key_sha256
        );
        Ok(Self {
            intent: store.location(&format!("{base}/intent.json"))?,
            completion: store.location(&format!("{base}/completion.json"))?,
        })
    }
}

fn validate_path_id(field: &'static str, value: &str, max_len: usize) -> Result<(), IdentityError> {
    if value.is_empty() || value.len() > max_len {
        return Err(IdentityError {
            field,
            reason: "has an invalid length",
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(IdentityError {
            field,
            reason: "must contain only ASCII letters, digits, '-' or '_'",
        });
    }
    Ok(())
}

fn validate_bounded_text(
    field: &'static str,
    value: &str,
    max_len: usize,
) -> Result<(), IdentityError> {
    if value.is_empty() || value.len() > max_len {
        return Err(IdentityError {
            field,
            reason: "has an invalid length",
        });
    }
    if value.chars().any(char::is_control) {
        return Err(IdentityError {
            field,
            reason: "must not contain control characters",
        });
    }
    Ok(())
}

pub(crate) fn digest_hex(domain: &[u8], value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::new()
        .chain_update(domain)
        .chain_update(value)
        .finalize();
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn unix_time_ms() -> anyhow::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_millis();
    u64::try_from(millis).context("Unix timestamp does not fit in u64 milliseconds")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::memory::InMemory;

    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestResponse {
        revision: u64,
    }

    fn journal() -> MutationJournal {
        MutationJournal::from_object_store(Arc::new(InMemory::new()), ObjectPath::from("abashiri"))
    }

    fn request(key: &str, input: &[u8]) -> MutationRequest {
        MutationRequest::try_new(
            key,
            Actor::try_new(ActorKind::Workload, "test-issuer", "publisher").unwrap(),
            MutationAction::Publish,
            ResourceTarget::new(
                AccountId::try_new("example").unwrap(),
                ResourceKind::Tileset,
                LocalResourceId::try_new("weather").unwrap(),
            ),
            input,
            RequestId::new_random(),
        )
        .unwrap()
    }

    fn evidence(value: &str) -> VersionEvidence {
        VersionEvidence {
            e_tag: Some(value.to_string()),
            version: None,
        }
    }

    #[test]
    fn entity_tag_round_trips_etag_and_backend_version() {
        let evidence = VersionEvidence::try_new(
            Some("\"content-etag\"".to_string()),
            Some("1735689600123456".to_string()),
        )
        .unwrap();
        let encoded = evidence.to_entity_tag().unwrap();

        assert!(encoded.starts_with("\"abashiri-v1."));
        assert_eq!(
            VersionEvidence::from_entity_tag(&encoded).unwrap(),
            evidence
        );
        assert!(VersionEvidence::from_entity_tag("*").is_err());
        assert!(VersionEvidence::from_entity_tag("W/\"weak\"").is_err());
    }

    #[tokio::test]
    async fn completed_retry_returns_redacted_proof_without_recommitting() {
        let journal = journal();
        let commits = AtomicUsize::new(0);

        let first = journal
            .execute(request("publish-1", b"same"), |_| async {
                commits.fetch_add(1, Ordering::Relaxed);
                Ok(StateCommit::new(
                    evidence("v1"),
                    TestResponse { revision: 1 },
                    b"state-v1",
                ))
            })
            .await
            .unwrap();
        let retry = journal
            .execute(request("publish-1", b"same"), |_| async {
                commits.fetch_add(1, Ordering::Relaxed);
                Ok(StateCommit::new(
                    evidence("unexpected"),
                    TestResponse { revision: 2 },
                    b"unexpected",
                ))
            })
            .await
            .unwrap();

        assert!(matches!(
            first,
            Execution::Committed(TestResponse { revision: 1 })
        ));
        let Execution::AlreadyCompleted(completed) = retry else {
            panic!("retry unexpectedly committed again");
        };
        assert_eq!(
            completed.state_sha256,
            digest_hex(b"abashiri-committed-state-v1\0", b"state-v1")
        );
        assert_eq!(commits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn reused_key_with_different_input_is_rejected_before_commit() {
        let journal = journal();
        journal
            .execute(request("publish-2", b"first"), |_| async {
                Ok(StateCommit::new(
                    evidence("v1"),
                    TestResponse { revision: 1 },
                    b"state-v1",
                ))
            })
            .await
            .unwrap();

        let commits = AtomicUsize::new(0);
        let error = journal
            .execute(request("publish-2", b"different"), |_| async {
                commits.fetch_add(1, Ordering::Relaxed);
                Ok(StateCommit::new(
                    evidence("v2"),
                    TestResponse { revision: 2 },
                    b"state-v2",
                ))
            })
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<IdempotencyConflict>().is_some());
        assert!(
            error
                .to_string()
                .contains("idempotency key was reused for a different mutation")
        );
        assert_eq!(commits.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn invalid_completion_after_state_commit_is_not_reported_as_success() {
        let journal = journal();
        let request = request("publish-3", b"same");
        let paths = MutationPaths::new(&journal.store, &request).unwrap();
        let commits = AtomicUsize::new(0);

        let error = journal
            .execute(request, |intent| {
                let completion_path = paths.completion.clone();
                let store = &journal.store;
                let commits = &commits;
                let state_identity = intent.state_reference().as_bytes().to_vec();
                async move {
                    commits.fetch_add(1, Ordering::Relaxed);
                    store
                        .create(&completion_path, Bytes::from_static(b"not-json"))
                        .await
                        .unwrap();
                    Ok(StateCommit::new(
                        evidence("v1"),
                        TestResponse { revision: 1 },
                        &state_identity,
                    ))
                }
            })
            .await
            .unwrap_err();

        assert!(error.to_string().contains("decode mutation journal object"));
        assert_eq!(commits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn caller_response_is_not_persisted_in_completion() {
        struct SecretResponse {
            token: &'static str,
        }

        let journal = journal();
        let request = request("publish-4", b"same");
        let paths = MutationPaths::new(&journal.store, &request).unwrap();
        let result = journal
            .execute(request, |_| async {
                Ok(StateCommit::new(
                    evidence("v1"),
                    SecretResponse {
                        token: "one-time-secret",
                    },
                    b"redacted-token-metadata",
                ))
            })
            .await
            .unwrap();

        let Execution::Committed(response) = result else {
            panic!("first execution unexpectedly reported a prior completion");
        };
        assert_eq!(response.token, "one-time-secret");
        let persisted = journal.store.read(&paths.completion).await.unwrap();
        assert!(
            !persisted
                .windows(b"one-time-secret".len())
                .any(|window| { window == b"one-time-secret" })
        );
    }

    #[test]
    fn management_identifiers_are_bounded_single_segments() {
        for invalid in ["", "contains/slash", "contains.dot", "contains space"] {
            assert!(AccountId::try_new(invalid).is_err(), "{invalid:?}");
            assert!(LocalResourceId::try_new(invalid).is_err(), "{invalid:?}");
        }
        assert!(AccountId::try_new("a".repeat(MAX_ACCOUNT_ID_LEN + 1)).is_err());
        assert!(LocalResourceId::try_new("a".repeat(MAX_LOCAL_ID_LEN + 1)).is_err());
        assert!(AccountId::try_new("account_01").is_ok());
        assert!(LocalResourceId::try_new("weather-2026").is_ok());
    }

    #[test]
    fn principal_parts_reject_controls_and_unbounded_values() {
        assert!(Actor::try_new(ActorKind::Human, "issuer", "subject").is_ok());
        assert!(Actor::try_new(ActorKind::Human, "bad\nissuer", "subject").is_err());
        assert!(
            Actor::try_new(
                ActorKind::Human,
                "issuer",
                "a".repeat(MAX_PRINCIPAL_PART_LEN + 1)
            )
            .is_err()
        );
    }

    #[test]
    fn raw_idempotency_key_and_input_are_not_serialized() {
        let request = request("raw-secret-key", b"sensitive-input");
        let serialized = serde_json::to_string(&request).unwrap();
        assert!(!serialized.contains("raw-secret-key"));
        assert!(!serialized.contains("sensitive-input"));
    }

    #[test]
    fn completion_schema_contains_only_redacted_result_identity() {
        let completion = MutationCompletion {
            schema_version: SCHEMA_VERSION,
            key_sha256: "key-digest".to_string(),
            state_version: evidence("v1"),
            state_sha256: digest_hex(b"abashiri-committed-state-v1\0", b"safe-state"),
            completed_at_unix_ms: 1,
        };
        let serialized = serde_json::to_string(&completion).unwrap();
        assert!(!serialized.contains("response"));
        assert!(!serialized.contains("token"));
        assert!(serialized.contains("state_sha256"));
    }
}
