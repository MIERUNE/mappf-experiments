//! Cluster-stable `StyleId` to style definition resolution.
//!
//! Production and simulator both register explicit definitions or configure a
//! lazy URL template. Template resolution is computed on demand and is not
//! persisted merely by lookup, so misses cannot grow the catalog. Successfully
//! fetched identities do become process-lifetime revision authority; see
//! `StyleCatalogInner::observed` for why that state is not evicted like a cache.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::types::{StyleId, StyleRevision};

/// A style may change under a stable id, so a cached style is re-checked when its
/// served freshness expires — but never more often than this. Without a floor a
/// short or absent upstream policy would turn every render into a style fetch.
const MIN_STYLE_REVALIDATE_INTERVAL: Duration = Duration::from_secs(10);
const MAX_REVALIDATION_GENERATIONS: usize = 4_096;
const MAX_PENDING_STYLE_HINTS: usize = 4_096;
pub const STYLE_REVISION_GOSSIP_SLOTS: usize = 16;
const MAX_STYLE_REVISION_GOSSIP_BYTES: usize = 1_024;
const STYLE_REVISION_GOSSIP_KEY_PREFIX: &str = "observed-style-revision-v1-";

/// Derives the revision version from style content.
///
/// Equal content yields an equal version, so an expired freshness window causes
/// a re-check rather than a rebuild: warm-worker judgment, prepared profiles, and
/// the render-output cache all key on `StyleRevision` and therefore skip the
/// reload on their own. Changed content yields a different version, which is
/// already treated as cold cluster-wide.
///
/// The digest must be stable across processes because the version is gossiped.
/// A truncated SHA-256 is used instead of a fast non-cryptographic hash because
/// style bytes may come from an external provider and a chosen collision would
/// incorrectly reuse warm or rendered state.
///
/// The high bit is always set so a content version can never collide with the
/// reserved low versions used before any content has been observed.
#[must_use]
pub fn style_content_version(style_json: &str) -> u64 {
    let canonical = match serde_json::from_str::<serde_json::Value>(style_json) {
        Ok(mut style) => {
            normalize_credential_urls(&mut style);
            serde_json::to_vec(&style).unwrap_or_else(|_| style_json.as_bytes().to_vec())
        }
        // Callers validate style JSON before recording it. Keep this helper
        // total for tests and defensive use without making malformed input
        // collide at one sentinel value.
        Err(_) => style_json.as_bytes().to_vec(),
    };
    let digest = Sha256::digest(canonical);
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 prefix is 8 bytes"))
        | RESERVED_VERSION_BIT
}

/// Ishikari propagates the caller's `access_token` into generated dependency
/// URLs. That transport credential is already isolated by
/// `CredentialCachePartition`; including it in the semantic style revision
/// would make every caller flip the cluster-wide revision. Remove only this
/// query parameter while retaining every other URL and style difference.
fn normalize_credential_urls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                normalize_credential_urls(value);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                normalize_credential_urls(value);
            }
        }
        serde_json::Value::String(value) if value.contains("access_token") => {
            let Ok(mut url) = url::Url::parse(value) else {
                return;
            };
            let mut removed = false;
            let retained = url.query_pairs().filter_map(|(name, value)| {
                if name == "access_token" {
                    removed = true;
                    None
                } else {
                    Some((name.into_owned(), value.into_owned()))
                }
            });
            let retained = retained.collect::<Vec<_>>();
            if !removed {
                return;
            }
            url.query_pairs_mut().clear().extend_pairs(retained);
            *value = url.into();
        }
        _ => {}
    }
}

/// Separates observed content versions from the reserved pre-observation values.
const RESERVED_VERSION_BIT: u64 = 1 << 63;

/// Content revision observed after one specific publisher refresh hint.
///
/// The hint id supplies transition identity because content hashes themselves
/// have no ordering. Receivers adopt this revision only while waiting for the
/// same hint, so a retained observation from an older mutation cannot roll a
/// style backward.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct StyleRevisionObservation {
    pub schema_version: u8,
    pub hint_id: String,
    pub style_id: String,
    pub version: u64,
}

impl StyleRevisionObservation {
    const SCHEMA_VERSION: u8 = 1;

    fn new(hint_id: String, style_id: &StyleId, version: u64) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            hint_id,
            style_id: style_id.as_str().to_owned(),
            version,
        }
    }

    pub fn encode(&self) -> Option<String> {
        if self.schema_version != Self::SCHEMA_VERSION
            || self.hint_id.is_empty()
            || self.style_id.is_empty()
            || self.version & RESERVED_VERSION_BIT == 0
        {
            return None;
        }
        let encoded = serde_json::to_string(self).ok()?;
        (encoded.len() <= MAX_STYLE_REVISION_GOSSIP_BYTES).then_some(encoded)
    }

    pub fn decode(encoded: &str) -> Option<Self> {
        if encoded.len() > MAX_STYLE_REVISION_GOSSIP_BYTES {
            return None;
        }
        let observation = serde_json::from_str::<Self>(encoded).ok()?;
        observation.encode().map(|_| observation)
    }

    pub fn gossip_key(&self) -> String {
        style_revision_gossip_key(stable_revision_slot(&self.hint_id))
    }
}

#[must_use]
pub fn style_revision_gossip_key(slot: usize) -> String {
    debug_assert!(slot < STYLE_REVISION_GOSSIP_SLOTS);
    format!("{STYLE_REVISION_GOSSIP_KEY_PREFIX}{slot:02}")
}

fn stable_revision_slot(hint_id: &str) -> usize {
    let digest = Sha256::digest(hint_id.as_bytes());
    usize::from(u16::from_be_bytes([digest[0], digest[1]])) % STYLE_REVISION_GOSSIP_SLOTS
}

/// What the process last actually fetched for a style id.
#[derive(Clone, Copy, Debug)]
struct ObservedStyle {
    version: u64,
    /// The immediately superseded version, still accepted so a request already
    /// admitted — or forwarded by a peer that has not re-checked yet — is not
    /// rejected during the changeover.
    previous: Option<u64>,
    /// Configured placeholder accepted only across the first content
    /// observation, so independently partitioned credentials admitted during
    /// bootstrap can install the same validated content. Cleared on the first
    /// real content-to-content change.
    bootstrap: Option<u64>,
    /// Completion time of the last provider fetch. Refresh hints may shorten
    /// provider freshness, but never schedule another fetch before this time
    /// plus [`MIN_STYLE_REVALIDATE_INTERVAL`].
    last_observed_at: Instant,
    revalidate_after: Instant,
    /// A publisher hint has not yet been satisfied by a provider fetch that
    /// began after that hint. Rendered outputs must not hide this state.
    pending_revalidation: bool,
}

#[derive(Clone, Copy, Debug)]
struct ClusterObservedStyle {
    version: u64,
    previous: Option<u64>,
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StyleDefinition {
    pub style_url: String,
    pub version: u64,
}

impl StyleDefinition {
    pub fn new(style_url: impl Into<String>, version: u64) -> Self {
        Self {
            style_url: style_url.into(),
            version,
        }
    }
}

const INITIAL_STYLE_VERSION: u64 = 1;

/// The refresh fence a fetch captured immediately before its provider I/O.
///
/// `generation` is `None` when the style had no fence entry, which is
/// deliberately distinct from `Some(0)`: the per-style map is bounded, so an
/// absent entry may mean "never hinted" or may mean capacity was exhausted when a
/// hint arrived. `requests` resolves that ambiguity — it counts hints
/// process-wide and nothing lowers it, so an unchanged value proves no hint
/// landed during the flight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RevalidationFence {
    generation: Option<u64>,
    requests: u64,
}

/// What [`StyleCatalog::request_revalidation`] did with a hint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevalidationRequest {
    /// The style was pulled forward and its fence advanced.
    Accepted,
    /// The style was pulled forward, but the bounded fence map had no room for a
    /// new entry. An in-flight fetch still stays due, at the cost of one extra
    /// revalidation; the caller should surface this as a cardinality signal.
    AcceptedWithoutFence,
    /// The style has never been observed, so its next request is already due and
    /// no fence is needed.
    AlreadyDue,
}

impl RevalidationRequest {
    /// Whether the hint changed anything about an already-observed style.
    #[must_use]
    pub fn applied_to_observed_style(self) -> bool {
        matches!(self, Self::Accepted | Self::AcceptedWithoutFence)
    }
}

#[derive(Debug, Default)]
struct StyleCatalogInner {
    by_id: HashMap<StyleId, StyleDefinition>,
    /// Content actually observed by this process, which supersedes any
    /// configured version for revision purposes.
    ///
    /// This is authoritative runtime revision state, not a cache: evicting it
    /// can resurrect a bootstrap revision or reject peer work still using the
    /// current/previous revision. Template misses are deliberately not stored,
    /// but successfully fetched identities remain until process restart. A
    /// future bound must be an explicit admission/catalog limit, not LRU
    /// eviction.
    observed: HashMap<StyleId, ObservedStyle>,
    /// Revision validated by a peer for the same pending publisher hint.
    /// Kept separate from `observed`: it may drive routing, but never claims
    /// that this process fetched or cached the corresponding style bytes.
    cluster_observed: HashMap<StyleId, ClusterObservedStyle>,
    /// Latest publisher transition this process is waiting to resolve.
    pending_hints: HashMap<StyleId, String>,
    /// Fixed gossip ring populated only by local provider observations.
    revision_announcements: HashMap<usize, (String, String)>,
    /// Monotonic per-style fence incremented by advisory refresh hints. A fetch
    /// captures the value before I/O so its completion cannot postpone a newer
    /// hint by restarting the upstream freshness window. Bounded by
    /// [`MAX_REVALIDATION_GENERATIONS`], so a missing entry is ambiguous and
    /// `revalidation_requests` exists to disambiguate it.
    revalidation_generations: HashMap<StyleId, u64>,
    /// Count of every refresh hint this process has accepted, for any style.
    ///
    /// Nothing evicts or lowers it, so an unchanged value proves no hint arrived
    /// while a fetch was in flight. That is what lets a *missing* per-style entry
    /// fail closed — staying due — instead of being read as "never hinted".
    revalidation_requests: u64,
    /// `namespace -> template`, keyed on the first path segment of a style id.
    /// A match strips the namespace, substituting only the remaining segments.
    namespace_templates: HashMap<String, String>,
    /// Catch-all used when no namespace template matches; substitutes the whole
    /// style id (so `default` behaves like the historic single template).
    default_template: Option<String>,
}

impl StyleCatalogInner {
    fn latest_version(&self, style_id: &StyleId) -> Option<u64> {
        self.cluster_observed
            .get(style_id)
            .map(|observed| observed.version)
            .or_else(|| self.observed.get(style_id).map(|observed| observed.version))
            .or_else(|| {
                self.by_id
                    .get(style_id)
                    .map(|definition| definition.version)
            })
            .or_else(|| self.template_for(style_id).map(|_| INITIAL_STYLE_VERSION))
    }

    fn accepts_version(&self, style_id: &StyleId, version: u64) -> bool {
        self.cluster_observed.get(style_id).is_some_and(|observed| {
            observed.version == version || observed.previous == Some(version)
        }) || self.observed.get(style_id).is_some_and(|observed| {
            observed.version == version
                || observed.previous == Some(version)
                || observed.bootstrap == Some(version)
        }) || (!self.cluster_observed.contains_key(style_id)
            && !self.observed.contains_key(style_id)
            && self.latest_version(style_id) == Some(version))
    }

    fn needs_revalidation(&self, style_id: &StyleId, now: Instant) -> bool {
        let local = self.observed.get(style_id);
        self.cluster_observed
            .get(style_id)
            .is_some_and(|cluster| local.is_none_or(|local| local.version != cluster.version))
            || local.is_none_or(|observed| now >= observed.revalidate_after)
    }

    fn has_pending_revalidation(&self, style_id: &StyleId) -> bool {
        self.pending_hints.contains_key(style_id)
            || self
                .observed
                .get(style_id)
                .is_some_and(|observed| observed.pending_revalidation)
    }

    fn is_current_cluster_revision(&self, revision: &StyleRevision) -> bool {
        self.cluster_observed
            .get(&revision.id)
            .is_some_and(|observed| observed.version == revision.version)
    }

    /// Whether no refresh hint at all can have landed since `fence` was captured.
    ///
    /// When true the per-style map cannot have lost anything relevant, so a
    /// completing fetch may take the provider's full freshness window.
    fn hint_is_impossible_since(&self, fence: RevalidationFence) -> bool {
        self.revalidation_requests == fence.requests
    }

    /// Pick the template for `id` and the value to substitute for `{style_id}`:
    /// a namespace match strips its prefix (provider-local id), otherwise the
    /// default template receives the whole id.
    fn template_for<'a>(&'a self, id: &'a StyleId) -> Option<(&'a str, &'a str)> {
        if let Some((namespace, rest)) = id.as_str().split_once('/')
            && let Some(template) = self.namespace_templates.get(namespace)
        {
            return Some((template, rest));
        }
        self.default_template
            .as_deref()
            .map(|template| (template, id.as_str()))
    }
}

#[derive(Debug)]
pub struct StyleCatalog {
    inner: RwLock<StyleCatalogInner>,
    minimum_revalidation_interval: Duration,
}

impl Default for StyleCatalog {
    fn default() -> Self {
        Self {
            inner: RwLock::new(StyleCatalogInner::default()),
            minimum_revalidation_interval: MIN_STYLE_REVALIDATE_INTERVAL,
        }
    }
}

impl StyleCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test-only clock-policy override for integration tests that exercise a
    /// complete refresh without sleeping for the production floor.
    #[doc(hidden)]
    pub fn with_minimum_revalidation_interval_for_tests(interval: Duration) -> Self {
        Self {
            inner: RwLock::new(StyleCatalogInner::default()),
            minimum_revalidation_interval: interval,
        }
    }

    /// Add or update the renderable style definition.
    pub fn upsert_definition(&self, style_id: StyleId, definition: StyleDefinition) {
        write_unpoisoned(&self.inner)
            .by_id
            .insert(style_id, definition);
    }

    /// Configure the default lazy `StyleId -> style.json URL` template. Unknown
    /// styles with no matching namespace template resolve on demand by replacing
    /// `{style_id}` (with the whole id) in this template. Explicit
    /// `upsert_definition` entries still take precedence.
    pub fn set_url_template(&self, template: impl Into<String>) {
        write_unpoisoned(&self.inner).default_template = Some(template.into());
    }

    /// Register a per-namespace template. A style id whose first path segment is
    /// `namespace` resolves against this template, substituting `{style_id}`
    /// with the segments after the namespace.
    pub fn add_namespace_template(
        &self,
        namespace: impl Into<String>,
        template: impl Into<String>,
    ) {
        write_unpoisoned(&self.inner)
            .namespace_templates
            .insert(namespace.into(), template.into());
    }

    pub fn resolve_latest(&self, style_id: &StyleId) -> Option<u64> {
        read_unpoisoned(&self.inner).latest_version(style_id)
    }

    /// Content-derived version already observed by this process. Unlike
    /// [`resolve_latest`](Self::resolve_latest), this excludes the configured
    /// pre-observation placeholder.
    pub fn observed_version(&self, style_id: &StyleId) -> Option<u64> {
        read_unpoisoned(&self.inner)
            .observed
            .get(style_id)
            .map(|observed| observed.version)
    }

    /// Records the content this process just fetched. Returns whether the
    /// version changed, so a caller can log or count an actual style change
    /// rather than an ordinary re-check.
    ///
    /// `served_freshness` is the upstream freshness for the fetched document;
    /// the next re-check is due after that, floored by
    /// `MIN_STYLE_REVALIDATE_INTERVAL`.
    pub fn record_observed(
        &self,
        style_id: &StyleId,
        version: u64,
        served_freshness: Duration,
    ) -> bool {
        let fence = self.revalidation_fence(style_id);
        self.record_observed_for_generation(style_id, version, served_freshness, fence)
    }

    /// Records a fetch that began under `fence`.
    ///
    /// If a refresh hint advanced the fence while I/O was running, the content
    /// may still satisfy the admitted request, but it must remain due instead of
    /// suppressing the newer hint for the provider's full TTL.
    pub fn record_observed_for_generation(
        &self,
        style_id: &StyleId,
        version: u64,
        served_freshness: Duration,
        fence: RevalidationFence,
    ) -> bool {
        let mut inner = write_unpoisoned(&self.inner);
        let observed_at = Instant::now();
        let full_freshness = observed_at + served_freshness.max(self.minimum_revalidation_interval);
        let stay_due = observed_at + self.minimum_revalidation_interval;
        let fetch_is_current = if inner.hint_is_impossible_since(fence) {
            // No hint landed anywhere during the flight, so nothing can have been
            // lost from the bounded per-style map.
            true
        } else {
            matches!(
                inner.revalidation_generations.get(style_id).copied(),
                // The fence is intact and unchanged: this fetch is not superseded.
                Some(current) if Some(current) == fence.generation
            )
        };
        // Either a hint advanced this style's generation, or a hint landed while
        // this style has no entry to compare — indistinguishable from an entry
        // that capacity pressure prevented allocating. Preserve the hint either
        // way, without letting another request bypass the floor.
        let revalidate_after = if fetch_is_current {
            full_freshness
        } else {
            stay_due
        };
        let changed = match inner.observed.get_mut(style_id) {
            Some(observed) if observed.version == version => {
                observed.last_observed_at = observed_at;
                observed.revalidate_after = revalidate_after;
                observed.pending_revalidation = !fetch_is_current;
                false
            }
            Some(observed) => {
                observed.previous = Some(observed.version);
                observed.bootstrap = None;
                observed.version = version;
                observed.last_observed_at = observed_at;
                observed.revalidate_after = revalidate_after;
                observed.pending_revalidation = !fetch_is_current;
                true
            }
            None => {
                let bootstrap = inner
                    .by_id
                    .get(style_id)
                    .map(|definition| definition.version)
                    .or_else(|| inner.template_for(style_id).map(|_| INITIAL_STYLE_VERSION))
                    .filter(|bootstrap| *bootstrap != version);
                inner.observed.insert(
                    style_id.clone(),
                    ObservedStyle {
                        version,
                        previous: None,
                        bootstrap,
                        last_observed_at: observed_at,
                        revalidate_after,
                        pending_revalidation: !fetch_is_current,
                    },
                );
                true
            }
        };

        if fetch_is_current {
            if let Some(hint_id) = inner.pending_hints.remove(style_id) {
                let observation = StyleRevisionObservation::new(hint_id, style_id, version);
                let slot = stable_revision_slot(&observation.hint_id);
                let encoded = observation
                    .encode()
                    .expect("validated style and hint ids fit the gossip envelope");
                inner
                    .revision_announcements
                    .insert(slot, (observation.gossip_key(), encoded));
                inner.cluster_observed.insert(
                    style_id.clone(),
                    ClusterObservedStyle {
                        version,
                        previous: None,
                    },
                );
            } else if inner
                .cluster_observed
                .get(style_id)
                .is_some_and(|cluster| cluster.version != version)
            {
                // A direct provider observation is stronger than an old peer
                // announcement retained after its transition.
                inner.cluster_observed.remove(style_id);
            }
        }
        changed
    }

    /// Whether the style should be re-checked against its provider. A style with
    /// no observed content has never been fetched and always needs one.
    pub fn needs_revalidation(&self, style_id: &StyleId) -> bool {
        read_unpoisoned(&self.inner).needs_revalidation(style_id, Instant::now())
    }

    /// Whether an accepted publisher hint still awaits a provider fetch that
    /// began after the hint. Output caches must not short-circuit that fetch.
    pub fn has_pending_revalidation(&self, style_id: &StyleId) -> bool {
        read_unpoisoned(&self.inner).has_pending_revalidation(style_id)
    }

    /// Captures the refresh fence immediately before provider I/O begins.
    pub fn revalidation_fence(&self, style_id: &StyleId) -> RevalidationFence {
        let inner = read_unpoisoned(&self.inner);
        RevalidationFence {
            generation: inner.revalidation_generations.get(style_id).copied(),
            requests: inner.revalidation_requests,
        }
    }

    /// Pulls the next provider check forward to the earliest time allowed by
    /// `MIN_STYLE_REVALIDATE_INTERVAL`.
    ///
    /// This does not perform I/O or change the active revision by itself. It is
    /// the bounded hook for an authenticated publisher refresh hint; the normal
    /// single-flight fetch path still validates and activates content. Repeated
    /// hints cannot postpone or accelerate the same style beyond that floor.
    ///
    /// Generation entries are allocated only for styles this process has actually
    /// observed. An unobserved style needs no entry — its next request is already
    /// due — and refusing to allocate for one is what keeps a stream of hints for
    /// arbitrary identifiers from consuming the bounded map. Correctness does not
    /// depend on the allocation succeeding: `revalidation_requests` records that a
    /// hint happened at all, and
    /// [`record_observed_for_generation`](Self::record_observed_for_generation)
    /// treats a missing entry after a hint as a reason to stay due.
    pub fn request_revalidation(&self, style_id: &StyleId) -> RevalidationRequest {
        self.request_revalidation_inner(style_id, None)
    }

    /// Requests revalidation for one publisher transition. A later peer
    /// observation is accepted only when it carries this exact hint id.
    pub fn request_revalidation_for_hint(
        &self,
        style_id: &StyleId,
        hint_id: &str,
    ) -> RevalidationRequest {
        self.request_revalidation_inner(style_id, Some(hint_id))
    }

    fn request_revalidation_inner(
        &self,
        style_id: &StyleId,
        hint_id: Option<&str>,
    ) -> RevalidationRequest {
        let mut inner = write_unpoisoned(&self.inner);
        // Count the hint before touching per-style state, so a fetch completing
        // concurrently can never see the pre-hint count together with post-hint
        // per-style state.
        inner.revalidation_requests = inner.revalidation_requests.wrapping_add(1);
        let hint_stored = hint_id.is_none_or(|hint_id| {
            if inner.pending_hints.contains_key(style_id)
                || inner.pending_hints.len() < MAX_PENDING_STYLE_HINTS
            {
                inner
                    .pending_hints
                    .insert(style_id.clone(), hint_id.to_owned());
                true
            } else {
                false
            }
        });
        let Some(observed) = inner.observed.get(style_id) else {
            return if hint_stored {
                RevalidationRequest::AlreadyDue
            } else {
                RevalidationRequest::AcceptedWithoutFence
            };
        };
        let earliest = observed.last_observed_at + self.minimum_revalidation_interval;
        let outcome = if let Some(generation) = inner.revalidation_generations.get_mut(style_id) {
            *generation = generation.wrapping_add(1);
            RevalidationRequest::Accepted
        } else if inner.revalidation_generations.len() < MAX_REVALIDATION_GENERATIONS {
            inner.revalidation_generations.insert(style_id.clone(), 1);
            RevalidationRequest::Accepted
        } else {
            // The pull-forward below still applies, and an in-flight fetch still
            // stays due via `revalidation_requests`. Report it so exhaustion is
            // visible rather than silent.
            RevalidationRequest::AcceptedWithoutFence
        };
        let observed = inner
            .observed
            .get_mut(style_id)
            .expect("presence checked above");
        observed.revalidate_after = observed.revalidate_after.min(earliest);
        observed.pending_revalidation = true;
        if hint_stored {
            outcome
        } else {
            RevalidationRequest::AcceptedWithoutFence
        }
    }

    /// Applies a peer observation only to the matching pending transition.
    /// Returns true when the cluster-visible current revision changed.
    pub fn apply_cluster_observation(&self, observation: &StyleRevisionObservation) -> bool {
        if observation.encode().is_none() {
            return false;
        }
        let style_id = StyleId(observation.style_id.clone());
        let mut inner = write_unpoisoned(&self.inner);
        if inner.pending_hints.get(&style_id).map(String::as_str)
            != Some(observation.hint_id.as_str())
        {
            return false;
        }
        let previous = inner
            .latest_version(&style_id)
            .filter(|old| *old != observation.version);
        let changed = previous.is_some();
        inner.cluster_observed.insert(
            style_id.clone(),
            ClusterObservedStyle {
                version: observation.version,
                previous,
            },
        );
        inner.pending_hints.remove(&style_id);
        changed
    }

    /// Current fixed-ring gossip records produced by this process.
    pub fn revision_gossip_kvs(&self) -> Vec<(String, String)> {
        read_unpoisoned(&self.inner)
            .revision_announcements
            .values()
            .cloned()
            .collect()
    }

    pub fn accepts_revision(&self, revision: &StyleRevision) -> bool {
        read_unpoisoned(&self.inner).accepts_version(&revision.id, revision.version)
    }

    /// Whether the task already carries the current peer-validated revision.
    /// An exact output-cache entry under this content-derived key is safe even
    /// before this process has fetched the style bytes itself.
    pub fn is_current_cluster_revision(&self, revision: &StyleRevision) -> bool {
        read_unpoisoned(&self.inner).is_current_cluster_revision(revision)
    }

    /// Whether output-cache reuse would hide a required provider revalidation.
    ///
    /// Keep the three related observations under one read lock: taking separate
    /// snapshots can both add per-request synchronization and combine states that
    /// never existed together while a refresh hint is being applied.
    pub(crate) fn revalidation_blocks_output_cache(&self, revision: &StyleRevision) -> bool {
        let inner = read_unpoisoned(&self.inner);
        !inner.is_current_cluster_revision(revision)
            && (inner.has_pending_revalidation(&revision.id)
                || inner.needs_revalidation(&revision.id, Instant::now()))
    }

    /// Whether `revision` is the configured placeholder retained across the
    /// first content observation, not a genuine superseded content version.
    pub fn is_bootstrap_revision(&self, revision: &StyleRevision) -> bool {
        read_unpoisoned(&self.inner)
            .observed
            .get(&revision.id)
            .is_some_and(|observed| observed.bootstrap == Some(revision.version))
    }

    pub fn definition_for_revision(&self, revision: &StyleRevision) -> Option<StyleDefinition> {
        let inner = read_unpoisoned(&self.inner);
        if !inner.accepts_version(&revision.id, revision.version) {
            return None;
        }
        if let Some(definition) = inner.by_id.get(&revision.id) {
            return Some(StyleDefinition::new(
                definition.style_url.clone(),
                revision.version,
            ));
        }
        inner.template_for(&revision.id).map(|(template, subst)| {
            StyleDefinition::new(template.replace("{style_id}", subst), revision.version)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_content_yields_an_equal_revision() {
        // The requirement that unchanged content must not trigger a re-render is
        // satisfied by identity: the version is the content, so every cache and
        // warm-judgment check that keys on `StyleRevision` skips the rebuild.
        let json = r#"{"version":8,"sources":{},"layers":[]}"#;
        assert_eq!(style_content_version(json), style_content_version(json));
        assert_ne!(
            style_content_version(json),
            style_content_version(r#"{"version":8,"sources":{},"layers":[{}]}"#)
        );
    }

    #[test]
    fn propagated_access_token_does_not_change_the_semantic_revision() {
        let first = r#"{
            "version": 8,
            "sources": {
                "base": {
                    "url": "https://ishikari.test/tilesets/base?encoding=mlt&access_token=public.first"
                }
            },
            "glyphs": "https://ishikari.test/fonts/{fontstack}/{range}.pbf?access_token=public.first",
            "layers": []
        }"#;
        let second = r#"{
            "layers": [],
            "glyphs": "https://ishikari.test/fonts/{fontstack}/{range}.pbf?access_token=public.second",
            "sources": {
                "base": {
                    "url": "https://ishikari.test/tilesets/base?encoding=mlt&access_token=public.second"
                }
            },
            "version": 8
        }"#;
        assert_eq!(
            style_content_version(first),
            style_content_version(second),
            "credential transport and JSON formatting are not semantic style changes"
        );
    }

    #[test]
    fn a_noncredential_url_change_changes_the_semantic_revision() {
        let first = r#"{"version":8,"sources":{"base":{"url":"https://ishikari.test/tilesets/a?access_token=one"}},"layers":[]}"#;
        let second = r#"{"version":8,"sources":{"base":{"url":"https://ishikari.test/tilesets/b?access_token=two"}},"layers":[]}"#;
        assert_ne!(style_content_version(first), style_content_version(second));
    }

    #[test]
    fn a_content_version_never_collides_with_a_pre_observation_version() {
        for json in ["{}", r#"{"version":8}"#, "[]"] {
            let version = style_content_version(json);
            assert_ne!(version, INITIAL_STYLE_VERSION);
            assert_ne!(version, 0);
            assert!(version & RESERVED_VERSION_BIT != 0);
        }
    }

    #[test]
    fn observed_content_supersedes_the_configured_version() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("gl/basic".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new("https://styles.test/basic/style.json", 3),
        );
        assert_eq!(catalog.resolve_latest(&style_id), Some(3));

        let observed = style_content_version(r#"{"version":8,"layers":[]}"#);
        assert!(catalog.record_observed(&style_id, observed, Duration::ZERO));
        assert_eq!(catalog.resolve_latest(&style_id), Some(observed));
        // The URL is content-independent, so the observed revision still resolves.
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: style_id,
                    version: observed,
                })
                .expect("observed revision resolves")
                .style_url,
            "https://styles.test/basic/style.json"
        );
    }

    #[test]
    fn re_observing_the_same_content_reports_no_change() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("basic".to_string());
        let version = style_content_version("{}");
        assert!(
            catalog.record_observed(&style_id, version, Duration::ZERO),
            "first observation is a change"
        );
        assert!(
            !catalog.record_observed(&style_id, version, Duration::ZERO),
            "unchanged content must not report a style change"
        );
    }

    #[test]
    fn the_superseded_revision_is_still_accepted_during_a_changeover() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("basic".to_string());
        catalog.set_url_template("https://styles.test/{style_id}/style.json");
        let first = style_content_version(r#"{"version":8,"layers":[]}"#);
        let second = style_content_version(r#"{"version":8,"layers":[{}]}"#);
        catalog.record_observed(&style_id, first, Duration::ZERO);
        catalog.record_observed(&style_id, second, Duration::ZERO);

        for (version, accepted) in [(second, true), (first, true), (0, false)] {
            assert_eq!(
                catalog.accepts_revision(&StyleRevision {
                    id: style_id.clone(),
                    version,
                }),
                accepted,
                "version {version} acceptance"
            );
        }
        // An in-flight or peer-forwarded request on the superseded revision must
        // still resolve a style URL rather than fail mid-render.
        assert!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: style_id,
                    version: first,
                })
                .is_some()
        );
    }

    #[test]
    fn bootstrap_is_dropped_on_the_first_real_content_change() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("basic".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new("https://styles.test/basic/style.json", 7),
        );
        let bootstrap = StyleRevision {
            id: style_id.clone(),
            version: 7,
        };
        let first = style_content_version(r#"{"version":8,"layers":[]}"#);
        let second = style_content_version(r#"{"version":8,"layers":[{}]}"#);

        catalog.record_observed(&style_id, first, Duration::ZERO);
        assert!(catalog.accepts_revision(&bootstrap));
        assert!(catalog.is_bootstrap_revision(&bootstrap));

        // Re-observing byte-equivalent content keeps bootstrap safe: it still
        // names the same semantic content that was fetched during startup.
        catalog.record_observed(&style_id, first, Duration::ZERO);
        assert!(catalog.accepts_revision(&bootstrap));

        catalog.record_observed(&style_id, second, Duration::ZERO);
        assert!(!catalog.accepts_revision(&bootstrap));
        assert!(!catalog.is_bootstrap_revision(&bootstrap));
        assert!(catalog.definition_for_revision(&bootstrap).is_none());
    }

    #[test]
    fn revalidation_is_floored_even_when_upstream_freshness_is_zero() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("basic".to_string());
        assert!(
            catalog.needs_revalidation(&style_id),
            "a style with no observed content must be fetched"
        );
        catalog.record_observed(&style_id, style_content_version("{}"), Duration::ZERO);
        assert!(
            !catalog.needs_revalidation(&style_id),
            "the floor applies even when the served policy is zero"
        );
        assert!(MIN_STYLE_REVALIDATE_INTERVAL >= Duration::from_secs(10));
    }

    #[test]
    fn a_longer_served_freshness_extends_beyond_the_floor() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("basic".to_string());
        let long = Duration::from_hours(1);
        catalog.record_observed(&style_id, style_content_version("{}"), long);
        let due = read_unpoisoned(&catalog.inner).observed[&style_id].revalidate_after;
        assert!(
            due >= Instant::now() + long - Duration::from_secs(1),
            "the served freshness must be honoured when it exceeds the floor"
        );
    }

    #[test]
    fn upsert_definition_resolves_latest() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("voyager-gl-style".to_string());
        let definition = StyleDefinition::new(
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
            3,
        );

        catalog.upsert_definition(style_id.clone(), definition.clone());
        assert_eq!(catalog.resolve_latest(&style_id), Some(3));
        assert_eq!(
            catalog.definition_for_revision(&StyleRevision {
                id: style_id,
                version: 3
            }),
            Some(definition)
        );
    }

    #[test]
    fn definition_lookup_requires_matching_version() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("voyager-gl-style".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new("https://example.test/style.json", 7),
        );

        assert_eq!(
            catalog.definition_for_revision(&StyleRevision {
                id: style_id,
                version: 6
            }),
            None
        );
    }

    #[test]
    fn url_template_lazily_resolves_unknown_styles() {
        let catalog = StyleCatalog::new();
        catalog.set_url_template("http://style-provider.local/styles/{style_id}/style.json");
        let style_id = StyleId("example-basic".to_string());

        assert_eq!(catalog.resolve_latest(&style_id), Some(1));
        assert!(catalog.accepts_revision(&StyleRevision {
            id: style_id.clone(),
            version: 1,
        }));
        assert!(!catalog.accepts_revision(&StyleRevision {
            id: style_id.clone(),
            version: 0,
        }));
        assert_eq!(
            catalog.definition_for_revision(&StyleRevision {
                id: style_id,
                version: 1,
            }),
            Some(StyleDefinition::new(
                "http://style-provider.local/styles/example-basic/style.json",
                1,
            ))
        );
        assert!(
            read_unpoisoned(&catalog.inner).by_id.is_empty(),
            "template resolution must not persist attacker-controlled style ids"
        );
    }

    #[test]
    fn namespace_template_strips_prefix_and_default_keeps_whole_id() {
        let catalog = StyleCatalog::new();
        catalog.add_namespace_template(
            "gl",
            "https://basemaps.cartocdn.com/gl/{style_id}/style.json",
        );
        catalog.set_url_template("https://fallback.example/{style_id}/style.json");

        // Matched namespace: prefix stripped, only the remainder substituted.
        let matched = StyleId("gl/voyager-gl-style".to_string());
        assert_eq!(catalog.resolve_latest(&matched), Some(1));
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: matched,
                    version: 1,
                })
                .expect("namespace template resolves")
                .style_url,
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        );

        // Unmatched namespace falls back to the default with the whole id.
        let unmatched = StyleId("other/basic".to_string());
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: unmatched,
                    version: 1,
                })
                .expect("default template resolves")
                .style_url,
            "https://fallback.example/other/basic/style.json"
        );
    }

    #[test]
    fn namespace_only_catalog_404s_unmatched() {
        let catalog = StyleCatalog::new();
        catalog.add_namespace_template(
            "gl",
            "https://basemaps.cartocdn.com/gl/{style_id}/style.json",
        );

        assert_eq!(
            catalog.resolve_latest(&StyleId("voyager-gl-style".to_string())),
            None,
            "single-segment id has no namespace and no default template"
        );
        assert_eq!(
            catalog.resolve_latest(&StyleId("unknown/foo".to_string())),
            None,
        );
    }

    #[test]
    fn explicit_definition_overrides_url_template() {
        let catalog = StyleCatalog::new();
        catalog.set_url_template("https://styles.example.com/{style_id}/style.json");
        let style_id = StyleId("voyager-gl-style".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new(
                "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json",
                3,
            ),
        );

        assert_eq!(catalog.resolve_latest(&style_id), Some(3));
        assert_eq!(
            catalog
                .definition_for_revision(&StyleRevision {
                    id: style_id,
                    version: 3,
                })
                .expect("explicit definition exists")
                .style_url,
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        );
    }

    /// A hint for a style this process never fetched must not consume a slot in
    /// the bounded fence map. Otherwise a stream of hints for arbitrary
    /// identifiers exhausts the map and disables the fence for real styles.
    #[test]
    fn hints_for_unobserved_styles_never_consume_fence_capacity() {
        let catalog = StyleCatalog::new();
        catalog.set_url_template("https://example/{style_id}/style.json");
        for index in 0..(MAX_REVALIDATION_GENERATIONS * 2) {
            let style_id = StyleId(format!("never-fetched-{index}"));
            // A default template makes `resolve_latest` succeed for any id, which
            // is exactly why the receiver's own guard is not enough here.
            assert!(catalog.resolve_latest(&style_id).is_some());
            assert_eq!(
                catalog.request_revalidation(&style_id),
                RevalidationRequest::AlreadyDue
            );
        }
        assert!(
            read_unpoisoned(&catalog.inner)
                .revalidation_generations
                .is_empty(),
            "unobserved styles must not allocate fence entries"
        );
    }

    /// When the fence map is full, a hint is still honoured: the outcome reports
    /// the exhaustion, and a fetch already in flight stays due rather than being
    /// granted the provider's full freshness window.
    #[test]
    fn fence_exhaustion_is_reported_and_still_keeps_an_inflight_fetch_due() {
        let catalog = StyleCatalog::new();
        let long_ttl = Duration::from_hours(1);

        // Fill the fence map with observed, hinted styles.
        for index in 0..MAX_REVALIDATION_GENERATIONS {
            let filler = StyleId(format!("filler-{index}"));
            catalog.record_observed(&filler, style_content_version("{}"), long_ttl);
            assert_eq!(
                catalog.request_revalidation(&filler),
                RevalidationRequest::Accepted
            );
        }
        assert_eq!(
            read_unpoisoned(&catalog.inner)
                .revalidation_generations
                .len(),
            MAX_REVALIDATION_GENERATIONS
        );

        // A style observed but never hinted therefore has no entry, and cannot
        // get one.
        let victim = StyleId("victim".to_string());
        catalog.record_observed(&victim, style_content_version("{}"), long_ttl);
        let fence = catalog.revalidation_fence(&victim);
        assert_eq!(fence.generation, None);

        assert_eq!(
            catalog.request_revalidation(&victim),
            RevalidationRequest::AcceptedWithoutFence,
            "exhaustion must be reported rather than silently ignored"
        );

        catalog.record_observed_for_generation(
            &victim,
            style_content_version(r#"{"changed":true}"#),
            long_ttl,
            fence,
        );
        let observed = read_unpoisoned(&catalog.inner).observed[&victim];
        assert!(
            observed.revalidate_after < observed.last_observed_at + long_ttl,
            "an exhausted fence must not let the hint be erased by the provider TTL"
        );
    }

    /// The fail-closed rule must not tax a style whose own fence is intact: a
    /// hint for an unrelated style advances the process-wide count, and that
    /// alone must not discard this fetch's freshness.
    #[test]
    fn a_hint_for_another_style_does_not_shorten_an_intact_fence() {
        let catalog = StyleCatalog::new();
        let long_ttl = Duration::from_hours(1);
        let mine = StyleId("mine".to_string());
        let theirs = StyleId("theirs".to_string());
        catalog.record_observed(&mine, style_content_version("{}"), long_ttl);
        catalog.record_observed(&theirs, style_content_version("{}"), long_ttl);

        // Give `mine` a fence entry so its state is present rather than absent.
        assert_eq!(
            catalog.request_revalidation(&mine),
            RevalidationRequest::Accepted
        );
        let fence = catalog.revalidation_fence(&mine);
        assert!(fence.generation.is_some());

        assert_eq!(
            catalog.request_revalidation(&theirs),
            RevalidationRequest::Accepted
        );

        catalog.record_observed_for_generation(
            &mine,
            style_content_version(r#"{"v":2}"#),
            long_ttl,
            fence,
        );
        let observed = read_unpoisoned(&catalog.inner).observed[&mine];
        assert_eq!(
            observed.revalidate_after,
            observed.last_observed_at + long_ttl,
            "another style's hint must not shorten this style's freshness"
        );
    }

    /// With no hint at all, a completion takes the provider's full window even
    /// though the style has no fence entry.
    #[test]
    fn without_any_hint_a_completion_takes_the_full_freshness_window() {
        let catalog = StyleCatalog::new();
        let long_ttl = Duration::from_hours(1);
        let style_id = StyleId("quiet".to_string());
        let fence = catalog.revalidation_fence(&style_id);
        assert_eq!(fence.generation, None);
        catalog.record_observed_for_generation(
            &style_id,
            style_content_version("{}"),
            long_ttl,
            fence,
        );
        let observed = read_unpoisoned(&catalog.inner).observed[&style_id];
        assert_eq!(
            observed.revalidate_after,
            observed.last_observed_at + long_ttl
        );
    }

    #[test]
    fn hint_during_fetch_preserves_the_floor_without_restarting_the_old_freshness_window() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("style".to_string());
        catalog.upsert_definition(
            style_id.clone(),
            StyleDefinition::new("https://example/style.json", 1),
        );
        let fence = catalog.revalidation_fence(&style_id);
        assert_eq!(
            catalog.request_revalidation(&style_id),
            RevalidationRequest::AlreadyDue
        );

        catalog.record_observed_for_generation(
            &style_id,
            style_content_version("{}"),
            Duration::from_hours(1),
            fence,
        );

        let observed = read_unpoisoned(&catalog.inner).observed[&style_id];
        assert!(!catalog.needs_revalidation(&style_id));
        assert!(catalog.has_pending_revalidation(&style_id));
        assert!(
            observed.revalidate_after >= observed.last_observed_at + MIN_STYLE_REVALIDATE_INTERVAL
        );
        assert!(
            observed.revalidate_after < observed.last_observed_at + Duration::from_hours(1),
            "the stale provider TTL must not erase the newer hint"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_hints_pull_forward_once_without_bypassing_or_extending_the_floor() {
        let catalog = StyleCatalog::new();
        let style_id = StyleId("style".to_string());
        catalog.record_observed(
            &style_id,
            style_content_version("{}"),
            Duration::from_hours(1),
        );
        let first_observation = read_unpoisoned(&catalog.inner).observed[&style_id];

        assert!(
            catalog
                .request_revalidation(&style_id)
                .applied_to_observed_style()
        );
        let first_due = read_unpoisoned(&catalog.inner).observed[&style_id].revalidate_after;
        assert_eq!(
            first_due,
            first_observation.last_observed_at + MIN_STYLE_REVALIDATE_INTERVAL
        );
        assert!(!catalog.needs_revalidation(&style_id));

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(
            catalog
                .request_revalidation(&style_id)
                .applied_to_observed_style()
        );
        let repeated_due = read_unpoisoned(&catalog.inner).observed[&style_id].revalidate_after;
        assert_eq!(
            repeated_due, first_due,
            "a retry loop must not extend or accelerate the pending refresh"
        );
        assert!(!catalog.needs_revalidation(&style_id));

        tokio::time::advance(Duration::from_secs(5)).await;
        assert!(catalog.needs_revalidation(&style_id));

        let fence = catalog.revalidation_fence(&style_id);
        catalog.record_observed_for_generation(
            &style_id,
            style_content_version("{}"),
            Duration::from_hours(1),
            fence,
        );
        assert!(!catalog.has_pending_revalidation(&style_id));
    }

    #[test]
    fn renderer_observation_advances_matching_ingress_transition() {
        let style_id = StyleId("demo/basic".to_owned());
        let old = style_content_version(r#"{"version":8,"layers":[]}"#);
        let new = style_content_version(r#"{"version":8,"name":"new","layers":[]}"#);
        let long = Duration::from_hours(1);

        let renderer = StyleCatalog::new();
        renderer.set_url_template("https://styles.test/{style_id}/style.json");
        renderer.record_observed(&style_id, old, long);
        renderer.request_revalidation_for_hint(&style_id, "mutation-2");
        let fence = renderer.revalidation_fence(&style_id);
        renderer.record_observed_for_generation(&style_id, new, long, fence);
        let encoded = renderer.revision_gossip_kvs();
        assert_eq!(encoded.len(), 1);
        let observation = StyleRevisionObservation::decode(&encoded[0].1).unwrap();

        let ingress = StyleCatalog::new();
        ingress.set_url_template("https://styles.test/{style_id}/style.json");
        ingress.record_observed(&style_id, old, long);
        ingress.request_revalidation_for_hint(&style_id, "mutation-2");
        assert!(ingress.apply_cluster_observation(&observation));
        assert_eq!(ingress.resolve_latest(&style_id), Some(new));
        assert!(ingress.accepts_revision(&StyleRevision {
            id: style_id.clone(),
            version: old,
        }));
        assert!(
            ingress
                .definition_for_revision(&StyleRevision {
                    id: style_id.clone(),
                    version: new,
                })
                .is_some()
        );
        assert!(
            ingress.needs_revalidation(&style_id),
            "peer evidence advances routing without pretending local content was fetched"
        );

        let fence = ingress.revalidation_fence(&style_id);
        ingress.record_observed_for_generation(&style_id, new, long, fence);
        assert!(!ingress.needs_revalidation(&style_id));
    }

    #[test]
    fn an_observation_for_an_old_hint_cannot_roll_back_a_newer_transition() {
        let style_id = StyleId("demo/basic".to_owned());
        let old = style_content_version(r#"{"version":8,"name":"old"}"#);
        let new = style_content_version(r#"{"version":8,"name":"new"}"#);
        let ingress = StyleCatalog::new();
        ingress.set_url_template("https://styles.test/{style_id}/style.json");
        ingress.record_observed(&style_id, old, Duration::from_hours(1));
        ingress.request_revalidation_for_hint(&style_id, "mutation-new");

        let stale = StyleRevisionObservation::new("mutation-old".to_owned(), &style_id, old);
        assert!(!ingress.apply_cluster_observation(&stale));
        assert_eq!(ingress.resolve_latest(&style_id), Some(old));

        let current = StyleRevisionObservation::new("mutation-new".to_owned(), &style_id, new);
        assert!(ingress.apply_cluster_observation(&current));
        assert_eq!(ingress.resolve_latest(&style_id), Some(new));

        ingress.request_revalidation_for_hint(&style_id, "mutation-next");
        assert!(!ingress.apply_cluster_observation(&current));
        assert_eq!(ingress.resolve_latest(&style_id), Some(new));
    }
}
