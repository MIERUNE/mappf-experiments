//! Bounded, advisory cache-refresh hints carried through membership metadata.
//!
//! Hints contain a logical style id, never a URL, credential, body, or trusted
//! revision. A receiver merely makes its next ordinary provider lookup
//! revalidate; that lookup remains responsible for authorization, validation,
//! and activation. The fixed key ring bounds Chitchat state even if a publisher
//! is noisy. Ring overwrite is acceptable because provider TTL/ETag polling is
//! the correctness path.
//!
//! # Identifier envelope
//!
//! The transport and all receivers share the canonical, bounded
//! `namespace/style_id` identity from `mmpf-http`. Validating the same grammar
//! before gossip publication prevents a hint that no receiver can apply.

use std::array;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use mmpf_http::style_key::StyleKey;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::LiveNodesRef;

const KEY_PREFIX: &str = "refresh-style-v1-";
pub(crate) const STYLE_REFRESH_HINT_SLOTS: usize = 16;
pub const MAX_STYLE_REFRESH_HINT_BYTES: usize = 1_024;
const MAX_HINT_ID_BYTES: usize = 128;
/// How long an accepted hint suppresses a repeat of the same `(hint_id,
/// style_id)`.
///
/// A `hint_id` identifies one publisher mutation, so re-arrival of the same pair
/// is a retry by construction — a lost response, a proxy retry, or a fan-out to
/// several pods — never a second intentional refresh. The window only has to
/// outlive a publisher's retry sequence.
const HINT_DEDUP_WINDOW: Duration = Duration::from_mins(5);
/// Bounded so a noisy or hostile publisher cannot grow this map. Eviction is
/// oldest-first, and evicting early only re-admits a retry, which is the
/// pre-existing behavior rather than a new failure.
const HINT_DEDUP_CAPACITY: usize = 1_024;
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StyleRefreshHint {
    pub schema_version: u8,
    pub hint_id: String,
    pub style_id: String,
}

impl StyleRefreshHint {
    pub const SCHEMA_VERSION: u8 = 1;

    pub fn new(
        hint_id: impl Into<String>,
        style_id: impl Into<String>,
    ) -> Result<Self, RefreshHintError> {
        let hint = Self {
            schema_version: Self::SCHEMA_VERSION,
            hint_id: hint_id.into(),
            style_id: style_id.into(),
        };
        hint.validate()?;
        Ok(hint)
    }

    pub fn validate(&self) -> Result<(), RefreshHintError> {
        if self.schema_version != Self::SCHEMA_VERSION {
            return Err(RefreshHintError::UnsupportedVersion);
        }
        if self.hint_id.is_empty()
            || self.hint_id.len() > MAX_HINT_ID_BYTES
            || !self
                .hint_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(RefreshHintError::InvalidHintId);
        }
        validate_style_id(&self.style_id)
    }

    pub fn encode(&self) -> Result<String, RefreshHintError> {
        self.validate()?;
        let encoded = serde_json::to_string(self).map_err(|_| RefreshHintError::InvalidEncoding)?;
        if encoded.len() > MAX_STYLE_REFRESH_HINT_BYTES {
            return Err(RefreshHintError::TooLarge);
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &str) -> Result<Self, RefreshHintError> {
        if encoded.len() > MAX_STYLE_REFRESH_HINT_BYTES {
            return Err(RefreshHintError::TooLarge);
        }
        let hint =
            serde_json::from_str::<Self>(encoded).map_err(|_| RefreshHintError::InvalidEncoding)?;
        hint.validate()?;
        Ok(hint)
    }

    pub fn gossip_key(&self) -> String {
        style_refresh_hint_key(stable_slot(&self.hint_id))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshHintError {
    UnsupportedVersion,
    InvalidHintId,
    InvalidStyleId,
    InvalidEncoding,
    TooLarge,
}

impl std::fmt::Display for RefreshHintError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedVersion => "unsupported refresh hint schema version",
            Self::InvalidHintId => "invalid refresh hint id",
            Self::InvalidStyleId => "invalid refresh style id",
            Self::InvalidEncoding => "invalid refresh hint encoding",
            Self::TooLarge => "refresh hint is too large",
        })
    }
}

impl std::error::Error for RefreshHintError {}

pub(crate) fn style_refresh_hint_key(slot: usize) -> String {
    debug_assert!(slot < STYLE_REFRESH_HINT_SLOTS);
    format!("{KEY_PREFIX}{slot:02}")
}

/// Tracks the last value observed in every node/slot pair.
///
/// Memory is bounded by live nodes times `STYLE_REFRESH_HINT_SLOTS`. The first
/// snapshot is intentionally delivered so a pod joining just after a
/// publication still receives the retained hints.
pub struct StyleRefreshHintTracker {
    seen: BTreeMap<(String, usize), String>,
    keys: [String; STYLE_REFRESH_HINT_SLOTS],
}

impl Default for StyleRefreshHintTracker {
    fn default() -> Self {
        Self {
            seen: BTreeMap::new(),
            keys: array::from_fn(style_refresh_hint_key),
        }
    }
}

impl StyleRefreshHintTracker {
    pub fn observe(
        &mut self,
        nodes: LiveNodesRef<'_>,
        excluded_node_id: Option<&str>,
    ) -> RefreshHintBatch {
        let mut previous = std::mem::take(&mut self.seen);
        let mut current = BTreeMap::new();
        let mut hints = Vec::new();
        let mut invalid = 0;

        for node in nodes.live_logical_nodes() {
            if excluded_node_id == Some(node.id()) {
                continue;
            }
            for (slot, key) in self.keys.iter().enumerate() {
                let Some(value) = node.get(key) else {
                    continue;
                };
                let identity = (node.id().to_string(), slot);
                if let Some((_, seen)) = previous.remove_entry(&identity)
                    && seen == value
                {
                    current.insert(identity, seen);
                    continue;
                }
                current.insert(identity, value.to_string());
                match StyleRefreshHint::decode(value) {
                    Ok(hint) => hints.push(hint),
                    Err(_) => invalid += 1,
                }
            }
        }
        self.seen = current;
        RefreshHintBatch { hints, invalid }
    }
}

pub struct RefreshHintBatch {
    pub hints: Vec<StyleRefreshHint>,
    pub invalid: usize,
}

/// Whether a hint was newly accepted or is a repeat inside the dedup window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HintAdmission {
    /// First arrival: apply it and publish it.
    Accepted,
    /// A repeat of an already-applied `(hint_id, style_id)`. The caller should
    /// answer as it did the first time and do nothing else.
    Duplicate,
}

/// Bounded, time-limited suppression of repeated refresh hints.
///
/// HTTP and gossip receivers share one instance per process. The slot tracker
/// suppresses unchanged gossip values, while this also catches the same hint
/// arriving through both transports or through more than one peer. Without it,
/// every duplicate can invalidate concurrent in-flight work and keep a hot style
/// re-fetching.
///
/// Suppression is per process. Two pods each admit the same hint once, which is
/// correct — each has its own caches to invalidate.
pub struct RefreshHintDedup {
    state: Mutex<DedupState>,
    window: Duration,
    capacity: usize,
}

#[derive(Default)]
struct DedupState {
    /// Insertion-ordered, so expiry is a prefix scan and capacity eviction is a
    /// pop from the front.
    order: VecDeque<(DedupKey, Instant)>,
    present: HashSet<DedupKey>,
}

type DedupKey = (String, String);

impl Default for RefreshHintDedup {
    fn default() -> Self {
        Self::new(HINT_DEDUP_WINDOW, HINT_DEDUP_CAPACITY)
    }
}

impl RefreshHintDedup {
    #[must_use]
    pub fn new(window: Duration, capacity: usize) -> Self {
        Self {
            state: Mutex::new(DedupState::default()),
            window,
            capacity: capacity.max(1),
        }
    }

    /// Records the hint and reports whether this is its first arrival.
    pub fn admit(&self, hint: &StyleRefreshHint) -> HintAdmission {
        let key = (hint.hint_id.clone(), hint.style_id.clone());
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Drop expired entries from the front before deciding, so a repeat after
        // the window is admitted rather than suppressed forever.
        while state
            .order
            .front()
            .is_some_and(|(_, inserted_at)| now.duration_since(*inserted_at) >= self.window)
        {
            let Some((expired, _)) = state.order.pop_front() else {
                break;
            };
            state.present.remove(&expired);
        }

        if state.present.contains(&key) {
            return HintAdmission::Duplicate;
        }
        state.present.insert(key.clone());
        state.order.push_back((key, now));
        while state.order.len() > self.capacity {
            if let Some((evicted, _)) = state.order.pop_front() {
                state.present.remove(&evicted);
            }
        }
        HintAdmission::Accepted
    }
}

fn stable_slot(value: &str) -> usize {
    // FNV-1a is sufficient here: this chooses a bounded retention slot, not a
    // security boundary or ownership assignment.
    let hash = value.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    });
    hash as usize % STYLE_REFRESH_HINT_SLOTS
}

fn validate_style_id(style_id: &str) -> Result<(), RefreshHintError> {
    StyleKey::parse(style_id)
        .map(|_| ())
        .map_err(|_| RefreshHintError::InvalidStyleId)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mmpf_http::style_key::{MAX_LOCAL_STYLE_ID_BYTES, MAX_STYLE_NAMESPACE_BYTES};
    use std::{net::SocketAddr, time::Duration};

    use crate::{ClusterOwner, Config, GossipEndpoint};

    #[test]
    fn wire_round_trip_is_bounded_and_strict() {
        let hint = StyleRefreshHint::new("mutation-42", "mierune/basic").unwrap();
        assert_eq!(
            StyleRefreshHint::decode(&hint.encode().unwrap()).unwrap(),
            hint
        );
        assert!(
            StyleRefreshHint::decode(
                r#"{"schema_version":1,"hint_id":"x","style_id":"a","url":"https://x"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_paths_credentials_and_unbounded_ids() {
        for style_id in [
            "",
            "/a",
            "a/",
            "a//b",
            "a/../b",
            "a/./b",
            "a?access_token=secret",
            "a%2fb",
            "a b",
            "a\\b",
        ] {
            assert!(
                StyleRefreshHint::new("hint", style_id).is_err(),
                "{style_id}"
            );
        }
        assert!(StyleRefreshHint::new("hint", "a/b-c_1").is_ok());
        assert!(StyleRefreshHint::new("bad/id", "a").is_err());
        assert!(StyleRefreshHint::new("hint", "default/basic/extra").is_err());
    }

    /// The envelope and both receivers share the canonical public identity.
    #[test]
    fn envelope_accepts_the_maximal_canonical_style_key() {
        let longest = format!(
            "{}/{}",
            "n".repeat(MAX_STYLE_NAMESPACE_BYTES),
            "s".repeat(MAX_LOCAL_STYLE_ID_BYTES)
        );
        assert!(StyleRefreshHint::new("hint", longest).is_ok());
    }

    /// The gossip value budget has to cover a maximal hint, or the largest
    /// legitimate ids would validate and then fail to encode.
    #[test]
    fn a_maximal_hint_still_fits_the_gossip_value_budget() {
        let hint = StyleRefreshHint::new(
            "h".repeat(MAX_HINT_ID_BYTES),
            format!(
                "{}/{}",
                "n".repeat(MAX_STYLE_NAMESPACE_BYTES),
                "s".repeat(MAX_LOCAL_STYLE_ID_BYTES)
            ),
        )
        .expect("maximal ids are valid");
        let encoded = hint.encode().expect("maximal hint encodes");
        assert!(
            encoded.len() <= MAX_STYLE_REFRESH_HINT_BYTES,
            "{} exceeds {MAX_STYLE_REFRESH_HINT_BYTES}",
            encoded.len()
        );
        assert_eq!(StyleRefreshHint::decode(&encoded).unwrap(), hint);
    }

    /// The whole point: a retried publish must not invalidate twice.
    #[tokio::test(start_paused = true)]
    async fn a_repeated_hint_is_suppressed_within_the_window() {
        let dedup = RefreshHintDedup::default();
        let hint = StyleRefreshHint::new("mutation-42", "mierune/basic").unwrap();
        assert_eq!(dedup.admit(&hint), HintAdmission::Accepted);
        assert_eq!(dedup.admit(&hint), HintAdmission::Duplicate);
        assert_eq!(dedup.admit(&hint), HintAdmission::Duplicate);
    }

    /// A different mutation for the same style, and the same mutation naming a
    /// different style, are both distinct work.
    #[tokio::test(start_paused = true)]
    async fn suppression_keys_on_both_hint_id_and_style_id() {
        let dedup = RefreshHintDedup::default();
        let first = StyleRefreshHint::new("mutation-1", "mierune/basic").unwrap();
        let other_mutation = StyleRefreshHint::new("mutation-2", "mierune/basic").unwrap();
        let other_style = StyleRefreshHint::new("mutation-1", "mierune/dark").unwrap();
        assert_eq!(dedup.admit(&first), HintAdmission::Accepted);
        assert_eq!(dedup.admit(&other_mutation), HintAdmission::Accepted);
        assert_eq!(dedup.admit(&other_style), HintAdmission::Accepted);
        assert_eq!(dedup.admit(&first), HintAdmission::Duplicate);
    }

    /// Suppression must expire, or a genuinely re-issued hint would be ignored
    /// for the life of the process.
    #[tokio::test(start_paused = true)]
    async fn suppression_expires_with_the_window() {
        let window = Duration::from_secs(30);
        let dedup = RefreshHintDedup::new(window, 16);
        let hint = StyleRefreshHint::new("mutation-42", "mierune/basic").unwrap();
        assert_eq!(dedup.admit(&hint), HintAdmission::Accepted);

        tokio::time::advance(window.checked_sub(Duration::from_millis(1)).unwrap()).await;
        assert_eq!(dedup.admit(&hint), HintAdmission::Duplicate);

        tokio::time::advance(Duration::from_millis(2)).await;
        assert_eq!(
            dedup.admit(&hint),
            HintAdmission::Accepted,
            "a hint re-issued after the window is real work again"
        );
    }

    /// A hostile or noisy publisher must not grow the map. Evicting early only
    /// re-admits a retry, which is no worse than having no dedup at all.
    #[tokio::test(start_paused = true)]
    async fn capacity_is_bounded_and_evicts_oldest_first() {
        let dedup = RefreshHintDedup::new(Duration::from_mins(5), 4);
        let hints: Vec<_> = (0..5)
            .map(|index| {
                StyleRefreshHint::new(format!("mutation-{index}"), "default/style").unwrap()
            })
            .collect();
        for hint in &hints {
            assert_eq!(dedup.admit(hint), HintAdmission::Accepted);
        }
        // The fifth insert evicted the oldest. Check retention before re-admitting
        // anything: a re-admission itself evicts the next-oldest entry, so the
        // order of these assertions is load-bearing.
        assert_eq!(
            dedup.admit(&hints[4]),
            HintAdmission::Duplicate,
            "the newest entry must still be suppressed"
        );
        assert_eq!(
            dedup.admit(&hints[0]),
            HintAdmission::Accepted,
            "the oldest entry was evicted, so its retry is admitted again"
        );
    }

    #[test]
    fn gossip_slot_and_key_are_stable() {
        let hint = StyleRefreshHint::new("mutation-42", "default/a").unwrap();
        assert_eq!(hint.gossip_key(), hint.gossip_key());
        assert!(hint.gossip_key().starts_with(KEY_PREFIX));
    }

    #[tokio::test]
    async fn tracker_delivers_each_changed_slot_once() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let owner = ClusterOwner::spawn(Config::new(
            "refresh-test",
            "node-a",
            GossipEndpoint::standalone(addr, addr),
            Vec::new(),
            Duration::from_millis(50),
            Duration::from_mins(1),
        ))
        .await
        .unwrap();
        let cluster = owner.handle();
        let mut watcher = cluster.live_nodes_watcher().await;
        let hint = StyleRefreshHint::new("mutation-42", "mierune/basic").unwrap();
        cluster
            .set(&hint.gossip_key(), &hint.encode().unwrap())
            .await;
        tokio::time::timeout(Duration::from_secs(1), watcher.changed())
            .await
            .expect("membership update")
            .expect("watch remains open");

        let mut tracker = StyleRefreshHintTracker::default();
        let first = watcher.inspect(|nodes| tracker.observe(nodes, None));
        assert_eq!(first.hints, vec![hint]);
        assert_eq!(first.invalid, 0);
        let duplicate = watcher.inspect(|nodes| tracker.observe(nodes, None));
        assert!(duplicate.hints.is_empty());
        let excluded = watcher.inspect(|nodes| tracker.observe(nodes, Some("node-a")));
        assert!(excluded.hints.is_empty());

        owner.shutdown().await.unwrap();
    }
}
