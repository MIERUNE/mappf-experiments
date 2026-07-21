//! Resolved Ishikari runtime configuration, independent of CLI and environment access.

use std::{net::SocketAddr, time::Duration};

use crate::{mapterhorn::MapterhornConfig, membership::MembershipConfig, provider::ProviderConfig};
use ishikari_core::storage::{ResolverTuning, ResolverTuningInput};
use mmpf_cluster::GossipEndpoint;
use mmpf_common::resource_templates::{
    NamespaceKeyPolicy, NamespacedEntries, NamespacedEntriesPolicy,
};
use url::Url;

const MIB: u64 = 1024 * 1024;
pub(crate) const DEFAULT_CACHE_WEIGHT_BUDGET_BYTES: u64 = 1024 * MIB;
pub(crate) const DEFAULT_TILE_CACHE_MAX_BYTES: u64 = 256 * MIB;
pub(crate) const DEFAULT_CHUNK_CACHE_MAX_BYTES: u64 = 256 * MIB;
pub(crate) const DEFAULT_BACKEND_ACTIVE_BODY_BUDGET_BYTES: u64 = 128 * MIB;

const RESOURCE_CACHE_MAX_BYTES: u64 = 64 * MIB;
const ARCHIVE_CACHE_MAX_BYTES: u64 = 64 * MIB;
const LEAF_CACHE_MAX_BYTES: u64 = 64 * MIB;
const PROVIDER_CACHE_MAX_BYTES: u64 = 64 * MIB;
const MLT_CACHE_MAX_BYTES: u64 = 64 * MIB;
const DERIVED_TILE_CACHE_MAX_BYTES: u64 = 128 * MIB;
const DEM_TILE_CACHE_MAX_BYTES: u64 = 64 * MIB;

/// Resolved byte-weight ceilings for material caches owned by one process.
///
/// These weights deliberately exclude Moka/hash-map/key overhead, auxiliary
/// entry-count caches, inflight bodies, and working memory. The deployment must
/// reserve RSS headroom beyond `budget_bytes` for those allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CacheCapacities {
    budget_bytes: u64,
    configured_weight_bytes: u64,
    tile_bytes: u64,
    chunk_bytes: u64,
    resource_bytes: u64,
    archive_bytes: u64,
    leaf_bytes: u64,
    provider_bytes: u64,
    mlt_bytes: u64,
    derived_tile_bytes: u64,
    dem_tile_bytes: u64,
}

impl CacheCapacities {
    fn resolve(budget_bytes: u64, tile_bytes: u64, chunk_bytes: u64) -> Result<Self, String> {
        let capacities = [
            tile_bytes,
            chunk_bytes,
            RESOURCE_CACHE_MAX_BYTES,
            ARCHIVE_CACHE_MAX_BYTES,
            LEAF_CACHE_MAX_BYTES,
            PROVIDER_CACHE_MAX_BYTES,
            MLT_CACHE_MAX_BYTES,
            DERIVED_TILE_CACHE_MAX_BYTES,
            DEM_TILE_CACHE_MAX_BYTES,
        ];
        let configured_weight_bytes = capacities
            .into_iter()
            .try_fold(0_u64, |total, value| total.checked_add(value));
        let Some(configured_weight_bytes) = configured_weight_bytes else {
            return Err("configured material-cache weights overflow u64".to_string());
        };
        if configured_weight_bytes > budget_bytes {
            return Err(format!(
                "configured material-cache weight {configured_weight_bytes} bytes exceeds \
                 cache-weight budget {budget_bytes} bytes; reduce ISKR_TILE_CACHE_MAX_BYTES or \
                 ISKR_CHUNK_CACHE_MAX_BYTES, or raise ISKR_CACHE_WEIGHT_BUDGET_BYTES only with \
                 matching container-memory headroom"
            ));
        }
        Ok(Self {
            budget_bytes,
            configured_weight_bytes,
            tile_bytes,
            chunk_bytes,
            resource_bytes: RESOURCE_CACHE_MAX_BYTES,
            archive_bytes: ARCHIVE_CACHE_MAX_BYTES,
            leaf_bytes: LEAF_CACHE_MAX_BYTES,
            provider_bytes: PROVIDER_CACHE_MAX_BYTES,
            mlt_bytes: MLT_CACHE_MAX_BYTES,
            derived_tile_bytes: DERIVED_TILE_CACHE_MAX_BYTES,
            dem_tile_bytes: DEM_TILE_CACHE_MAX_BYTES,
        })
    }

    pub(crate) fn budget_bytes(self) -> u64 {
        self.budget_bytes
    }

    pub(crate) fn configured_weight_bytes(self) -> u64 {
        self.configured_weight_bytes
    }

    pub(crate) fn tile_bytes(self) -> u64 {
        self.tile_bytes
    }

    pub(crate) fn chunk_bytes(self) -> u64 {
        self.chunk_bytes
    }

    pub(crate) fn resource_bytes(self) -> u64 {
        self.resource_bytes
    }

    pub(crate) fn archive_bytes(self) -> u64 {
        self.archive_bytes
    }

    pub(crate) fn leaf_bytes(self) -> u64 {
        self.leaf_bytes
    }

    pub(crate) fn provider_bytes(self) -> u64 {
        self.provider_bytes
    }

    pub(crate) fn mlt_bytes(self) -> u64 {
        self.mlt_bytes
    }

    pub(crate) fn derived_tile_bytes(self) -> u64 {
        self.derived_tile_bytes
    }

    pub(crate) fn dem_tile_bytes(self) -> u64 {
        self.dem_tile_bytes
    }
}

impl Default for CacheCapacities {
    fn default() -> Self {
        Self::resolve(
            DEFAULT_CACHE_WEIGHT_BUDGET_BYTES,
            DEFAULT_TILE_CACHE_MAX_BYTES,
            DEFAULT_CHUNK_CACHE_MAX_BYTES,
        )
        .expect("checked-in cache defaults fit their aggregate budget")
    }
}

/// Validated and derived configuration consumed by the Ishikari runtime.
///
/// Entry points parse their input contract and supply [`OptionsInput`]; this
/// crate validates it and builds `Options`. Non-CLI entry points can therefore
/// run the same production assembly without depending on `clap` or process
/// globals.
#[non_exhaustive]
pub(crate) struct Options {
    pub(crate) http_listen_addr: SocketAddr,
    /// Cluster-internal listener (metrics and peer forwarding).
    pub(crate) internal_listen_addr: SocketAddr,
    pub(crate) membership: MembershipConfig,
    /// Require one initial peer observation before reporting ready. The server
    /// applies a bounded fail-open grace and latches success permanently.
    pub(crate) require_gossip_bootstrap: bool,
    pub(crate) tileset_sources: String,
    pub(crate) tileset_source_inventory: TilesetSourceInventory,
    pub(crate) resolver_tuning: ResolverTuning,
    /// Maximum configured bytes across concurrently active backend bodies.
    pub(crate) backend_max_active_body_bytes: u64,
    /// Startup ceiling for concurrently active backend response bodies.
    pub(crate) backend_active_body_budget_bytes: u64,
    pub(crate) artificial_backend_delay_ms: u64,
    pub(crate) provider: ProviderConfig,
    pub(crate) mapterhorn: Option<MapterhornConfig>,
    pub(crate) cache_capacities: CacheCapacities,
    /// Maximum number of CPU-heavy decode/generate/transcode jobs per pod.
    pub(crate) cpu_work_concurrency: usize,
    /// Maximum admitted CPU-work units before new work is shed.
    pub(crate) cpu_work_max_inflight: usize,
}

/// Bounded, non-sensitive description of configured tileset backends for
/// startup diagnostics. It deliberately retains no URL or namespace text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TilesetSourceInventory {
    source_count: usize,
    has_default: bool,
    backend_kinds: Vec<&'static str>,
}

impl TilesetSourceInventory {
    fn parse(raw: &str) -> Result<Self, String> {
        let entries = NamespacedEntries::parse(
            raw,
            NamespacedEntriesPolicy {
                config_name: "TILESET_SOURCES",
                entry_name: "source",
                namespace_keys: NamespaceKeyPolicy::AsciiIdentifier,
            },
        )
        .map_err(|error| error.to_string())?;
        let has_default = entries.default_value().is_some();
        let mut backend_kinds = entries
            .namespaces()
            .iter()
            .map(|(_, source)| backend_kind(source))
            .chain(entries.default_value().map(|source| backend_kind(source)))
            .collect::<Vec<_>>();
        backend_kinds.sort_unstable();
        backend_kinds.dedup();

        Ok(Self {
            source_count: entries.namespaces().len() + usize::from(has_default),
            has_default,
            backend_kinds,
        })
    }

    pub(crate) fn source_count(&self) -> usize {
        self.source_count
    }

    pub(crate) fn has_default(&self) -> bool {
        self.has_default
    }

    pub(crate) fn backend_kinds(&self) -> &[&'static str] {
        &self.backend_kinds
    }
}

fn backend_kind(source: &str) -> &'static str {
    let Ok(url) = Url::parse(source) else {
        return "file";
    };
    match url.scheme() {
        "file" => "file",
        "memory" => "memory",
        "gs" => "gcs",
        "s3" => "s3",
        "http" => "http",
        "https" => "https",
        _ => "other",
    }
}

/// Unvalidated process configuration supplied by an entry point.
///
/// Units mirror Ishikari's external contract so the core owns all validation
/// and derivation while the CLI remains responsible only for parsing and
/// choosing process-local defaults.
pub(crate) struct OptionsInput {
    pub(crate) node_id: String,
    pub(crate) gossip_seeds: Vec<String>,
    pub(crate) gossip_advertise_addr: Option<SocketAddr>,
    pub(crate) internal_http_advertise_addr: Option<SocketAddr>,
    pub(crate) gossip_bind: SocketAddr,
    pub(crate) http_port: u16,
    pub(crate) internal_http_port: u16,
    pub(crate) cluster: bool,
    pub(crate) require_gossip_bootstrap: bool,
    pub(crate) tileset_sources: String,
    pub(crate) router_candidate_count: usize,
    pub(crate) router_tile_group_size: u64,
    pub(crate) gossip_interval_ms: u64,
    pub(crate) chunk_size_bytes: u64,
    pub(crate) max_fetch_chunks: u64,
    pub(crate) chunk_fetch_merge_window_ms: u64,
    pub(crate) backend_fetch_concurrency: usize,
    pub(crate) backend_fetch_max_inflight: Option<usize>,
    pub(crate) backend_active_body_budget_bytes: u64,
    pub(crate) artificial_backend_delay_ms: u64,
    pub(crate) cache_weight_budget_bytes: u64,
    pub(crate) tile_cache_max_bytes: u64,
    pub(crate) chunk_cache_max_bytes: u64,
    pub(crate) tile_negative_ttl_secs: u64,
    pub(crate) style_templates: Option<String>,
    pub(crate) glyph_url_template: Option<String>,
    pub(crate) sprite_templates: Option<String>,
    pub(crate) mapterhorn_tileset: Option<String>,
    pub(crate) mapterhorn_maxzoom: Option<u8>,
    pub(crate) mapterhorn_negative_ttl_secs: u64,
    pub(crate) cpu_work_concurrency: usize,
    pub(crate) cpu_work_max_inflight: Option<usize>,
}

impl Options {
    /// Validate entry-point configuration and resolve derived addresses and capacities.
    pub(crate) fn resolve(input: OptionsInput) -> Result<Self, String> {
        let tileset_source_inventory =
            TilesetSourceInventory::parse(input.tileset_sources.as_str())?;
        let cache_capacities = CacheCapacities::resolve(
            input.cache_weight_budget_bytes,
            input.tile_cache_max_bytes,
            input.chunk_cache_max_bytes,
        )?;
        let resolver_tuning = ResolverTuningInput {
            candidate_count: input.router_candidate_count,
            tile_group_size: input.router_tile_group_size,
            chunk_size_bytes: input.chunk_size_bytes,
            max_fetch_chunks: input.max_fetch_chunks,
            chunk_fetch_merge_window: Duration::from_millis(input.chunk_fetch_merge_window_ms),
            backend_fetch_concurrency: input.backend_fetch_concurrency,
            backend_fetch_max_inflight: input
                .backend_fetch_max_inflight
                .unwrap_or_else(|| input.backend_fetch_concurrency.max(1).saturating_mul(4)),
            tile_cache_max_bytes: cache_capacities.tile_bytes(),
            chunk_cache_max_bytes: cache_capacities.chunk_bytes(),
            tile_negative_ttl: Duration::from_secs(input.tile_negative_ttl_secs),
        }
        .resolve()
        .map_err(|error| error.to_string())?;
        let backend_max_active_body_bytes = resolve_backend_active_body_bytes(
            resolver_tuning,
            input.backend_active_body_budget_bytes,
        )?;
        let gossip_advertise_addr = input.gossip_advertise_addr.unwrap_or(input.gossip_bind);
        let http_listen_addr = SocketAddr::new(input.gossip_bind.ip(), input.http_port);
        let internal_listen_addr =
            SocketAddr::new(input.gossip_bind.ip(), input.internal_http_port);
        let internal_http_advertise_addr =
            input.internal_http_advertise_addr.unwrap_or_else(|| {
                SocketAddr::new(gossip_advertise_addr.ip(), input.internal_http_port)
            });
        let seed_nodes: Vec<_> = input
            .gossip_seeds
            .into_iter()
            .filter(|value| !value.is_empty())
            .collect();

        // A seed node can participate without joining another seed, so the
        // explicit cluster flag also activates advertise-address validation.
        let clustered = input.cluster || !seed_nodes.is_empty();
        let gossip_endpoint = if clustered {
            GossipEndpoint::clustered(input.gossip_bind, gossip_advertise_addr).map_err(
                |error| {
                    format!(
                        "{error}; set --gossip-advertise-addr (ISKR_GOSSIP_ADVERTISE_ADDR) to a \
                     routable address in cluster mode"
                    )
                },
            )?
        } else {
            GossipEndpoint::standalone(input.gossip_bind, gossip_advertise_addr)
        };
        if clustered && internal_http_advertise_addr.ip().is_unspecified() {
            return Err(format!(
                "internal HTTP advertise address {internal_http_advertise_addr} is a \
                 wildcard; set --internal-http-advertise-addr \
                 (ISKR_INTERNAL_HTTP_ADVERTISE_ADDR) to a routable address in cluster mode"
            ));
        }

        let provider = ProviderConfig::new(
            non_empty(input.style_templates),
            non_empty(input.glyph_url_template),
            non_empty(input.sprite_templates),
        )?;
        let mapterhorn = match non_empty(input.mapterhorn_tileset) {
            Some(tileset) => {
                let maxzoom = input.mapterhorn_maxzoom.ok_or_else(|| {
                    "ISKR_MAPTERHORN_MAXZOOM is required when ISKR_MAPTERHORN_TILESET is set \
                     (the detail archives' max zoom, e.g. 16)"
                        .to_string()
                })?;
                Some(MapterhornConfig::new(
                    &tileset,
                    maxzoom,
                    Duration::from_secs(input.mapterhorn_negative_ttl_secs),
                )?)
            }
            None => None,
        };
        let cpu_work_concurrency = input.cpu_work_concurrency.max(1);
        Ok(Self {
            http_listen_addr,
            internal_listen_addr,
            membership: MembershipConfig {
                node_id: input.node_id,
                gossip_endpoint,
                http_advertise_addr: internal_http_advertise_addr,
                seed_nodes,
                gossip_interval: Duration::from_millis(input.gossip_interval_ms.max(1)),
            },
            require_gossip_bootstrap: input.require_gossip_bootstrap,
            tileset_sources: input.tileset_sources,
            tileset_source_inventory,
            resolver_tuning,
            backend_max_active_body_bytes,
            backend_active_body_budget_bytes: input.backend_active_body_budget_bytes,
            artificial_backend_delay_ms: input.artificial_backend_delay_ms,
            provider,
            mapterhorn,
            cache_capacities,
            cpu_work_concurrency,
            cpu_work_max_inflight: input
                .cpu_work_max_inflight
                .unwrap_or_else(|| cpu_work_concurrency.saturating_mul(64))
                .max(cpu_work_concurrency),
        })
    }
}

fn resolve_backend_active_body_bytes(
    tuning: ResolverTuning,
    budget_bytes: u64,
) -> Result<u64, String> {
    let max_body_bytes = tuning
        .chunk_size_bytes()
        .checked_mul(tuning.max_fetch_chunks())
        .ok_or_else(|| {
            "configured backend body size overflows u64; reduce ISKR_CHUNK_SIZE_BYTES or \
             ISKR_MAX_FETCH_CHUNKS"
                .to_string()
        })?;
    let concurrency = u64::try_from(tuning.backend_fetch_concurrency()).map_err(|_| {
        "configured backend fetch concurrency does not fit u64; reduce \
         ISKR_BACKEND_FETCH_CONCURRENCY"
            .to_string()
    })?;
    let active_body_bytes = max_body_bytes.checked_mul(concurrency).ok_or_else(|| {
        "configured aggregate active backend body size overflows u64; reduce \
         ISKR_CHUNK_SIZE_BYTES, ISKR_MAX_FETCH_CHUNKS, or ISKR_BACKEND_FETCH_CONCURRENCY"
            .to_string()
    })?;
    if active_body_bytes > budget_bytes {
        return Err(format!(
            "configured active backend bodies require up to {active_body_bytes} bytes, exceeding \
             the {budget_bytes}-byte ISKR_BACKEND_ACTIVE_BODY_BUDGET_BYTES reserve; reduce \
             ISKR_CHUNK_SIZE_BYTES, ISKR_MAX_FETCH_CHUNKS, or ISKR_BACKEND_FETCH_CONCURRENCY, or \
             raise the reserve only with matching container-memory headroom"
        ));
    }
    Ok(active_body_bytes)
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input() -> OptionsInput {
        OptionsInput {
            node_id: "node-a".to_string(),
            gossip_seeds: Vec::new(),
            gossip_advertise_addr: None,
            internal_http_advertise_addr: None,
            gossip_bind: "0.0.0.0:7946".parse().unwrap(),
            http_port: 8080,
            internal_http_port: 9090,
            cluster: false,
            require_gossip_bootstrap: false,
            tileset_sources: "data".to_string(),
            router_candidate_count: 3,
            router_tile_group_size: 512,
            gossip_interval_ms: 200,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window_ms: 10,
            backend_fetch_concurrency: 32,
            backend_fetch_max_inflight: None,
            backend_active_body_budget_bytes: DEFAULT_BACKEND_ACTIVE_BODY_BUDGET_BYTES,
            artificial_backend_delay_ms: 0,
            cache_weight_budget_bytes: DEFAULT_CACHE_WEIGHT_BUDGET_BYTES,
            tile_cache_max_bytes: DEFAULT_TILE_CACHE_MAX_BYTES,
            chunk_cache_max_bytes: DEFAULT_CHUNK_CACHE_MAX_BYTES,
            tile_negative_ttl_secs: 60,
            style_templates: None,
            glyph_url_template: None,
            sprite_templates: None,
            mapterhorn_tileset: None,
            mapterhorn_maxzoom: None,
            mapterhorn_negative_ttl_secs: 3600,
            cpu_work_concurrency: 2,
            cpu_work_max_inflight: None,
        }
    }

    #[test]
    fn rejects_zero_chunk_size() {
        let mut input = input();
        input.chunk_size_bytes = 0;

        let error = match Options::resolve(input) {
            Ok(_) => panic!("zero chunk size must be rejected"),
            Err(error) => error,
        };

        assert_eq!(error, "chunk_size_bytes must be greater than zero");
    }

    #[test]
    fn derives_internal_listener_and_bounded_capacities() {
        let mut input = input();
        input.router_candidate_count = 0;
        input.router_tile_group_size = 0;
        input.max_fetch_chunks = 0;
        input.backend_fetch_concurrency = 0;
        input.backend_fetch_max_inflight = Some(0);
        input.cpu_work_concurrency = 0;
        input.cpu_work_max_inflight = Some(0);

        let options = Options::resolve(input).expect("single-node options resolve");

        assert_eq!(
            options.internal_listen_addr,
            "0.0.0.0:9090".parse().unwrap()
        );
        assert_eq!(
            options.membership.http_advertise_addr,
            options.internal_listen_addr
        );
        assert_eq!(
            options.membership.gossip_endpoint.listen_addr(),
            "0.0.0.0:7946".parse().unwrap()
        );
        assert_eq!(options.resolver_tuning.candidate_count(), 1);
        assert_eq!(options.resolver_tuning.tile_group_size(), 1);
        assert_eq!(options.resolver_tuning.max_fetch_chunks(), 1);
        assert_eq!(options.resolver_tuning.backend_fetch_concurrency(), 1);
        assert_eq!(options.resolver_tuning.backend_fetch_max_inflight(), 1);
        assert_eq!(options.cpu_work_concurrency, 1);
        assert_eq!(options.cpu_work_max_inflight, 1);
    }

    #[test]
    fn default_backend_active_bodies_fit_their_reserve() {
        let options = Options::resolve(input()).expect("default options resolve");

        assert_eq!(options.backend_max_active_body_bytes, 128 * MIB);
        assert_eq!(
            options.backend_active_body_budget_bytes,
            DEFAULT_BACKEND_ACTIVE_BODY_BUDGET_BYTES
        );
    }

    #[test]
    fn rejects_backend_active_body_oversubscription() {
        let mut input = input();
        input.backend_active_body_budget_bytes = 127 * MIB;

        let error = Options::resolve(input)
            .err()
            .expect("active backend bodies exceed their reserve");

        assert!(error.contains("active backend bodies require up to 134217728 bytes"));
        assert!(error.contains("133169152-byte ISKR_BACKEND_ACTIVE_BODY_BUDGET_BYTES reserve"));
    }

    #[test]
    fn rejects_backend_active_body_arithmetic_overflow() {
        let mut input = input();
        input.chunk_size_bytes = u64::MAX;
        input.max_fetch_chunks = 2;
        input.backend_active_body_budget_bytes = u64::MAX;

        let error = Options::resolve(input)
            .err()
            .expect("per-fetch body arithmetic must not overflow");

        assert!(error.contains("backend body size overflows u64"));
    }

    #[test]
    fn default_material_cache_weights_fit_one_gibibyte_budget() {
        let options = Options::resolve(input()).expect("default options resolve");
        let caches = options.cache_capacities;

        assert_eq!(caches.budget_bytes(), 1024 * MIB);
        assert_eq!(caches.configured_weight_bytes(), caches.budget_bytes());
        assert_eq!(caches.tile_bytes(), 256 * MIB);
        assert_eq!(caches.chunk_bytes(), 256 * MIB);
        assert_eq!(caches.resource_bytes(), 64 * MIB);
        assert_eq!(caches.archive_bytes(), 64 * MIB);
        assert_eq!(caches.leaf_bytes(), 64 * MIB);
        assert_eq!(caches.provider_bytes(), 64 * MIB);
        assert_eq!(caches.mlt_bytes(), 64 * MIB);
        assert_eq!(caches.derived_tile_bytes(), 128 * MIB);
        assert_eq!(caches.dem_tile_bytes(), 64 * MIB);
    }

    #[test]
    fn aggregate_cache_budget_rejects_individually_valid_oversubscription() {
        let mut input = input();
        input.tile_cache_max_bytes = 512 * MIB;
        input.chunk_cache_max_bytes = 512 * MIB;

        let error = Options::resolve(input)
            .err()
            .expect("old 1.5 GiB cache defaults exceed the 1 GiB budget");

        assert!(error.contains("configured material-cache weight 1610612736 bytes"));
        assert!(error.contains("cache-weight budget 1073741824 bytes"));
    }

    #[test]
    fn larger_cache_budget_requires_an_explicit_matching_override() {
        let mut input = input();
        input.tile_cache_max_bytes = 512 * MIB;
        input.chunk_cache_max_bytes = 512 * MIB;
        input.cache_weight_budget_bytes = 1536 * MIB;

        let options = Options::resolve(input).expect("explicit 1.5 GiB budget resolves");

        assert_eq!(options.cache_capacities.budget_bytes(), 1536 * MIB);
        assert_eq!(
            options.cache_capacities.configured_weight_bytes(),
            1536 * MIB
        );
    }

    #[test]
    fn gossip_bootstrap_requirement_is_explicit() {
        let mut input = input();
        input.require_gossip_bootstrap = true;

        let options = Options::resolve(input).expect("options resolve");

        assert!(options.require_gossip_bootstrap);
    }

    #[test]
    fn cluster_rejects_wildcard_advertise_addresses() {
        let mut input = input();
        input.cluster = true;

        let error = Options::resolve(input)
            .err()
            .expect("cluster wildcard must be rejected");

        assert!(error.contains("gossip advertise address"));
    }

    #[test]
    fn empty_optional_configuration_is_normalized() {
        let mut input = input();
        input.style_templates = Some("  ".to_string());
        input.glyph_url_template = Some(String::new());
        input.sprite_templates = Some("   ".to_string());

        let options = Options::resolve(input).expect("options resolve");

        assert!(options.provider.resolve_style_url("demo").is_none());
        assert!(!options.provider.has_glyph_provider());
        assert!(!options.provider.has_sprite_provider("demo"));
    }

    #[test]
    fn rejects_invalid_provider_templates_at_the_options_boundary() {
        let mut input = input();
        input.glyph_url_template = Some("https://glyphs.example/{fontstack}.pbf".to_string());

        let error = Options::resolve(input)
            .err()
            .expect("missing glyph range placeholder must be rejected");

        assert!(error.contains("template must contain {range}"));
    }

    #[test]
    fn tileset_source_inventory_is_bounded_and_redacts_url_details() {
        let mut input = input();
        input.tileset_sources = concat!(
            "regional=https://alice:super-secret@tiles.example/private?token=signed-secret;",
            "default=s3://private-bucket/prefix?credential=hidden;",
            "local=data"
        )
        .to_string();

        let options = Options::resolve(input).expect("tileset sources resolve");
        let inventory = &options.tileset_source_inventory;

        assert_eq!(inventory.source_count(), 3);
        assert!(inventory.has_default());
        assert_eq!(inventory.backend_kinds(), ["file", "https", "s3"]);
        let diagnostic = format!("{inventory:?}");
        for secret in [
            "alice",
            "super-secret",
            "tiles.example",
            "signed-secret",
            "private-bucket",
            "hidden",
            "regional",
        ] {
            assert!(!diagnostic.contains(secret), "inventory leaked {secret:?}");
        }
    }

    #[test]
    fn validates_mapterhorn_at_the_options_boundary() {
        let mut missing_maxzoom = input();
        missing_maxzoom.mapterhorn_tileset = Some("mapterhorn/planet".to_string());
        let error = Options::resolve(missing_maxzoom)
            .err()
            .expect("mapterhorn maxzoom is required");
        assert!(error.contains("ISKR_MAPTERHORN_MAXZOOM is required"));

        let mut invalid_maxzoom = input();
        invalid_maxzoom.mapterhorn_tileset = Some("mapterhorn/planet".to_string());
        invalid_maxzoom.mapterhorn_maxzoom = Some(12);
        let error = Options::resolve(invalid_maxzoom)
            .err()
            .expect("base zoom cannot be advertised as detail maxzoom");
        assert!(error.contains("mapterhorn maxzoom must be 13..=30"));

        let mut valid = input();
        valid.mapterhorn_tileset = Some("mapterhorn/planet".to_string());
        valid.mapterhorn_maxzoom = Some(16);
        assert!(
            Options::resolve(valid)
                .expect("valid mapterhorn")
                .mapterhorn
                .is_some()
        );
    }
}
