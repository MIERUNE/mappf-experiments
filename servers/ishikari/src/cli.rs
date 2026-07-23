//! Ishikari's command-line and environment contract.

use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::time::Duration;

use clap::Parser;

use crate::options::{
    DEFAULT_BACKEND_ACTIVE_BODY_BUDGET_BYTES, DEFAULT_CACHE_WEIGHT_BUDGET_BYTES,
    DEFAULT_CHUNK_CACHE_MAX_BYTES, DEFAULT_TILE_CACHE_MAX_BYTES, Options, OptionsInput,
};

/// CLI flags and environment variables for configuring the server.
#[derive(Parser, Debug)]
struct Cli {
    /// Optional delivery-auth registries as `registry_id=auth-root;...`.
    /// Each root contains a `current.json` registry snapshot.
    #[arg(long, env = "ISKR_AUTH_REGISTRIES", default_value = "")]
    auth_registries: String,
    #[arg(
        long = "gossip-seeds",
        env = "ISKR_GOSSIP_SEEDS",
        value_delimiter = ',',
        value_name = "ADDR"
    )]
    gossip_seeds: Option<Vec<String>>,
    #[arg(long, env = "ISKR_NODE_ID")]
    node_id: Option<String>,
    #[arg(long = "gossip-advertise-addr", env = "ISKR_GOSSIP_ADVERTISE_ADDR")]
    gossip_advertise_addr: Option<SocketAddr>,
    #[arg(
        long = "internal-http-advertise-addr",
        env = "ISKR_INTERNAL_HTTP_ADVERTISE_ADDR"
    )]
    internal_http_advertise_addr: Option<SocketAddr>,
    #[arg(
        long = "gossip-bind",
        env = "ISKR_GOSSIP_BIND",
        default_value = "0.0.0.0:7946"
    )]
    gossip_bind: SocketAddr,
    #[arg(long, env = "ISKR_HTTP_PORT", default_value_t = 8080)]
    http_port: u16,
    /// Cluster-internal port: metrics, `/_internal/*` and peer-to-peer
    /// forwarding. Served on a separate listener, never exposed via the Gateway.
    #[arg(
        long = "internal-http-port",
        env = "ISKR_INTERNAL_HTTP_PORT",
        default_value_t = 9090
    )]
    internal_http_port: u16,
    /// Mark this node as part of a multi-node cluster. Rejects wildcard
    /// advertise addresses even with no `--gossip-seeds` (a seed node that others will
    /// join must publish a routable address). Implied whenever `--gossip-seeds` is set.
    #[arg(long, env = "ISKR_CLUSTER", default_value_t = false)]
    cluster: bool,
    /// Hold startup readiness until another gossip node is observed. The gate
    /// fails open after a bounded grace period and never re-closes.
    #[arg(
        long = "require-gossip-bootstrap",
        env = "ISKR_REQUIRE_GOSSIP_BOOTSTRAP",
        default_value_t = false
    )]
    require_gossip_bootstrap: bool,
    /// Semicolon-separated namespace roots or URL templates. A default template
    /// without `{namespace}` expands `{tileset_id}` to the complete logical id;
    /// other templates may use `{namespace}` as an optional whole segment.
    #[arg(long, env = "ISKR_TILESET_SOURCES", default_value = "data")]
    tileset_sources: String,
    #[arg(long, env = "ISKR_ROUTER_TOP_K", default_value_t = 3)]
    router_candidate_count: usize,
    #[arg(long, env = "ISKR_ROUTER_TILE_GROUP_SIZE", default_value_t = 512)]
    router_tile_group_size: u64,
    #[arg(long, env = "ISKR_GOSSIP_INTERVAL_MS", default_value_t = 200)]
    gossip_interval_ms: u64,
    #[arg(long, env = "ISKR_CHUNK_SIZE_BYTES", default_value_t = 1 * 1024 * 1024)]
    chunk_size_bytes: u64,
    #[arg(long, env = "ISKR_MAX_FETCH_CHUNKS", default_value_t = 4)]
    max_fetch_chunks: u64,
    /// Scheduler delay used to collect nearby missing chunks before dispatch.
    /// Zero removes the intentional delay while preserving pending/inflight sharing.
    #[arg(
        long = "chunk-fetch-merge-window-ms",
        env = "ISKR_CHUNK_FETCH_MERGE_WINDOW_MS",
        default_value_t = 10
    )]
    chunk_fetch_merge_window_ms: u64,
    /// Process-wide object-storage range-fetch limit. This complements the
    /// per-tileset coordinator cap and prevents distinct-id enumeration from
    /// multiplying backend concurrency.
    #[arg(
        long = "backend-fetch-concurrency",
        env = "ISKR_BACKEND_FETCH_CONCURRENCY",
        default_value_t = 32
    )]
    backend_fetch_concurrency: usize,
    /// Maximum backend range-fetch groups admitted per process, including
    /// operations waiting for the active-I/O limit. Excess distinct work is
    /// shed with 503; callers joining an admitted group still coalesce.
    /// Defaults to four times backend fetch concurrency.
    #[arg(
        long = "backend-fetch-max-inflight",
        env = "ISKR_BACKEND_FETCH_MAX_INFLIGHT"
    )]
    backend_fetch_max_inflight: Option<usize>,
    /// Startup ceiling for the largest possible set of concurrently active
    /// object-store response bodies. This reserve is separate from cache weight.
    #[arg(
        long = "backend-active-body-budget-bytes",
        env = "ISKR_BACKEND_ACTIVE_BODY_BUDGET_BYTES",
        default_value_t = DEFAULT_BACKEND_ACTIVE_BODY_BUDGET_BYTES
    )]
    backend_active_body_budget_bytes: u64,
    #[arg(
        long = "artificial-backend-delay-ms",
        env = "ISKR_ARTIFICIAL_BACKEND_DELAY_MS",
        default_value_t = 0
    )]
    artificial_backend_delay_ms: u64,
    /// Aggregate ceiling for all byte-weighted material caches in one process.
    /// This is not an RSS limit: leave container headroom for keys, inflight
    /// bodies, decompression, CPU jobs, the runtime, and allocator overhead.
    #[arg(
        long,
        env = "ISKR_CACHE_WEIGHT_BUDGET_BYTES",
        default_value_t = DEFAULT_CACHE_WEIGHT_BUDGET_BYTES
    )]
    cache_weight_budget_bytes: u64,
    #[arg(
        long,
        env = "ISKR_TILE_CACHE_MAX_BYTES",
        default_value_t = DEFAULT_TILE_CACHE_MAX_BYTES
    )]
    tile_cache_max_bytes: u64,
    #[arg(
        long,
        env = "ISKR_CHUNK_CACHE_MAX_BYTES",
        default_value_t = DEFAULT_CHUNK_CACHE_MAX_BYTES
    )]
    chunk_cache_max_bytes: u64,
    /// Seconds a negative (tile-absent) L1 cache entry lives before the tile is
    /// re-resolved. Kept short so a republished archive's newly-added tiles
    /// surface quickly and lookups of not-yet-existing tiles cannot poison the
    /// cache to delay their rollout. Positive entries are unaffected.
    #[arg(
        long = "tile-negative-ttl",
        env = "ISKR_TILE_NEGATIVE_TTL",
        default_value_t = 60
    )]
    tile_negative_ttl_secs: u64,
    #[arg(long, env = "ISKR_STYLE_TEMPLATES")]
    style_templates: Option<String>,
    #[arg(long, env = "ISKR_GLYPH_URL_TEMPLATE")]
    glyph_url_template: Option<String>,
    #[arg(long, env = "ISKR_SPRITE_TEMPLATES")]
    sprite_templates: Option<String>,
    /// Logical tileset key (e.g. `mapterhorn/planet`) to serve as a Mapterhorn
    /// composite: z<=12 from the base archive, z>12 from `6-{x6}-{y6}` detail
    /// archives in the same namespace. Unset disables composite serving.
    #[arg(long = "mapterhorn-tileset", env = "ISKR_MAPTERHORN_TILESET")]
    mapterhorn_tileset: Option<String>,
    /// Max zoom advertised in the composite tileset's TileJSON (detail coverage).
    #[arg(long = "mapterhorn-maxzoom", env = "ISKR_MAPTERHORN_MAXZOOM")]
    mapterhorn_maxzoom: Option<u8>,
    /// Seconds an absent Mapterhorn detail archive stays negative-cached. Detail
    /// coverage rarely changes, so a long TTL keeps probes off the hot path.
    #[arg(
        long = "mapterhorn-negative-ttl",
        env = "ISKR_MAPTERHORN_NEGATIVE_TTL",
        default_value_t = 3600
    )]
    mapterhorn_negative_ttl_secs: u64,
    /// Maximum number of CPU-heavy DEM decode, terrain generation, and MLT
    /// transcode jobs running concurrently per pod. Defaults to the effective
    /// parallelism reported by the runtime (including cgroup limits where the
    /// platform exposes them).
    #[arg(long = "cpu-work-concurrency", env = "ISKR_CPU_WORK_CONCURRENCY")]
    cpu_work_concurrency: Option<usize>,
    /// Maximum CPU-work units (terrain generation, DEM decode, MLT transcode)
    /// admitted at once — holding a concurrency permit or queued for one.
    /// Requests beyond this are shed with 503 so an extreme flood fails fast
    /// instead of growing the queue without bound. Defaults to 64x the CPU-work
    /// concurrency.
    #[arg(long = "cpu-work-max-inflight", env = "ISKR_CPU_WORK_MAX_INFLIGHT")]
    cpu_work_max_inflight: Option<usize>,
}

/// Parse CLI arguments and environment variables into runtime configuration.
pub(crate) fn load() -> anyhow::Result<Options> {
    resolve(Cli::parse()).map_err(anyhow::Error::msg)
}

/// Resolve derived settings and validate parsed CLI input.
fn resolve(cli: Cli) -> Result<Options, String> {
    // Use an explicit node id when configured (Kubernetes passes the pod
    // name); otherwise generate one. HOSTNAME is not auto-used because the
    // local dev cluster runs several nodes on one host.
    let node_id = cli
        .node_id
        .filter(|value| !value.is_empty())
        .unwrap_or_else(auto_node_id);
    let cpu_work_concurrency = cli
        .cpu_work_concurrency
        .unwrap_or_else(default_cpu_work_concurrency)
        .max(1);

    Options::resolve(OptionsInput {
        auth_registries: cli.auth_registries,
        node_id,
        gossip_seeds: cli.gossip_seeds.unwrap_or_default(),
        gossip_advertise_addr: cli.gossip_advertise_addr,
        internal_http_advertise_addr: cli.internal_http_advertise_addr,
        gossip_bind: cli.gossip_bind,
        http_port: cli.http_port,
        internal_http_port: cli.internal_http_port,
        cluster: cli.cluster,
        require_gossip_bootstrap: cli.require_gossip_bootstrap,
        tileset_sources: cli.tileset_sources,
        router_candidate_count: cli.router_candidate_count,
        router_tile_group_size: cli.router_tile_group_size,
        gossip_interval_ms: cli.gossip_interval_ms,
        chunk_size_bytes: cli.chunk_size_bytes,
        max_fetch_chunks: cli.max_fetch_chunks,
        chunk_fetch_merge_window_ms: cli.chunk_fetch_merge_window_ms,
        backend_fetch_concurrency: cli.backend_fetch_concurrency,
        backend_fetch_max_inflight: cli.backend_fetch_max_inflight,
        backend_active_body_budget_bytes: cli.backend_active_body_budget_bytes,
        artificial_backend_delay_ms: cli.artificial_backend_delay_ms,
        cache_weight_budget_bytes: cli.cache_weight_budget_bytes,
        tile_cache_max_bytes: cli.tile_cache_max_bytes,
        chunk_cache_max_bytes: cli.chunk_cache_max_bytes,
        tile_negative_ttl_secs: cli.tile_negative_ttl_secs,
        style_templates: cli.style_templates,
        glyph_url_template: cli.glyph_url_template,
        sprite_templates: cli.sprite_templates,
        mapterhorn_tileset: cli.mapterhorn_tileset,
        mapterhorn_maxzoom: cli.mapterhorn_maxzoom,
        mapterhorn_negative_ttl_secs: cli.mapterhorn_negative_ttl_secs,
        cpu_work_concurrency,
        cpu_work_max_inflight: cli.cpu_work_max_inflight,
    })
}

/// Generates a process-local node id for ad-hoc local runs.
fn auto_node_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("node-{}-{now}", std::process::id())
}

fn default_cpu_work_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli() -> Cli {
        Cli {
            auth_registries: String::new(),
            gossip_seeds: None,
            node_id: Some("node-a".to_string()),
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
            cpu_work_concurrency: Some(2),
            cpu_work_max_inflight: None,
        }
    }

    #[test]
    fn cluster_seed_node_requires_routable_advertise_addresses() {
        let mut cli = cli();
        cli.cluster = true;

        let err = match resolve(cli) {
            Ok(_) => panic!("wildcard advertise is invalid in cluster mode"),
            Err(err) => err,
        };
        assert!(err.contains("gossip advertise address"));
    }

    #[test]
    fn seeds_imply_cluster_validation() {
        let mut cli = cli();
        cli.gossip_seeds = Some(vec!["ishikari-gossip:7946".to_string()]);

        let err = match resolve(cli) {
            Ok(_) => panic!("seeds require routable advertise addresses"),
            Err(err) => err,
        };
        assert!(err.contains("gossip advertise address"));
    }

    #[test]
    fn local_single_node_allows_wildcard_listeners() {
        let config = resolve(cli()).expect("local single-node wildcard bind is allowed");

        assert_eq!(config.internal_listen_addr, "0.0.0.0:9090".parse().unwrap());
        assert!(config.membership.seed_nodes.is_empty());
        assert_eq!(config.cpu_work_concurrency, 2);
    }

    #[test]
    fn cpu_work_concurrency_is_always_positive() {
        assert!(default_cpu_work_concurrency() >= 1);

        let mut cli = cli();
        cli.cpu_work_concurrency = Some(0);
        let config = resolve(cli).expect("zero concurrency is clamped");
        assert_eq!(config.cpu_work_concurrency, 1);
    }

    #[test]
    fn zero_chunk_fetch_merge_window_is_allowed() {
        let mut cli = cli();
        cli.chunk_fetch_merge_window_ms = 0;
        let config = resolve(cli).expect("zero merge window is valid");
        assert_eq!(
            config.resolver_tuning.chunk_fetch_merge_window(),
            Duration::ZERO
        );
    }

    #[test]
    fn backend_fetch_concurrency_is_always_positive() {
        let mut cli = cli();
        cli.backend_fetch_concurrency = 0;
        let config = resolve(cli).expect("zero concurrency is clamped");
        assert_eq!(config.resolver_tuning.backend_fetch_concurrency(), 1);
        assert_eq!(config.resolver_tuning.backend_fetch_max_inflight(), 4);
    }

    #[test]
    fn backend_fetch_max_inflight_is_never_below_concurrency() {
        let mut cli = cli();
        cli.backend_fetch_concurrency = 8;
        cli.backend_fetch_max_inflight = Some(1);
        let config = resolve(cli).expect("ceiling is clamped");
        assert_eq!(config.resolver_tuning.backend_fetch_max_inflight(), 8);
    }

    #[test]
    fn cpu_work_max_inflight_defaults_to_multiple_of_concurrency() {
        let mut cli = cli();
        cli.cpu_work_concurrency = Some(4);
        cli.cpu_work_max_inflight = None;
        let config = resolve(cli).expect("defaults resolve");
        // Generous headroom over the concurrency so normal viewport bursts pass.
        assert_eq!(config.cpu_work_max_inflight, 4 * 64);
    }

    #[test]
    fn cpu_work_max_inflight_is_never_below_concurrency() {
        let mut cli = cli();
        cli.cpu_work_concurrency = Some(8);
        // A too-small explicit ceiling is clamped up to the concurrency so at
        // least every permit holder is admissible.
        cli.cpu_work_max_inflight = Some(1);
        let config = resolve(cli).expect("ceiling is clamped");
        assert_eq!(config.cpu_work_max_inflight, 8);
    }

    #[test]
    fn cluster_allows_seed_node_with_routable_advertise_addresses() {
        let mut cli = cli();
        cli.cluster = true;
        cli.gossip_advertise_addr = Some("127.0.0.1:7946".parse().unwrap());
        cli.internal_http_advertise_addr = Some("127.0.0.1:9090".parse().unwrap());

        let config = resolve(cli).expect("routable seed-node advertise addresses work");
        assert_eq!(
            config.membership.gossip_endpoint.advertise_addr(),
            "127.0.0.1:7946".parse().unwrap()
        );
        assert_eq!(
            config.membership.http_advertise_addr,
            "127.0.0.1:9090".parse().unwrap()
        );
        assert!(config.membership.seed_nodes.is_empty());
    }
}
