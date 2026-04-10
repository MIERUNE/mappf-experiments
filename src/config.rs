//! Runtime configuration loading from CLI flags and environment variables.

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;

use crate::membership;

/// Resolved application configuration used at startup.
pub struct Config {
    pub node_id: String,
    pub http_port: u16,
    pub http_listen_addr: SocketAddr,
    /// Cluster-internal listener (metrics, peer forwarding); not exposed
    /// through the Gateway.
    pub internal_listen_addr: SocketAddr,
    pub membership: membership::MembershipConfig,
    pub tileset_sources: String,
    pub router_candidate_count: usize,
    pub router_tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub artificial_backend_delay_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
    pub style_templates: Option<String>,
    pub glyph_url_template: Option<String>,
    pub sprite_templates: Option<String>,
    /// Logical tileset key served as a Mapterhorn composite (base `planet`
    /// archive for z<=12, per-region detail archives for z>12). `None` disables
    /// composite serving.
    pub mapterhorn_tileset: Option<String>,
    /// Maximum zoom advertised for the Mapterhorn composite tileset, so clients
    /// request the z13+ tiles backed by detail archives.
    pub mapterhorn_maxzoom: Option<u8>,
    /// How long an absent Mapterhorn detail archive stays negative-cached.
    pub mapterhorn_negative_ttl: Duration,
}

/// CLI flags and environment variables for configuring the server.
#[derive(Parser, Debug)]
pub struct Cli {
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
    #[arg(
        long = "artificial-backend-delay-ms",
        env = "ISKR_ARTIFICIAL_BACKEND_DELAY_MS",
        default_value_t = 0
    )]
    artificial_backend_delay_ms: u64,
    #[arg(long, env = "ISKR_TILE_CACHE_MAX_BYTES", default_value_t = 512 * 1024 * 1024)]
    tile_cache_max_bytes: u64,
    #[arg(long, env = "ISKR_CHUNK_CACHE_MAX_BYTES", default_value_t = 512 * 1024 * 1024)]
    chunk_cache_max_bytes: u64,
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
}

impl Config {
    /// Parses CLI arguments and environment variables into runtime configuration.
    pub fn load() -> Self {
        match Self::from_cli(Cli::parse()) {
            Ok(config) => config,
            Err(message) => {
                eprintln!("configuration error: {message}");
                std::process::exit(2);
            }
        }
    }

    /// Resolves derived settings and defaults from parsed CLI input.
    fn from_cli(cli: Cli) -> Result<Self, String> {
        // Use an explicit node id when configured (Kubernetes passes the pod
        // name); otherwise generate one. HOSTNAME is not auto-used because the
        // local dev cluster runs several nodes on one host.
        let node_id = cli
            .node_id
            .filter(|value| !value.is_empty())
            .unwrap_or_else(auto_node_id);
        let gossip_advertise_addr = cli.gossip_advertise_addr.unwrap_or(cli.gossip_bind);
        let http_listen_addr = SocketAddr::new(cli.gossip_bind.ip(), cli.http_port);
        let internal_listen_addr = SocketAddr::new(cli.gossip_bind.ip(), cli.internal_http_port);
        // The HTTP address peers forward to. `/_internal/*` (peer forwarding)
        // lives on the internal port, so this advertises the internal port —
        // never the Gateway-fronted public port. Published as its own gossip KV
        // rather than reconstructed from the gossip IP, which is fragile across
        // networks.
        let internal_http_advertise_addr = cli
            .internal_http_advertise_addr
            .unwrap_or_else(|| SocketAddr::new(gossip_advertise_addr.ip(), cli.internal_http_port));
        let seed_nodes = cli
            .gossip_seeds
            .filter(|values| !values.is_empty())
            .unwrap_or_default();

        // A node participates in a cluster when it joins peers (`--gossip-seeds`) or is
        // explicitly flagged as one (`--cluster`) — e.g. a seed node others join.
        // In that case a wildcard advertise address is published to peers but is
        // not routable, so reject it early.
        let in_cluster = cli.cluster || !seed_nodes.is_empty();
        if in_cluster {
            if gossip_advertise_addr.ip().is_unspecified() {
                return Err(format!(
                    "gossip advertise address {gossip_advertise_addr} is a wildcard; set --gossip-advertise-addr (ISKR_GOSSIP_ADVERTISE_ADDR) to a routable address in cluster mode"
                ));
            }
            if internal_http_advertise_addr.ip().is_unspecified() {
                return Err(format!(
                    "internal HTTP advertise address {internal_http_advertise_addr} is a wildcard; set --internal-http-advertise-addr (ISKR_INTERNAL_HTTP_ADVERTISE_ADDR) to a routable address in cluster mode"
                ));
            }
        }

        Ok(Self {
            node_id: node_id.clone(),
            http_port: cli.http_port,
            http_listen_addr,
            internal_listen_addr,
            membership: membership::MembershipConfig {
                node_id,
                listen_addr: cli.gossip_bind,
                advertise_addr: gossip_advertise_addr,
                http_advertise_addr: internal_http_advertise_addr,
                http_port: cli.http_port,
                seed_nodes,
                gossip_interval: Duration::from_millis(cli.gossip_interval_ms.max(1)),
            },
            tileset_sources: cli.tileset_sources,
            router_candidate_count: cli.router_candidate_count,
            router_tile_group_size: cli.router_tile_group_size,
            chunk_size_bytes: cli.chunk_size_bytes,
            max_fetch_chunks: cli.max_fetch_chunks.max(1),
            artificial_backend_delay_ms: cli.artificial_backend_delay_ms,
            tile_cache_max_bytes: cli.tile_cache_max_bytes,
            chunk_cache_max_bytes: cli.chunk_cache_max_bytes,
            style_templates: cli.style_templates.filter(|value| !value.trim().is_empty()),
            glyph_url_template: cli
                .glyph_url_template
                .filter(|value| !value.trim().is_empty()),
            sprite_templates: cli
                .sprite_templates
                .filter(|value| !value.trim().is_empty()),
            mapterhorn_tileset: cli
                .mapterhorn_tileset
                .filter(|value| !value.trim().is_empty()),
            mapterhorn_maxzoom: cli.mapterhorn_maxzoom,
            mapterhorn_negative_ttl: Duration::from_secs(cli.mapterhorn_negative_ttl_secs),
        })
    }
}

/// Generates a process-local node id for ad-hoc local runs.
fn auto_node_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("node-{}-{now}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli() -> Cli {
        Cli {
            gossip_seeds: None,
            node_id: Some("node-a".to_string()),
            gossip_advertise_addr: None,
            internal_http_advertise_addr: None,
            gossip_bind: "0.0.0.0:7946".parse().unwrap(),
            http_port: 8080,
            internal_http_port: 9090,
            cluster: false,
            tileset_sources: "data".to_string(),
            router_candidate_count: 3,
            router_tile_group_size: 512,
            gossip_interval_ms: 200,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            artificial_backend_delay_ms: 0,
            tile_cache_max_bytes: 512 * 1024 * 1024,
            chunk_cache_max_bytes: 512 * 1024 * 1024,
            style_templates: None,
            glyph_url_template: None,
            sprite_templates: None,
            mapterhorn_tileset: None,
            mapterhorn_maxzoom: None,
            mapterhorn_negative_ttl_secs: 3600,
        }
    }

    #[test]
    fn cluster_seed_node_requires_routable_advertise_addresses() {
        let mut cli = cli();
        cli.cluster = true;

        let err = match Config::from_cli(cli) {
            Ok(_) => panic!("wildcard advertise is invalid in cluster mode"),
            Err(err) => err,
        };
        assert!(err.contains("gossip advertise address"));
    }

    #[test]
    fn seeds_imply_cluster_validation() {
        let mut cli = cli();
        cli.gossip_seeds = Some(vec!["ishikari-gossip:7946".to_string()]);

        let err = match Config::from_cli(cli) {
            Ok(_) => panic!("seeds require routable advertise addresses"),
            Err(err) => err,
        };
        assert!(err.contains("gossip advertise address"));
    }

    #[test]
    fn local_single_node_allows_wildcard_listeners() {
        let config = Config::from_cli(cli()).expect("local single-node wildcard bind is allowed");

        assert_eq!(config.internal_listen_addr, "0.0.0.0:9090".parse().unwrap());
        assert!(config.membership.seed_nodes.is_empty());
    }

    #[test]
    fn cluster_allows_seed_node_with_routable_advertise_addresses() {
        let mut cli = cli();
        cli.cluster = true;
        cli.gossip_advertise_addr = Some("127.0.0.1:7946".parse().unwrap());
        cli.internal_http_advertise_addr = Some("127.0.0.1:9090".parse().unwrap());

        let config = Config::from_cli(cli).expect("routable seed-node advertise addresses work");
        assert_eq!(
            config.membership.advertise_addr,
            "127.0.0.1:7946".parse().unwrap()
        );
        assert_eq!(
            config.membership.http_advertise_addr,
            "127.0.0.1:9090".parse().unwrap()
        );
        assert!(config.membership.seed_nodes.is_empty());
    }
}
