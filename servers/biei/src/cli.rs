//! Biei's command-line and environment contract.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::options::{Options, OptionsInput};
use clap::Parser;

#[cfg(test)]
use anyhow::Context;

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_GOSSIP_BIND: &str = "0.0.0.0:7946";
const DEFAULT_SLA: &str = "5s";
const DEFAULT_QUEUE_CAPACITY_MULTIPLIER: usize = 2;
const DEFAULT_RENDER_OUTPUT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MLN_RESOURCE_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_TILESET_URL_TEMPLATE: &str =
    "https://tileset-provider.example.test/tilesets/{tileset_id}/tileset.json";

#[derive(Parser, Debug)]
#[command(name = "biei", version, about = "Distributed MapLibre renderer")]
struct Cli {
    /// Optional delivery-auth registries as `registry_id=auth-root;...`.
    /// Each root contains a `current.json` registry snapshot.
    #[arg(long, env = "BIEI_AUTH_REGISTRIES", default_value = "")]
    auth_registries: String,
    /// Exact Ishikari/provider origin allowed to receive verified delivery
    /// credentials, for example `http://ishikari:8080`.
    #[arg(long, env = "BIEI_AUTH_PROVIDER_ORIGIN")]
    auth_provider_origin: Option<String>,
    /// Style templates: `;`-separated entries, each either `namespace=<tmpl>`,
    /// the reserved `default=<tmpl>`, or a bare `<tmpl>` (treated as the
    /// default). Each `<tmpl>` must be http(s) and contain `{style_id}` in its
    /// URL path.
    #[arg(long, env = "BIEI_STYLE_TEMPLATES")]
    style_templates: String,
    #[arg(
        long,
        env = "BIEI_TILESET_URL_TEMPLATE",
        default_value = DEFAULT_TILESET_URL_TEMPLATE
    )]
    tileset_url_template: String,
    #[arg(long, env = "BIEI_CLUSTER", default_value_t = false)]
    cluster: bool,
    #[arg(
        long = "require-gossip-bootstrap",
        env = "BIEI_REQUIRE_GOSSIP_BOOTSTRAP",
        default_value_t = false
    )]
    require_gossip_bootstrap: bool,
    #[arg(long, env = "BIEI_HTTP_BIND", default_value = DEFAULT_BIND)]
    http_bind: SocketAddr,
    /// Cluster-internal port: metrics, `/_internal/*` and peer forwarding.
    #[arg(long, env = "BIEI_INTERNAL_PORT", default_value_t = 9090)]
    internal_port: u16,
    #[arg(long, env = "BIEI_INTERNAL_ADVERTISE_ADDR")]
    internal_advertise_addr: Option<SocketAddr>,
    #[arg(long, env = "BIEI_GOSSIP_BIND", default_value = DEFAULT_GOSSIP_BIND)]
    gossip_bind: SocketAddr,
    #[arg(long, env = "BIEI_GOSSIP_ADVERTISE_ADDR")]
    gossip_advertise_addr: Option<SocketAddr>,
    #[arg(long, env = "BIEI_GOSSIP_SEEDS", value_delimiter = ',')]
    gossip_seeds: Vec<String>,
    #[arg(long, env = "BIEI_NODE_ID")]
    node_id: Option<String>,
    #[arg(long, env = "BIEI_CORES")]
    cores: Option<usize>,
    #[arg(long, env = "BIEI_SLA", default_value = DEFAULT_SLA, value_parser = parse_duration)]
    sla: Duration,
    /// Fallback MLN ambient-cache path used with
    /// `--disable-mln-file-sources`.
    #[arg(long, env = "BIEI_MAPLIBRE_CACHE_PATH")]
    maplibre_cache_path: Option<PathBuf>,
    /// Font used for labels drawn inside static-map pins.
    #[arg(long, env = "BIEI_PIN_LABEL_FONT")]
    pin_label_font: Option<PathBuf>,
    #[arg(long, hide = true)]
    debug_renderer_slots: Option<usize>,
    #[arg(long, hide = true)]
    debug_render_permits: Option<usize>,
    #[arg(long, hide = true)]
    debug_native_render_permits: Option<usize>,
    /// Hard per-slot queue multiplier over the soft routing limit.
    #[arg(
        long,
        env = "BIEI_QUEUE_CAPACITY_MULTIPLIER",
        default_value_t = DEFAULT_QUEUE_CAPACITY_MULTIPLIER
    )]
    queue_capacity_multiplier: usize,
    #[arg(long, env = "BIEI_SOURCE_CACHE_CAPACITY", default_value_t = 1)]
    source_cache_capacity: usize,
    /// Node-local rendered image cache capacity in bytes. Set to 0 to disable.
    #[arg(
        long,
        env = "BIEI_RENDER_OUTPUT_CACHE_BYTES",
        default_value_t = DEFAULT_RENDER_OUTPUT_CACHE_BYTES
    )]
    render_output_cache_bytes: u64,
    /// Process-wide MapLibre tile/glyph/sprite response cache.
    #[arg(
        long,
        env = "BIEI_MLN_RESOURCE_CACHE_BYTES",
        default_value_t = DEFAULT_MLN_RESOURCE_CACHE_BYTES
    )]
    mln_resource_cache_bytes: u64,
    /// Concurrent response-body downloads in the Rust network FileSource.
    #[arg(long, env = "BIEI_MLN_BODY_PERMITS")]
    mln_body_permits: Option<usize>,
    /// Concurrent regular-priority upstream fetches.
    #[arg(long, hide = true, env = "BIEI_MLN_REGULAR_PERMITS")]
    mln_regular_permits: Option<usize>,
    /// Hosts allowed to resolve to non-public addresses. Prefer exact hosts;
    /// leading-wildcard domains are accepted only for controlled deployments.
    #[arg(long, env = "BIEI_MLN_RESOURCE_PRIVATE_HOSTS", value_delimiter = ',')]
    mln_resource_private_hosts: Vec<String>,
    #[arg(
        long,
        hide = true,
        env = "BIEI_DISABLE_MLN_FILE_SOURCES",
        default_value_t = false
    )]
    disable_mln_file_sources: bool,
}

pub(crate) fn load() -> anyhow::Result<Options> {
    resolve(Cli::parse())
}

fn resolve(cli: Cli) -> anyhow::Result<Options> {
    Options::resolve(OptionsInput {
        auth_registries: cli.auth_registries,
        auth_provider_origin: cli.auth_provider_origin,
        style_templates: cli.style_templates,
        tileset_url_template: cli.tileset_url_template,
        cluster: cli.cluster,
        require_gossip_bootstrap: cli.require_gossip_bootstrap,
        http_bind: cli.http_bind,
        internal_port: cli.internal_port,
        internal_advertise_addr: cli.internal_advertise_addr,
        gossip_bind: cli.gossip_bind,
        gossip_advertise_addr: cli.gossip_advertise_addr,
        gossip_seeds: cli.gossip_seeds,
        node_id: cli.node_id.unwrap_or_else(auto_node_id),
        cores: cli.cores.unwrap_or_else(default_cores),
        sla: cli.sla,
        maplibre_cache_path: cli
            .maplibre_cache_path
            .unwrap_or_else(default_maplibre_cache_path),
        pin_label_font_path: cli.pin_label_font,
        renderer_slots: cli.debug_renderer_slots,
        render_permits: cli.debug_render_permits,
        native_render_permits: cli.debug_native_render_permits,
        queue_capacity_multiplier: cli.queue_capacity_multiplier,
        source_cache_capacity: cli.source_cache_capacity,
        render_output_cache_bytes: cli.render_output_cache_bytes,
        mln_resource_cache_bytes: cli.mln_resource_cache_bytes,
        mln_body_permits: cli.mln_body_permits,
        mln_regular_permits: cli.mln_regular_permits,
        mln_resource_private_hosts: cli.mln_resource_private_hosts,
        disable_mln_file_sources: cli.disable_mln_file_sources,
    })
}

fn default_cores() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

fn auto_node_id() -> String {
    if let Ok(hostname) = std::env::var("HOSTNAME")
        && !hostname.is_empty()
    {
        return hostname;
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("node-{}-{now}", std::process::id())
}

fn default_maplibre_cache_path() -> PathBuf {
    std::env::temp_dir().join("biei-maplibre-ambient-cache.sqlite")
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> anyhow::Result<Options> {
        let cli = Cli::try_parse_from(args).context("parse server CLI")?;
        resolve(cli)
    }

    #[test]
    fn defaults_and_capacity_derivation_survive_the_boundary() {
        let options = parse(&[
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}.json",
            "--node-id",
            "biei-0",
            "--cores",
            "16",
        ])
        .expect("options parse");

        assert_eq!(options.renderer_slots_per_node, 20);
        assert_eq!(options.render_permits_per_node, 16);
        assert_eq!(options.native_render_permits_per_node, 16);
        assert_eq!(options.mln_body_permits, 64);
        assert_eq!(options.mln_regular_permits, 128);
        assert_eq!(options.queue_capacity_multiplier, 2);
        assert!(!options.require_gossip_bootstrap);
        assert_eq!(options.maplibre_cache_path, default_maplibre_cache_path());
    }

    #[test]
    fn gossip_bootstrap_requirement_is_explicit() {
        let options = parse(&[
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}.json",
            "--require-gossip-bootstrap",
        ])
        .expect("options parse");

        assert!(options.require_gossip_bootstrap);
    }

    #[test]
    fn rejects_invalid_resource_templates() {
        for template in [
            "https://styles.test/static.json",
            "https://{style_id}.styles.test/style.json",
            "file:///styles/{style_id}.json",
        ] {
            assert!(
                parse(&["biei", "--style-templates", template]).is_err(),
                "invalid template accepted: {template}"
            );
        }

        let error = parse(&[
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}.json",
            "--tileset-url-template",
            "https://tiles.test/static.json",
        ])
        .expect_err("tileset placeholder required");
        assert!(error.to_string().contains("must contain {tileset_id}"));
    }

    #[test]
    fn parses_file_source_and_debug_overrides() {
        let options = parse(&[
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}.json",
            "--cores",
            "16",
            "--debug-renderer-slots",
            "24",
            "--debug-render-permits",
            "12",
            "--debug-native-render-permits",
            "8",
            "--mln-body-permits",
            "7",
            "--mln-regular-permits",
            "5",
            "--mln-resource-private-hosts",
            "resource-api.default.svc.cluster.local,*.tiles.svc.cluster.local",
            "--disable-mln-file-sources",
            "--pin-label-font",
            "/fonts/NotoSans-Bold.ttf",
        ])
        .expect("options parse");

        assert_eq!(options.renderer_slots_per_node, 24);
        assert_eq!(options.render_permits_per_node, 12);
        assert_eq!(options.native_render_permits_per_node, 8);
        assert_eq!(options.mln_body_permits, 7);
        assert_eq!(options.mln_regular_permits, 7);
        assert_eq!(options.mln_resource_private_hosts.len(), 2);
        assert_eq!(
            options.pin_label_font_path,
            Some(PathBuf::from("/fonts/NotoSans-Bold.ttf"))
        );
        assert!(options.disable_mln_file_sources);
    }

    #[test]
    fn cluster_contract_rejects_implicit_or_unroutable_membership() {
        let base = [
            "biei",
            "--style-templates",
            "https://styles.test/{style_id}.json",
        ];
        let mut seeded = base.to_vec();
        seeded.extend(["--gossip-seeds", "127.0.0.1:7946"]);
        assert!(parse(&seeded).is_err());

        let mut wildcard = base.to_vec();
        wildcard.push("--cluster");
        assert!(parse(&wildcard).is_err());

        let mut wildcard_advertise = wildcard.clone();
        wildcard_advertise.extend([
            "--gossip-advertise-addr",
            "0.0.0.0:7946",
            "--internal-advertise-addr",
            "127.0.0.1:9090",
        ]);
        let error = parse(&wildcard_advertise).expect_err("wildcard gossip advertise rejected");
        assert!(error.to_string().contains("is a wildcard"));

        let mut valid = wildcard;
        valid.extend([
            "--gossip-advertise-addr",
            "127.0.0.1:7946",
            "--internal-advertise-addr",
            "127.0.0.1:9090",
        ]);
        assert!(parse(&valid).is_ok());
    }

    #[test]
    fn queue_multiplier_and_private_hosts_are_validated() {
        for multiplier in ["0", "5"] {
            let error = parse(&[
                "biei",
                "--style-templates",
                "https://styles.test/{style_id}.json",
                "--queue-capacity-multiplier",
                multiplier,
            ])
            .expect_err("queue multiplier rejected");
            assert!(error.to_string().contains("between 1 and 4"));
        }

        assert!(
            parse(&[
                "biei",
                "--style-templates",
                "https://styles.test/{style_id}.json",
                "--mln-resource-private-hosts",
                "https://resource-api.test",
            ])
            .is_err()
        );
    }
}
