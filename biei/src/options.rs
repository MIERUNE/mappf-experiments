//! Small server configuration surface: CLI flags with environment fallback.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};
use clap::Parser;

use crate::config::{BlCapacityPolicy, ClusterConfig};
use crate::style_catalog::StyleCatalog;
use crate::tileset_catalog::TilesetCatalog;
use crate::types::NodeId;

const DEFAULT_BIND: &str = "0.0.0.0:8080";
const DEFAULT_GOSSIP_BIND: &str = "0.0.0.0:7946";
const DEFAULT_SLA: &str = "5s";
const DEFAULT_QUEUE_CAPACITY_MULTIPLIER: usize = 2;
const MAX_QUEUE_CAPACITY_MULTIPLIER: usize = 4;
const STANDBY_RATIO_NUMERATOR: usize = 5;
const STANDBY_RATIO_DENOMINATOR: usize = 4;
const DEFAULT_RENDER_OUTPUT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_MLN_RESOURCE_CACHE_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_TILESET_URL_TEMPLATE: &str =
    "https://tileset-provider.example.test/tilesets/{tileset_id}/tileset.json";

/// Parsed `BIEI_STYLE_TEMPLATES` spec: zero-or-more namespace templates plus an
/// optional default (catch-all) template.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct StyleTemplates {
    /// `(namespace, template)` pairs, in declaration order, namespaces unique.
    pub namespaces: Vec<(String, String)>,
    /// Catch-all template for ids whose namespace isn't registered above. Set by
    /// a `default=<tmpl>` entry or a bare `<tmpl>` entry with no namespace key.
    pub default: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    pub style_templates: StyleTemplates,
    pub tileset_url_template: String,
    pub cluster: bool,
    pub http_bind: SocketAddr,
    /// Cluster-internal listener (metrics, `/_internal/*`, peer forwarding).
    /// Never fronted by the Gateway.
    pub internal_bind: SocketAddr,
    /// Address peers forward `/_internal/*` to — the admin port, not the
    /// Gateway-fronted public port.
    pub internal_advertise_addr: SocketAddr,
    pub gossip_bind: SocketAddr,
    pub gossip_seeds: Vec<String>,
    pub node_id: NodeId,
    pub cores: usize,
    pub sla: Duration,
    /// Used only when Rust FileSources are disabled; the default MLN Database
    /// FileSource persists its ambient cache at this path.
    pub maplibre_cache_path: PathBuf,
    pub renderer_slots_per_node: usize,
    pub render_permits_per_node: usize,
    pub cpu_render_permits_per_node: usize,
    /// Hard per-slot queue limit multiplier over the fixed soft limit.
    pub queue_capacity_multiplier: usize,
    pub source_cache_capacity: usize,
    pub render_output_cache_capacity_bytes: u64,
    pub mln_resource_cache_capacity_bytes: u64,
    /// Concurrent response-body downloads in the Rust network FileSource —
    /// the node's effective in-render I/O parallelism.
    pub mln_body_permits: usize,
    /// Concurrent regular-priority upstream fetches (admission lane).
    pub mln_regular_permits: usize,
    /// Hosts that may resolve to private, loopback, or link-local addresses.
    /// Other resource hosts must resolve to public addresses.
    pub mln_resource_private_hosts: Vec<String>,
    /// Expert escape hatch: fall back to MapLibre Native's default Network and
    /// Database FileSources.
    pub disable_mln_file_sources: bool,
}

#[derive(Parser, Debug)]
#[command(name = "biei", version, about = "Distributed MapLibre renderer")]
struct Cli {
    /// Style templates: `;`-separated entries, each either `namespace=<tmpl>`,
    /// the reserved `default=<tmpl>`, or a bare `<tmpl>` (treated as the
    /// default). Each `<tmpl>` must be http(s) and contain `{style_id}` in its
    /// URL path.
    /// e.g. `gl=https://basemaps.cartocdn.com/gl/{style_id}/style.json;default=https://styles.example/{style_id}/style.json`
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
    #[arg(long, env = "BIEI_HTTP_BIND", default_value = DEFAULT_BIND)]
    http_bind: SocketAddr,
    /// Cluster-internal port: metrics, `/_internal/*` and peer forwarding.
    /// Served on a separate listener, never exposed via the Gateway.
    #[arg(long, env = "BIEI_INTERNAL_PORT", default_value_t = 9090)]
    internal_port: u16,
    #[arg(long, env = "BIEI_INTERNAL_ADVERTISE_ADDR")]
    internal_advertise_addr: Option<SocketAddr>,
    #[arg(long, env = "BIEI_GOSSIP_BIND", default_value = DEFAULT_GOSSIP_BIND)]
    gossip_bind: SocketAddr,
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
    #[arg(long, hide = true)]
    debug_renderer_slots: Option<usize>,
    #[arg(long, hide = true)]
    debug_render_permits: Option<usize>,
    #[arg(long, hide = true)]
    debug_cpu_render_permits: Option<usize>,
    /// Hard per-slot queue multiplier over the soft routing limit. Keep this
    /// bounded: it absorbs short bursts but does not add render throughput.
    #[arg(
        long,
        env = "BIEI_QUEUE_CAPACITY_MULTIPLIER",
        default_value_t = DEFAULT_QUEUE_CAPACITY_MULTIPLIER
    )]
    queue_capacity_multiplier: usize,
    /// Per-renderer source warm-state cache capacity.
    #[arg(long, env = "BIEI_SOURCE_CACHE_CAPACITY", default_value_t = 1)]
    source_cache_capacity: usize,
    /// Node-local rendered image cache capacity in bytes. Set to 0 to disable.
    #[arg(
        long,
        env = "BIEI_RENDER_OUTPUT_CACHE_BYTES",
        default_value_t = DEFAULT_RENDER_OUTPUT_CACHE_BYTES
    )]
    render_output_cache_bytes: u64,
    /// Process-wide MapLibre tile/glyph/sprite response cache. Set to 0 to
    /// disable resource caching while keeping the Rust Network FileSource.
    #[arg(
        long,
        env = "BIEI_MLN_RESOURCE_CACHE_BYTES",
        default_value_t = DEFAULT_MLN_RESOURCE_CACHE_BYTES
    )]
    mln_resource_cache_bytes: u64,
    /// Concurrent response-body downloads in the Rust network FileSource.
    /// Defaults to `max(24, 4 × render permits)`; each slot reserves one
    /// worst-case body buffer (16 MiB for tiles/images).
    #[arg(long, env = "BIEI_MLN_BODY_PERMITS")]
    mln_body_permits: Option<usize>,
    /// Concurrent regular-priority upstream fetches. Defaults to
    /// `max(64, 2 × body permits)`; clamped to at least the body permits.
    #[arg(long, hide = true, env = "BIEI_MLN_REGULAR_PERMITS")]
    mln_regular_permits: Option<usize>,
    /// Resource hosts allowed to resolve to non-public addresses. Exact hosts
    /// and leading-wildcard domains (`*.svc.cluster.local`) are accepted; use
    /// the narrowest exact hosts possible when resource URLs are not trusted.
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

impl Options {
    pub fn parse() -> anyhow::Result<Self> {
        Self::from_cli(Cli::parse())
    }

    #[cfg(test)]
    pub fn try_parse_from<I, T>(args: I) -> anyhow::Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let cli = Cli::try_parse_from(args).context("parse server CLI")?;
        Self::from_cli(cli)
    }

    fn from_cli(cli: Cli) -> anyhow::Result<Self> {
        let style_templates = parse_style_templates(&cli.style_templates)?;
        validate_tileset_url_template(&cli.tileset_url_template)?;
        let mln_resource_private_hosts =
            normalize_private_resource_hosts(cli.mln_resource_private_hosts)?;
        if !(1..=MAX_QUEUE_CAPACITY_MULTIPLIER).contains(&cli.queue_capacity_multiplier) {
            bail!(
                "--queue-capacity-multiplier must be between 1 and {MAX_QUEUE_CAPACITY_MULTIPLIER}"
            );
        }
        let cores = cli.cores.unwrap_or_else(default_cores).max(1);
        let render_permits = cli
            .debug_render_permits
            .unwrap_or_else(|| execution_permits_for_cores(cores))
            .max(1);
        let cpu_render_permits = cli
            .debug_cpu_render_permits
            .unwrap_or_else(|| cpu_render_permits_for_cores(cores))
            .max(1)
            .min(render_permits);
        // Standby headroom is defined over concurrently-executing tasks
        // (render permits), not raw cores, so warm-slot coverage keeps its
        // ratio during explicit calibration sweeps.
        let renderer_slots = cli
            .debug_renderer_slots
            .unwrap_or_else(|| standby_slots_for(render_permits))
            .max(render_permits);
        let default_io_permits =
            crate::renderer::file_source::FileSourceIoPermits::for_render_permits(render_permits);
        let mln_body_permits = cli
            .mln_body_permits
            .unwrap_or(default_io_permits.body)
            .max(1);
        let mln_regular_permits = cli
            .mln_regular_permits
            .unwrap_or(default_io_permits.regular)
            .max(mln_body_permits);

        let gossip_seeds: Vec<_> = cli
            .gossip_seeds
            .into_iter()
            .filter(|seed| !seed.is_empty())
            .collect();
        let internal_bind = SocketAddr::new(cli.http_bind.ip(), cli.internal_port);
        // Peers forward `/_internal/*` to the internal port; default the
        // advertised address to it (on the bind IP) rather than the public port.
        let internal_advertise_addr = cli
            .internal_advertise_addr
            .unwrap_or_else(|| SocketAddr::new(cli.http_bind.ip(), cli.internal_port));
        if !cli.cluster && !gossip_seeds.is_empty() {
            bail!("use --cluster to enable cluster mode");
        }
        if cli.cluster && internal_advertise_addr.ip().is_unspecified() {
            bail!("cluster mode needs a non-wildcard --internal-advertise-addr");
        }

        Ok(Self {
            style_templates,
            tileset_url_template: cli.tileset_url_template,
            cluster: cli.cluster,
            http_bind: cli.http_bind,
            internal_bind,
            internal_advertise_addr,
            gossip_bind: cli.gossip_bind,
            gossip_seeds,
            node_id: NodeId::from(cli.node_id.unwrap_or_else(auto_node_id)),
            cores,
            sla: cli.sla,
            maplibre_cache_path: cli
                .maplibre_cache_path
                .unwrap_or_else(default_maplibre_cache_path),
            renderer_slots_per_node: renderer_slots,
            render_permits_per_node: render_permits.min(renderer_slots),
            cpu_render_permits_per_node: cpu_render_permits.min(renderer_slots),
            queue_capacity_multiplier: cli.queue_capacity_multiplier,
            source_cache_capacity: cli.source_cache_capacity,
            render_output_cache_capacity_bytes: cli.render_output_cache_bytes,
            mln_resource_cache_capacity_bytes: cli.mln_resource_cache_bytes,
            mln_body_permits,
            mln_regular_permits,
            mln_resource_private_hosts,
            disable_mln_file_sources: cli.disable_mln_file_sources,
        })
    }

    pub fn build_style_catalog(&self) -> StyleCatalog {
        let catalog = StyleCatalog::new();
        for (namespace, template) in &self.style_templates.namespaces {
            catalog.add_namespace_template(namespace.clone(), template.clone());
        }
        if let Some(default) = &self.style_templates.default {
            catalog.set_url_template(default.clone());
        }
        catalog
    }

    pub fn build_tileset_catalog(&self) -> TilesetCatalog {
        TilesetCatalog::new(self.tileset_url_template.clone())
    }

    pub fn cluster_config(&self) -> ClusterConfig {
        ClusterConfig {
            renderer_slots_per_node: self.renderer_slots_per_node,
            render_permits_per_node: Some(self.render_permits_per_node),
            cpu_render_permits_per_node: Some(self.cpu_render_permits_per_node),
            // Until a provenance-bearing production profile calibrates
            // setup/render residency distributions, keep the SLA-oriented
            // soft queue at one task per slot. The previous Auto value used a
            // CPU-only render estimate and over-admitted I/O-bound renders.
            bl_capacity: BlCapacityPolicy::Fixed(1),
            queue_capacity_multiplier: self.queue_capacity_multiplier,
            source_cache_capacity: self.source_cache_capacity,
            render_output_cache_capacity_bytes: self.render_output_cache_capacity_bytes,
        }
    }
}

/// Warm standby slots (1.25×) over the given execution-permit count.
fn standby_slots_for(render_permits: usize) -> usize {
    let slots = render_permits
        .saturating_mul(STANDBY_RATIO_NUMERATOR)
        .div_ceil(STANDBY_RATIO_DENOMINATOR)
        .max(1);
    if slots as f64 / render_permits.max(1) as f64 > ClusterConfig::STANDBY_RATIO_ERROR {
        render_permits.max(1)
    } else {
        slots
    }
}

/// Conservative uncalibrated baseline: one executing task per declared core.
/// Operators can use the hidden override for controlled calibration sweeps.
fn execution_permits_for_cores(cores: usize) -> usize {
    cores.max(1)
}

/// Native-render residency baseline. Despite the historical field name, this
/// permit is held across all of `renderStill`, including FileSource I/O waits;
/// it is not a measurement of pure CPU concurrency.
fn cpu_render_permits_for_cores(cores: usize) -> usize {
    cores.max(1)
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
    humantime::parse_duration(value).map_err(|err| err.to_string())
}

/// A namespace key is the first path segment of a style id, so it must be a
/// single, plain segment. Rejecting these characters also disambiguates a
/// `namespace=tmpl` entry from a bare URL whose query contains `=` (its left
/// side then holds `:`/`/`).
fn is_namespace_key(key: &str) -> bool {
    !key.is_empty()
        && !key
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, ':' | '/' | ';' | '=' | '{' | '}'))
}

fn validate_style_template(template: &str, label: &str) -> anyhow::Result<()> {
    if template.is_empty() {
        bail!("style template for {label} must not be empty");
    }
    if !template.contains("{style_id}") {
        bail!("style template for {label} must contain {{style_id}}");
    }
    validate_placeholder_in_url_path(template, "{style_id}", "style template")?;
    let sample_url = template.replace("{style_id}", "sample/style");
    let parsed = url::Url::parse(&sample_url)
        .with_context(|| format!("parse style template for {label}"))?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        scheme => bail!("style URL scheme {scheme:?} is not supported; expected http or https"),
    }
}

/// Parse `BIEI_STYLE_TEMPLATES`: `;`-separated entries, each `namespace=<tmpl>`,
/// `default=<tmpl>`, or a bare `<tmpl>` (which sets the default). At most one
/// default; namespaces unique; every template validated.
fn parse_style_templates(raw: &str) -> anyhow::Result<StyleTemplates> {
    let mut out = StyleTemplates::default();
    let mut default_source: Option<&str> = None;

    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        // `namespace=tmpl` only when the left of the first `=` is a plain
        // namespace key; otherwise the entry is a bare URL (its query `=` is
        // not a key separator) and becomes the default.
        let (label, template, is_default) = match entry.split_once('=') {
            Some((key, value)) if is_namespace_key(key.trim()) => {
                let key = key.trim();
                if key == "default" {
                    ("default", value.trim(), true)
                } else {
                    (key, value.trim(), false)
                }
            }
            _ => ("default", entry, true),
        };

        validate_style_template(template, label)?;

        if is_default {
            if let Some(prev) = default_source.replace(template) {
                bail!(
                    "BIEI_STYLE_TEMPLATES has multiple default templates ({prev:?} and {template:?}); keep only one bare/`default=` entry"
                );
            }
            out.default = Some(template.to_string());
        } else {
            if out.namespaces.iter().any(|(ns, _)| ns == label) {
                bail!("BIEI_STYLE_TEMPLATES has duplicate namespace {label:?}");
            }
            out.namespaces
                .push((label.to_string(), template.to_string()));
        }
    }

    if out.namespaces.is_empty() && out.default.is_none() {
        bail!("BIEI_STYLE_TEMPLATES must define at least one template");
    }
    Ok(out)
}

fn validate_tileset_url_template(template: &str) -> anyhow::Result<()> {
    if template.is_empty() {
        bail!("--tileset-url-template must not be empty");
    }
    if !template.contains("{tileset_id}") {
        bail!("--tileset-url-template must contain {{tileset_id}}");
    }
    validate_placeholder_in_url_path(template, "{tileset_id}", "--tileset-url-template")?;
    let sample_url = template.replace("{tileset_id}", "sample/tileset");
    let parsed = url::Url::parse(&sample_url).context("parse --tileset-url-template")?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        scheme => bail!("tileset URL scheme {scheme:?} is not supported; expected http or https"),
    }
}

fn validate_placeholder_in_url_path(
    template: &str,
    placeholder: &str,
    label: &str,
) -> anyhow::Result<()> {
    const MARKER: &str = "biei-placeholder-marker";
    let expected = template.matches(placeholder).count();
    let sample = template.replace(placeholder, MARKER);
    let parsed = url::Url::parse(&sample)
        .with_context(|| format!("parse {label} while validating placeholder position"))?;
    if parsed.path().matches(MARKER).count() != expected {
        bail!("{label} placeholder must appear only in the URL path");
    }
    Ok(())
}

fn normalize_private_resource_hosts(hosts: Vec<String>) -> anyhow::Result<Vec<String>> {
    hosts
        .into_iter()
        .filter(|host| !host.trim().is_empty())
        .map(|host| {
            let normalized = host.trim().trim_end_matches('.').to_ascii_lowercase();
            let wildcard = normalized.starts_with("*.");
            let candidate = normalized.strip_prefix("*.").unwrap_or(&normalized);
            let parsed = url::Host::parse(candidate).map_err(|_| {
                anyhow::anyhow!("invalid --mln-resource-private-hosts entry: {host}")
            })?;
            match parsed {
                url::Host::Domain(domain) if !domain.is_empty() && !domain.contains('*') => {
                    Ok(if wildcard {
                        format!("*.{domain}")
                    } else {
                        domain
                    })
                }
                url::Host::Ipv4(address) if !wildcard => Ok(address.to_string()),
                url::Host::Ipv6(address) if !wildcard => Ok(address.to_string()),
                _ => bail!("invalid --mln-resource-private-hosts entry: {host}"),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StyleId;

    #[test]
    fn rejects_template_without_placeholder() {
        let err = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/static/style.json",
        ])
        .expect_err("placeholder is required");

        assert!(format!("{err:#}").contains("must contain {style_id}"));
    }

    #[test]
    fn rejects_style_placeholder_outside_url_path() {
        for template in [
            "https://{style_id}.example.test/style.json",
            "https://styles.example.test/style.json?id={style_id}",
            "https://styles.example.test?next=/{style_id}",
        ] {
            let err = Options::try_parse_from(["biei", "--style-templates", template])
                .expect_err("style placeholder outside path is rejected");
            assert!(format!("{err:#}").contains("only in the URL path"));
        }
    }

    #[test]
    fn rejects_non_http_template() {
        let err = Options::try_parse_from([
            "biei",
            "--style-templates",
            "file:///styles/{style_id}/style.json",
        ])
        .expect_err("only http/https are accepted");

        assert!(format!("{err:#}").contains("scheme"));
    }

    #[test]
    fn rejects_tileset_template_without_placeholder() {
        let err = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--tileset-url-template",
            "https://tiles.example.test/static/tileset.json",
        ])
        .expect_err("tileset placeholder is required");

        assert!(format!("{err:#}").contains("must contain {tileset_id}"));
    }

    #[test]
    fn rejects_tileset_placeholder_outside_url_path() {
        for template in [
            "https://{tileset_id}.example.test/tileset.json",
            "https://tiles.example.test?next=/{tileset_id}",
        ] {
            let err = Options::try_parse_from([
                "biei",
                "--style-templates",
                "https://styles.example.test/{style_id}.json",
                "--tileset-url-template",
                template,
            ])
            .expect_err("tileset placeholder outside path is rejected");

            assert!(format!("{err:#}").contains("only in the URL path"));
        }
    }

    #[test]
    fn builds_tileset_catalog_with_raw_replacement() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--tileset-url-template",
            "https://tiles.example.test/{tileset_id}/tileset.json",
        ])
        .expect("options parse");

        assert_eq!(
            opts.build_tileset_catalog()
                .resolve_url("analysis/hrnowc/sample"),
            "https://tiles.example.test/analysis/hrnowc/sample/tileset.json"
        );
    }

    #[test]
    fn cores_expand_to_slots_and_permits() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "16",
            "--node-id",
            "biei-0",
        ])
        .expect("options parse");

        assert_eq!(opts.cores, 16);
        // Uncalibrated production defaults do not oversubscribe cores. Warm
        // standby slots remain separate from concurrently executing tasks.
        assert_eq!(opts.render_permits_per_node, 16);
        assert_eq!(opts.cpu_render_permits_per_node, 16);
        assert_eq!(opts.renderer_slots_per_node, 20);
        // FileSource I/O follows the execution permits: body = 4× render
        // permits, regular = 2× body.
        assert_eq!(opts.mln_body_permits, 64);
        assert_eq!(opts.mln_regular_permits, 128);
        assert!(matches!(
            opts.cluster_config().bl_capacity,
            BlCapacityPolicy::Fixed(1)
        ));
        assert_eq!(opts.node_id, NodeId::from("biei-0"));
        assert_eq!(opts.maplibre_cache_path, default_maplibre_cache_path());
        assert!(!opts.cluster);
    }

    #[test]
    fn small_nodes_keep_io_permit_floors() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "2",
        ])
        .expect("options parse");

        // One execution/native-render permit per core; warm slots retain 1.25×
        // headroom independently.
        assert_eq!(opts.render_permits_per_node, 2);
        assert_eq!(opts.cpu_render_permits_per_node, 2);
        assert_eq!(opts.renderer_slots_per_node, 3);
        assert_eq!(opts.queue_capacity_multiplier, 2);
        assert_eq!(opts.cluster_config().queue_capacity_multiplier, 2);
        // Floors dominate: body max(24, 4×3), regular max(64, 2×24).
        assert_eq!(opts.mln_body_permits, 24);
        assert_eq!(opts.mln_regular_permits, 64);
    }

    #[test]
    fn queue_capacity_multiplier_is_bounded_and_forwarded_to_cluster_config() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--queue-capacity-multiplier",
            "3",
        ])
        .expect("bounded queue multiplier parses");

        assert_eq!(opts.queue_capacity_multiplier, 3);
        assert_eq!(opts.cluster_config().queue_capacity_multiplier, 3);

        for invalid in ["0", "5"] {
            let err = Options::try_parse_from([
                "biei",
                "--style-templates",
                "http://style-api.test/styles/{style_id}/style.json",
                "--queue-capacity-multiplier",
                invalid,
            ])
            .expect_err("unbounded queue multiplier must be rejected");
            assert!(
                err.to_string()
                    .contains("--queue-capacity-multiplier must be between 1 and 4")
            );
        }
    }

    #[test]
    fn io_permit_flags_override_defaults_and_clamp() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "4",
            "--mln-body-permits",
            "128",
            "--mln-regular-permits",
            "32",
        ])
        .expect("options parse");

        assert_eq!(opts.mln_body_permits, 128);
        // Regular is clamped up to the body permits so it can never become
        // the tighter cap by accident.
        assert_eq!(opts.mln_regular_permits, 128);
    }

    #[test]
    fn parses_maplibre_cache_path() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--maplibre-cache-path",
            "/tmp/custom-biei-cache.sqlite",
        ])
        .expect("options parse");

        assert_eq!(
            opts.maplibre_cache_path,
            PathBuf::from("/tmp/custom-biei-cache.sqlite")
        );
    }

    #[test]
    fn parses_cache_capacity_knobs() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--source-cache-capacity",
            "4",
            "--render-output-cache-bytes",
            "1048576",
        ])
        .expect("options parse");

        assert_eq!(opts.source_cache_capacity, 4);
        assert_eq!(opts.render_output_cache_capacity_bytes, 1_048_576);
        assert_eq!(
            opts.mln_resource_cache_capacity_bytes,
            DEFAULT_MLN_RESOURCE_CACHE_BYTES
        );
        assert_eq!(opts.cluster_config().source_cache_capacity, 4);
        assert_eq!(
            opts.cluster_config().render_output_cache_capacity_bytes,
            1_048_576
        );
        assert!(!opts.disable_mln_file_sources);
    }

    #[test]
    fn parses_file_source_options() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--mln-resource-cache-bytes",
            "1048576",
            "--mln-resource-private-hosts",
            "resource-api.default.svc.cluster.local,*.tiles.svc.cluster.local",
            "--mln-body-permits",
            "7",
            "--mln-regular-permits",
            "5",
            "--disable-mln-file-sources",
        ])
        .expect("options parse");

        assert_eq!(opts.mln_resource_cache_capacity_bytes, 1_048_576);
        assert_eq!(opts.mln_body_permits, 7);
        assert_eq!(opts.mln_regular_permits, 7);
        assert_eq!(
            opts.mln_resource_private_hosts,
            [
                "resource-api.default.svc.cluster.local",
                "*.tiles.svc.cluster.local"
            ]
        );
        assert!(opts.disable_mln_file_sources);
    }

    #[test]
    fn rejects_invalid_private_resource_host_pattern() {
        let err = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--mln-resource-private-hosts",
            "https://resource-api.test",
        ])
        .expect_err("URL syntax is not a host pattern");

        assert!(
            err.to_string()
                .contains("invalid --mln-resource-private-hosts")
        );
    }

    #[test]
    fn debug_overrides_are_hidden_but_work() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cores",
            "16",
            "--debug-renderer-slots",
            "24",
            "--debug-render-permits",
            "12",
            "--debug-cpu-render-permits",
            "8",
        ])
        .expect("options parse");

        assert_eq!(opts.renderer_slots_per_node, 24);
        assert_eq!(opts.render_permits_per_node, 12);
        assert_eq!(opts.cpu_render_permits_per_node, 8);
    }

    #[test]
    fn parses_cluster_advertise_addr() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cluster",
            "--internal-advertise-addr",
            "127.0.0.1:18080",
        ])
        .expect("options parse");

        assert!(opts.cluster);
        assert_eq!(
            opts.internal_advertise_addr,
            "127.0.0.1:18080".parse().unwrap()
        );
    }

    #[test]
    fn rejects_gossip_seeds_without_cluster_flag() {
        let err = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--gossip-seeds",
            "127.0.0.1:7946",
        ])
        .expect_err("cluster intent must be explicit");

        assert!(format!("{err:#}").contains("use --cluster"));
    }

    #[test]
    fn rejects_cluster_with_wildcard_advertise_addr() {
        let err = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cluster",
        ])
        .expect_err("cluster peers need a reachable advertise address");

        assert!(format!("{err:#}").contains("non-wildcard --internal-advertise-addr"));
    }

    #[test]
    fn accepts_cluster_seed_node_without_gossip_seeds() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cluster",
            "--internal-advertise-addr",
            "127.0.0.1:8080",
        ])
        .expect("seed node can start without seeds");

        assert!(opts.cluster);
        assert!(opts.gossip_seeds.is_empty());
        assert_eq!(
            opts.internal_advertise_addr,
            "127.0.0.1:8080".parse().unwrap()
        );
    }

    #[test]
    fn accepts_cluster_join_node_with_gossip_seeds() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
            "--cluster",
            "--gossip-seeds",
            "127.0.0.1:7946",
            "--internal-advertise-addr",
            "127.0.0.1:8080",
        ])
        .expect("cluster join options parse");

        assert!(opts.cluster);
        assert_eq!(opts.gossip_seeds, vec!["127.0.0.1:7946"]);
        assert_eq!(
            opts.internal_advertise_addr,
            "127.0.0.1:8080".parse().unwrap()
        );
    }

    #[test]
    fn parses_namespace_and_default_templates() {
        let spec = parse_style_templates(
            "gl=https://basemaps.cartocdn.com/gl/{style_id}/style.json; \
             example=https://styles.example.test/{style_id}/style.json; \
             default=https://fallback.example/{style_id}/style.json",
        )
        .expect("spec parses");

        assert_eq!(
            spec.namespaces,
            vec![
                (
                    "gl".to_string(),
                    "https://basemaps.cartocdn.com/gl/{style_id}/style.json".to_string()
                ),
                (
                    "example".to_string(),
                    "https://styles.example.test/{style_id}/style.json".to_string()
                ),
            ]
        );
        assert_eq!(
            spec.default.as_deref(),
            Some("https://fallback.example/{style_id}/style.json")
        );
    }

    #[test]
    fn bare_single_template_is_the_default() {
        let spec = parse_style_templates("https://basemaps.cartocdn.com/{style_id}/style.json")
            .expect("bare spec parses");
        assert!(spec.namespaces.is_empty());
        assert_eq!(
            spec.default.as_deref(),
            Some("https://basemaps.cartocdn.com/{style_id}/style.json")
        );
    }

    #[test]
    fn bare_template_with_query_equals_is_not_a_namespace() {
        // The `=` lives in the query string; the left side has `:`/`/`, so it is
        // not a namespace key and the whole entry becomes the default.
        let spec = parse_style_templates("https://styles.example/{style_id}/style.json?key=abc123")
            .expect("query template parses");
        assert!(spec.namespaces.is_empty());
        assert_eq!(
            spec.default.as_deref(),
            Some("https://styles.example/{style_id}/style.json?key=abc123")
        );
    }

    #[test]
    fn rejects_multiple_default_templates() {
        let err = parse_style_templates(
            "https://a.example/{style_id}/style.json;default=https://b.example/{style_id}/style.json",
        )
        .expect_err("two defaults are ambiguous");
        assert!(format!("{err:#}").contains("multiple default templates"));
    }

    #[test]
    fn rejects_duplicate_namespace() {
        let err = parse_style_templates(
            "gl=https://a.example/{style_id}/style.json;gl=https://b.example/{style_id}/style.json",
        )
        .expect_err("duplicate namespace");
        assert!(format!("{err:#}").contains("duplicate namespace"));
    }

    #[test]
    fn rejects_empty_spec() {
        let err = parse_style_templates("  ;; ").expect_err("no templates");
        assert!(format!("{err:#}").contains("at least one template"));
    }

    #[test]
    fn namespace_template_resolves_through_catalog() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "gl=https://basemaps.cartocdn.com/gl/{style_id}/style.json",
        ])
        .expect("options parse");
        let catalog = opts.build_style_catalog();

        assert_eq!(
            catalog
                .definition_for_revision(&crate::types::StyleRevision {
                    id: StyleId("gl/voyager-gl-style".to_string()),
                    version: 1,
                })
                .expect("namespace resolves")
                .style_url,
            "https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        );
        // No default: an unmatched namespace is unknown.
        assert_eq!(
            catalog.resolve_latest(&StyleId("other/foo".to_string())),
            None
        );
    }

    #[test]
    fn builds_lazy_style_catalog_with_raw_replacement() {
        let opts = Options::try_parse_from([
            "biei",
            "--style-templates",
            "http://style-api.test/styles/{style_id}/style.json",
        ])
        .expect("options parse");
        let catalog = opts.build_style_catalog();
        let style_id = StyleId("carto/voyager".to_string());

        assert_eq!(catalog.resolve_latest(&style_id), Some(1));
        assert_eq!(
            catalog
                .definition_for_revision(&crate::types::StyleRevision {
                    id: style_id,
                    version: 1,
                })
                .expect("definition exists")
                .style_url,
            "http://style-api.test/styles/carto/voyager/style.json"
        );
    }
}
