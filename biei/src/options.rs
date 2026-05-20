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
const STANDBY_RATIO_NUMERATOR: usize = 5;
const STANDBY_RATIO_DENOMINATOR: usize = 4;
const DEFAULT_RENDER_OUTPUT_CACHE_BYTES: u64 = 256 * 1024 * 1024;
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
    pub maplibre_cache_path: PathBuf,
    pub renderer_slots_per_node: usize,
    pub render_permits_per_node: usize,
    pub cpu_render_permits_per_node: usize,
    pub source_cache_capacity: usize,
    pub render_output_cache_capacity_bytes: u64,
}

#[derive(Parser, Debug)]
#[command(name = "biei", about = "Distributed MapLibre renderer")]
struct Cli {
    /// Style templates: `;`-separated entries, each either `namespace=<tmpl>`,
    /// the reserved `default=<tmpl>`, or a bare `<tmpl>` (treated as the
    /// default). Each `<tmpl>` must contain `{style_id}` and be http(s).
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
    #[arg(long, env = "BIEI_MAPLIBRE_CACHE_PATH")]
    maplibre_cache_path: Option<PathBuf>,
    #[arg(long, hide = true)]
    debug_renderer_slots: Option<usize>,
    #[arg(long, hide = true)]
    debug_render_permits: Option<usize>,
    #[arg(long, hide = true)]
    debug_cpu_render_permits: Option<usize>,
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
}

impl Options {
    pub fn parse() -> anyhow::Result<Self> {
        Self::try_parse_from(std::env::args())
    }

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
        let cores = cli.cores.unwrap_or_else(default_cores).max(1);
        let render_permits = cli.debug_render_permits.unwrap_or(cores).max(1);
        let cpu_render_permits = cli
            .debug_cpu_render_permits
            .unwrap_or(render_permits)
            .max(1)
            .min(render_permits);
        let renderer_slots = cli
            .debug_renderer_slots
            .unwrap_or_else(|| standby_slots_for_cores(cores))
            .max(render_permits);

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
            source_cache_capacity: cli.source_cache_capacity,
            render_output_cache_capacity_bytes: cli.render_output_cache_bytes,
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
            bl_capacity: BlCapacityPolicy::Auto,
            queue_capacity_multiplier: 2,
            source_cache_capacity: self.source_cache_capacity,
            render_output_cache_capacity_bytes: self.render_output_cache_capacity_bytes,
        }
    }
}

fn standby_slots_for_cores(cores: usize) -> usize {
    let slots = cores
        .saturating_mul(STANDBY_RATIO_NUMERATOR)
        .div_ceil(STANDBY_RATIO_DENOMINATOR)
        .max(1);
    if slots as f64 / cores.max(1) as f64 > ClusterConfig::STANDBY_RATIO_ERROR {
        cores.max(1)
    } else {
        slots
    }
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
    let sample_url = template.replace("{tileset_id}", "sample/tileset");
    let parsed = url::Url::parse(&sample_url).context("parse --tileset-url-template")?;
    match parsed.scheme() {
        "http" | "https" => Ok(()),
        scheme => bail!("tileset URL scheme {scheme:?} is not supported; expected http or https"),
    }
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
        assert_eq!(opts.renderer_slots_per_node, 20);
        assert_eq!(opts.render_permits_per_node, 16);
        assert_eq!(opts.cpu_render_permits_per_node, 16);
        assert_eq!(opts.node_id, NodeId::from("biei-0"));
        assert_eq!(opts.maplibre_cache_path, default_maplibre_cache_path());
        assert!(!opts.cluster);
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
        assert_eq!(opts.cluster_config().source_cache_capacity, 4);
        assert_eq!(
            opts.cluster_config().render_output_cache_capacity_bytes,
            1_048_576
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
