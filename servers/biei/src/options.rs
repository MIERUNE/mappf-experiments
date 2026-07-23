//! Validated renderer configuration, independent of any CLI or environment.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail};
use mmpf_cluster::GossipEndpoint;
use mmpf_common::resource_templates::{NamespaceKeyPolicy, ResourceTemplates, TemplatePolicy};
use mmpf_mln_filesource::FileSourceIoPermits;

use biei_core::config::{BlCapacityPolicy, ClusterConfig};
use biei_core::style_catalog::StyleCatalog;
use biei_core::types::NodeId;

use crate::auth::RegistryCatalog;

const MAX_QUEUE_CAPACITY_MULTIPLIER: usize = 4;
const STANDBY_RATIO_NUMERATOR: usize = 5;
const STANDBY_RATIO_DENOMINATOR: usize = 4;

/// Parsed `BIEI_STYLE_TEMPLATES` spec.
pub(crate) type StyleTemplates = ResourceTemplates;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) struct Options {
    pub auth_registries: RegistryCatalog,
    /// Exact provider origin allowed to receive a verified delivery token.
    /// Required when delivery authentication is enabled.
    pub auth_provider_origin: Option<url::Url>,
    pub style_templates: StyleTemplates,
    pub tileset_url_template: String,
    pub cluster: bool,
    /// Require one initial raw membership peer observation before reporting
    /// ready. The readiness gate fails open after a bounded grace period.
    pub require_gossip_bootstrap: bool,
    pub http_bind: SocketAddr,
    /// Cluster-internal listener (metrics, `/_internal/*`, peer forwarding).
    /// Never fronted by the Gateway.
    pub internal_bind: SocketAddr,
    /// Address peers forward `/_internal/*` to — the admin port, not the
    /// Gateway-fronted public port.
    pub internal_advertise_addr: SocketAddr,
    pub gossip_endpoint: GossipEndpoint,
    pub gossip_seeds: Vec<String>,
    pub node_id: NodeId,
    pub cores: usize,
    pub sla: Duration,
    /// Optional font override for static pin labels.
    pub pin_label_font_path: Option<PathBuf>,
    /// Used only when Rust FileSources are disabled; the default MLN Database
    /// FileSource persists its ambient cache at this path.
    pub maplibre_cache_path: PathBuf,
    pub renderer_slots_per_node: usize,
    pub render_permits_per_node: usize,
    pub native_render_permits_per_node: usize,
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

/// Unvalidated process configuration supplied by an entry point.
///
/// Keeping this free of `clap` and environment access lets other front ends
/// embed the renderer without inheriting Biei's command-line contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OptionsInput {
    pub auth_registries: String,
    pub auth_provider_origin: Option<String>,
    pub style_templates: String,
    pub tileset_url_template: String,
    pub cluster: bool,
    pub require_gossip_bootstrap: bool,
    pub http_bind: SocketAddr,
    pub internal_port: u16,
    pub internal_advertise_addr: Option<SocketAddr>,
    pub gossip_bind: SocketAddr,
    pub gossip_advertise_addr: Option<SocketAddr>,
    pub gossip_seeds: Vec<String>,
    pub node_id: String,
    pub cores: usize,
    pub sla: Duration,
    pub maplibre_cache_path: PathBuf,
    pub pin_label_font_path: Option<PathBuf>,
    pub renderer_slots: Option<usize>,
    pub render_permits: Option<usize>,
    pub native_render_permits: Option<usize>,
    pub queue_capacity_multiplier: usize,
    pub source_cache_capacity: usize,
    pub render_output_cache_bytes: u64,
    pub mln_resource_cache_bytes: u64,
    pub mln_body_permits: Option<usize>,
    pub mln_regular_permits: Option<usize>,
    pub mln_resource_private_hosts: Vec<String>,
    pub disable_mln_file_sources: bool,
}

impl Options {
    /// Validate raw entry-point configuration and resolve derived capacities.
    pub(crate) fn resolve(input: OptionsInput) -> anyhow::Result<Self> {
        let auth_registries = RegistryCatalog::parse(&input.auth_registries)?;
        let auth_provider_origin =
            parse_auth_provider_origin(input.auth_provider_origin.as_deref())?;
        if auth_registries.is_empty() && auth_provider_origin.is_some() {
            bail!("--auth-provider-origin requires --auth-registries");
        }
        if !auth_registries.is_empty() && auth_provider_origin.is_none() {
            bail!(
                "--auth-provider-origin (BIEI_AUTH_PROVIDER_ORIGIN) is required when delivery auth is enabled"
            );
        }
        let style_templates = parse_style_templates(&input.style_templates)?;
        validate_tileset_url_template(&input.tileset_url_template)?;
        let mln_resource_private_hosts =
            normalize_private_resource_hosts(input.mln_resource_private_hosts)?;
        if !(1..=MAX_QUEUE_CAPACITY_MULTIPLIER).contains(&input.queue_capacity_multiplier) {
            bail!(
                "--queue-capacity-multiplier must be between 1 and {MAX_QUEUE_CAPACITY_MULTIPLIER}"
            );
        }
        let cores = input.cores.max(1);
        let render_permits = input
            .render_permits
            .unwrap_or_else(|| execution_permits_for_cores(cores))
            .max(1);
        let native_render_permits = input
            .native_render_permits
            .unwrap_or_else(|| native_render_permits_for_cores(cores))
            .max(1)
            .min(render_permits);
        // Standby headroom is defined over concurrently-executing tasks
        // (render permits), not raw cores, so warm-slot coverage keeps its
        // ratio during explicit calibration sweeps.
        let renderer_slots = input
            .renderer_slots
            .unwrap_or_else(|| standby_slots_for(render_permits))
            .max(render_permits);
        let default_io_permits = FileSourceIoPermits::for_render_permits(render_permits);
        let mln_body_permits = input
            .mln_body_permits
            .unwrap_or(default_io_permits.body)
            .max(1);
        let mln_regular_permits = input
            .mln_regular_permits
            .unwrap_or(default_io_permits.regular)
            .max(mln_body_permits);

        let gossip_seeds: Vec<_> = input
            .gossip_seeds
            .into_iter()
            .filter(|seed| !seed.is_empty())
            .collect();
        let internal_bind = SocketAddr::new(input.http_bind.ip(), input.internal_port);
        // Peers forward `/_internal/*` to the internal port; default the
        // advertised address to it (on the bind IP) rather than the public port.
        let internal_advertise_addr = input
            .internal_advertise_addr
            .unwrap_or_else(|| SocketAddr::new(input.http_bind.ip(), input.internal_port));
        if !input.cluster && !gossip_seeds.is_empty() {
            bail!("use --cluster to enable cluster mode");
        }
        let gossip_endpoint = if input.cluster {
            let advertise_addr = input.gossip_advertise_addr.ok_or_else(|| {
                anyhow::anyhow!(
                    "cluster mode needs an explicit --gossip-advertise-addr \
                     (BIEI_GOSSIP_ADVERTISE_ADDR)"
                )
            })?;
            GossipEndpoint::clustered(input.gossip_bind, advertise_addr).map_err(|error| {
                anyhow::anyhow!(
                    "{error}; set --gossip-advertise-addr (BIEI_GOSSIP_ADVERTISE_ADDR) \
                     to a routable address in cluster mode"
                )
            })?
        } else {
            GossipEndpoint::standalone(
                input.gossip_bind,
                input.gossip_advertise_addr.unwrap_or(input.gossip_bind),
            )
        };
        if input.cluster && internal_advertise_addr.ip().is_unspecified() {
            bail!("cluster mode needs a non-wildcard --internal-advertise-addr");
        }

        Ok(Self {
            auth_registries,
            auth_provider_origin,
            style_templates,
            tileset_url_template: input.tileset_url_template,
            cluster: input.cluster,
            require_gossip_bootstrap: input.require_gossip_bootstrap,
            http_bind: input.http_bind,
            internal_bind,
            internal_advertise_addr,
            gossip_endpoint,
            gossip_seeds,
            node_id: NodeId::from(input.node_id),
            cores,
            sla: input.sla,
            maplibre_cache_path: input.maplibre_cache_path,
            pin_label_font_path: input.pin_label_font_path,
            renderer_slots_per_node: renderer_slots,
            render_permits_per_node: render_permits.min(renderer_slots),
            native_render_permits_per_node: native_render_permits.min(renderer_slots),
            queue_capacity_multiplier: input.queue_capacity_multiplier,
            source_cache_capacity: input.source_cache_capacity,
            render_output_cache_capacity_bytes: input.render_output_cache_bytes,
            mln_resource_cache_capacity_bytes: input.mln_resource_cache_bytes,
            mln_body_permits,
            mln_regular_permits,
            mln_resource_private_hosts,
            disable_mln_file_sources: input.disable_mln_file_sources,
        })
    }

    pub(crate) fn build_style_catalog(&self) -> StyleCatalog {
        let catalog = StyleCatalog::new();
        for (namespace, template) in self.style_templates.namespaces() {
            catalog.add_namespace_template(namespace.clone(), template.clone());
        }
        if let Some(default) = self.style_templates.default_template() {
            catalog.set_url_template(default);
        }
        catalog
    }

    pub(crate) fn cluster_config(&self) -> ClusterConfig {
        ClusterConfig {
            renderer_slots_per_node: self.renderer_slots_per_node,
            render_permits_per_node: Some(self.render_permits_per_node),
            native_render_permits_per_node: Some(self.native_render_permits_per_node),
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

/// Native-render residency baseline. This permit is held across all of
/// `renderStill`, including FileSource I/O waits; it is not a measurement of
/// pure CPU concurrency.
fn native_render_permits_for_cores(cores: usize) -> usize {
    cores.max(1)
}

fn parse_auth_provider_origin(raw: Option<&str>) -> anyhow::Result<Option<url::Url>> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(None);
    };
    let origin = url::Url::parse(raw).context("parse --auth-provider-origin")?;
    if !matches!(origin.scheme(), "http" | "https") {
        bail!("--auth-provider-origin must use http or https");
    }
    if origin.cannot_be_a_base()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.query().is_some()
        || origin.fragment().is_some()
        || origin.path() != "/"
    {
        bail!(
            "--auth-provider-origin must be an exact origin without credentials, path, query, or fragment"
        );
    }
    Ok(Some(origin))
}

/// Parse `BIEI_STYLE_TEMPLATES` while retaining Biei's HTTP-only, path-scoped
/// placeholder contract and raw substitution behavior.
fn parse_style_templates(raw: &str) -> anyhow::Result<StyleTemplates> {
    ResourceTemplates::parse(
        raw,
        TemplatePolicy {
            config_name: "BIEI_STYLE_TEMPLATES",
            placeholder: "{style_id}",
            require_placeholder: true,
            placeholder_must_be_in_path: true,
            allowed_schemes: &["http", "https"],
            namespace_keys: NamespaceKeyPolicy::PlainSegment,
        },
    )
    .map_err(anyhow::Error::new)
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

/// Resolved [`Options`] with production-shaped test defaults. Exposed (behind
/// `#[doc(hidden)]`) so the server binary's tests can build a node without
/// duplicating the full [`OptionsInput`] contract; not part of the supported
/// API.
#[cfg(test)]
#[doc(hidden)]
pub(crate) fn test_options(style_templates: &str, cores: usize) -> Options {
    Options::resolve(test_input(style_templates, cores)).expect("valid test options")
}

#[cfg(test)]
fn test_input(style_templates: &str, cores: usize) -> OptionsInput {
    OptionsInput {
        auth_registries: String::new(),
        auth_provider_origin: None,
        style_templates: style_templates.to_string(),
        tileset_url_template: "https://tileset-provider.test/tilesets/{tileset_id}/tileset.json"
            .to_string(),
        cluster: false,
        require_gossip_bootstrap: false,
        http_bind: "127.0.0.1:0".parse().expect("test bind"),
        internal_port: 0,
        internal_advertise_addr: None,
        gossip_bind: "127.0.0.1:0".parse().expect("test gossip bind"),
        gossip_advertise_addr: None,
        gossip_seeds: Vec::new(),
        node_id: "biei-0".to_string(),
        cores,
        sla: Duration::from_secs(5),
        maplibre_cache_path: PathBuf::from("/tmp/biei-test-cache.sqlite"),
        pin_label_font_path: None,
        renderer_slots: None,
        render_permits: None,
        native_render_permits: None,
        queue_capacity_multiplier: 2,
        source_cache_capacity: 1,
        render_output_cache_bytes: 256 * 1024 * 1024,
        mln_resource_cache_bytes: 256 * 1024 * 1024,
        mln_body_permits: None,
        mln_regular_permits: None,
        mln_resource_private_hosts: Vec::new(),
        disable_mln_file_sources: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use biei_core::types::{StyleId, StyleRevision};

    #[test]
    fn resolves_capacity_defaults_without_process_state() {
        let options = Options::resolve(test_input("https://styles.test/{style_id}/style.json", 16))
            .expect("options resolve");

        assert_eq!(options.render_permits_per_node, 16);
        assert_eq!(options.native_render_permits_per_node, 16);
        assert_eq!(options.renderer_slots_per_node, 20);
        assert_eq!(options.mln_body_permits, 64);
        assert_eq!(options.mln_regular_permits, 128);
        assert!(matches!(
            options.cluster_config().bl_capacity,
            BlCapacityPolicy::Fixed(1)
        ));
    }

    #[test]
    fn validates_auth_catalog_without_retaining_roots_in_debug_output() {
        let mut input = test_input("https://styles.test/{style_id}/style.json", 1);
        input.auth_registries = "public=gs://private-auth-bucket/registries/public/".to_string();
        input.auth_provider_origin = Some("https://styles.test".to_string());
        let options = Options::resolve(input).expect("auth registry catalog");

        assert!(!options.auth_registries.is_empty());
        assert_eq!(
            options.auth_provider_origin.as_ref().unwrap().as_str(),
            "https://styles.test/"
        );
        let debug = format!("{options:?}");
        assert!(debug.contains("public"));
        assert!(!debug.contains("private-auth-bucket"));

        let mut invalid = test_input("https://styles.test/{style_id}/style.json", 1);
        invalid.auth_registries = "public=gs://bucket/not-a-directory".to_string();
        assert!(Options::resolve(invalid).is_err());
    }

    #[test]
    fn auth_requires_one_explicit_exact_provider_origin() {
        let mut missing = test_input("https://styles.test/{style_id}/style.json", 1);
        missing.auth_registries = "public=gs://bucket/auth/".to_string();
        assert!(Options::resolve(missing).is_err());

        for invalid_origin in [
            "https://styles.test/path",
            "https://user@styles.test",
            "https://styles.test?token=secret",
            "file:///tmp/provider",
        ] {
            let mut invalid = test_input("https://styles.test/{style_id}/style.json", 1);
            invalid.auth_registries = "public=gs://bucket/auth/".to_string();
            invalid.auth_provider_origin = Some(invalid_origin.to_string());
            assert!(
                Options::resolve(invalid).is_err(),
                "{invalid_origin} must not be accepted as an exact provider origin"
            );
        }
    }

    #[test]
    fn rejects_invalid_resource_templates() {
        let mut input = test_input("https://styles.test/static.json", 1);
        let error = Options::resolve(input.clone()).expect_err("style placeholder required");
        assert!(error.to_string().contains("must contain {style_id}"));

        input.style_templates = "https://styles.test/{style_id}.json".to_string();
        input.tileset_url_template = "https://tiles.test/static.json".to_string();
        let error = Options::resolve(input).expect_err("tileset placeholder required");
        assert!(error.to_string().contains("must contain {tileset_id}"));
    }

    #[test]
    fn placeholders_are_accepted_only_in_url_paths() {
        for style_template in [
            "https://{style_id}.styles.test/style.json",
            "https://styles.test/style.json?id={style_id}",
        ] {
            let input = test_input(style_template, 1);
            let error = Options::resolve(input).expect_err("style placeholder outside path");
            assert!(error.to_string().contains("only in the URL path"));
        }

        let mut input = test_input("https://styles.test/{style_id}.json", 1);
        input.tileset_url_template = "https://tiles.test/tileset.json?id={tileset_id}".to_string();
        let error = Options::resolve(input).expect_err("tileset placeholder outside path");
        assert!(error.to_string().contains("only in the URL path"));
    }

    #[test]
    fn style_template_forms_and_ambiguities_are_explicit() {
        let parsed = parse_style_templates(
            "gl=https://styles.test/{style_id}.json;default=https://fallback.test/{style_id}.json",
        )
        .expect("namespace and default parse");
        assert_eq!(parsed.namespaces()[0].0, "gl");
        assert_eq!(
            parsed.default_template(),
            Some("https://fallback.test/{style_id}.json")
        );

        let query = parse_style_templates("https://styles.test/{style_id}.json?key=abc123")
            .expect("query equals is part of a bare template");
        assert!(query.namespaces().is_empty());
        assert_eq!(
            query.default_template(),
            Some("https://styles.test/{style_id}.json?key=abc123")
        );

        for invalid in [
            "  ;; ",
            "https://a.test/{style_id}.json;default=https://b.test/{style_id}.json",
            "gl=https://a.test/{style_id}.json;gl=https://b.test/{style_id}.json",
        ] {
            assert!(
                parse_style_templates(invalid).is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[test]
    fn explicit_capacity_overrides_remain_bounded() {
        let mut input = test_input("https://styles.test/{style_id}.json", 16);
        input.renderer_slots = Some(24);
        input.render_permits = Some(12);
        input.native_render_permits = Some(20);
        input.mln_body_permits = Some(7);
        input.mln_regular_permits = Some(5);
        let options = Options::resolve(input).expect("options resolve");

        assert_eq!(options.renderer_slots_per_node, 24);
        assert_eq!(options.render_permits_per_node, 12);
        assert_eq!(options.native_render_permits_per_node, 12);
        assert_eq!(options.mln_body_permits, 7);
        assert_eq!(options.mln_regular_permits, 7);
    }

    #[test]
    fn rejects_unbounded_queue_and_invalid_private_host() {
        let mut input = test_input("https://styles.test/{style_id}.json", 1);
        input.queue_capacity_multiplier = 5;
        let error = Options::resolve(input).expect_err("queue must be bounded");
        assert!(error.to_string().contains("between 1 and 4"));

        let mut input = test_input("https://styles.test/{style_id}.json", 1);
        input.mln_resource_private_hosts = vec!["https://internal.test".to_string()];
        let error = Options::resolve(input).expect_err("URL is not a host pattern");
        assert!(
            error
                .to_string()
                .contains("invalid --mln-resource-private-hosts")
        );
    }

    #[test]
    fn cluster_requires_explicit_routable_gossip_address() {
        let mut input = test_input("https://styles.test/{style_id}.json", 1);
        input.cluster = true;
        input.internal_advertise_addr = Some("127.0.0.1:9090".parse().unwrap());

        let error = Options::resolve(input.clone()).expect_err("explicit gossip address required");
        assert!(
            error
                .to_string()
                .contains("explicit --gossip-advertise-addr")
        );

        input.gossip_advertise_addr = Some("0.0.0.0:7946".parse().unwrap());
        let error = Options::resolve(input).expect_err("wildcard gossip address rejected");
        assert!(error.to_string().contains("is a wildcard"));
    }

    #[test]
    fn cluster_requires_explicit_routable_internal_address() {
        let mut input = test_input("https://styles.test/{style_id}.json", 1);
        input.cluster = true;
        input.http_bind = "0.0.0.0:8080".parse().unwrap();
        input.gossip_advertise_addr = Some("127.0.0.1:7946".parse().unwrap());
        let error = Options::resolve(input).expect_err("wildcard advertise rejected");
        assert!(
            error
                .to_string()
                .contains("non-wildcard --internal-advertise-addr")
        );
    }

    #[test]
    fn cluster_seeds_require_explicit_cluster_intent() {
        let mut input = test_input("https://styles.test/{style_id}.json", 1);
        input.gossip_seeds = vec!["127.0.0.1:7946".to_string()];
        let error = Options::resolve(input.clone()).expect_err("implicit cluster rejected");
        assert!(error.to_string().contains("use --cluster"));

        input.cluster = true;
        input.gossip_advertise_addr = Some("127.0.0.1:7946".parse().unwrap());
        input.internal_advertise_addr = Some("127.0.0.1:9090".parse().unwrap());
        let options = Options::resolve(input).expect("explicit cluster resolves");
        assert_eq!(
            options.gossip_endpoint.advertise_addr(),
            "127.0.0.1:7946".parse().unwrap()
        );
        assert_eq!(options.gossip_seeds, ["127.0.0.1:7946"]);
    }

    #[test]
    fn catalogs_preserve_namespaces_and_raw_path_replacement() {
        let mut input = test_input(
            "gl=https://styles.test/{style_id}/style.json;default=https://fallback.test/{style_id}.json",
            1,
        );
        input.tileset_url_template = "https://tiles.test/{tileset_id}/tileset.json".to_string();
        let options = Options::resolve(input).expect("options resolve");

        assert_eq!(
            options
                .tileset_url_template
                .replace("{tileset_id}", "analysis/sample"),
            "https://tiles.test/analysis/sample/tileset.json"
        );
        assert_eq!(
            options
                .build_style_catalog()
                .definition_for_revision(&StyleRevision {
                    id: StyleId("gl/voyager".to_string()),
                    version: 1,
                })
                .expect("namespace resolves")
                .style_url,
            "https://styles.test/voyager/style.json"
        );
    }
}
