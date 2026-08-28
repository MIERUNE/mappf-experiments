//! Production runtime assembly for Ishikari.

use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::drain::DrainController;
use crate::internal_transport::HttpInternalTransport;
use crate::membership::{Membership, MembershipConfig};
use crate::options::Options;
use crate::server::{
    AppState, ServerRuntimeConfig, run_http_server, tileset::mapterhorn::MapterhornResolver,
};
use ishikari_core::{
    metrics::NodeMetrics,
    storage::{
        ObjectStoreRegistry, ResourceCacheCapacities, ResourceResolver, ResourceResolverConfig,
    },
};
use mmpf_cluster::{BootstrapReadinessGate, DEFAULT_BOOTSTRAP_GRACE};

const DRAIN_PUBLICATION_TIMEOUT: Duration = Duration::from_secs(2);
const DRAINING_PROPAGATION_DELAY: Duration = Duration::from_secs(2);
const MEMBERSHIP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Run a configured Ishikari node until the supplied shutdown future resolves.
pub(crate) async fn run<F>(
    options: Options,
    auth: Option<mmpf_auth::DeliveryAuth>,
    shutdown_requested: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let resolver_tuning = options.resolver_tuning;
    let cache_capacities = options.cache_capacities;
    info!(
        clustered = options.clustered,
        http_listen_addr = %options.http_listen_addr,
        internal_listen_addr = %options.internal_listen_addr,
        http_port = options.http_listen_addr.port(),
        require_gossip_bootstrap = options.require_gossip_bootstrap,
        tileset_source_count = options.tileset_source_inventory.source_count(),
        tileset_source_default = options.tileset_source_inventory.has_default(),
        tileset_source_backends = ?options.tileset_source_inventory.backend_kinds(),
        chunk_size_bytes = resolver_tuning.chunk_size_bytes(),
        max_fetch_chunks = resolver_tuning.max_fetch_chunks(),
        chunk_fetch_merge_window_ms = resolver_tuning.chunk_fetch_merge_window().as_millis(),
        archive_revalidation_interval_secs = resolver_tuning.archive_revalidation_interval().as_secs(),
        backend_fetch_concurrency = resolver_tuning.backend_fetch_concurrency(),
        backend_fetch_max_inflight = resolver_tuning.backend_fetch_max_inflight(),
        backend_max_active_body_bytes = options.backend_max_active_body_bytes,
        backend_active_body_budget_bytes = options.backend_active_body_budget_bytes,
        provider_fetch_concurrency = options.provider_fetch_concurrency,
        provider_max_active_body_bytes = options.provider_max_active_body_bytes,
        provider_active_body_budget_bytes = options.provider_active_body_budget_bytes,
        artificial_backend_delay_ms = options.artificial_backend_delay_ms,
        tile_cache_max_bytes = resolver_tuning.tile_cache_max_bytes(),
        chunk_cache_max_bytes = resolver_tuning.chunk_cache_max_bytes(),
        cache_weight_budget_bytes = cache_capacities.budget_bytes(),
        cache_configured_weight_bytes = cache_capacities.configured_weight_bytes(),
        cpu_work_concurrency = options.cpu_work_concurrency,
        delivery_auth_enabled = auth.is_some(),
        anonymous_access_enabled = options.anonymous_registry.is_some(),
        "starting ishikari"
    );
    if options.clustered {
        info!(
            gossip_bind = %options.membership.gossip_endpoint.listen_addr(),
            gossip_advertise_addr = %options.membership.gossip_endpoint.advertise_addr(),
            internal_http_advertise_addr = %options.membership.http_advertise_addr,
            seed_nodes = ?options.membership.seed_nodes,
            "starting Chitchat membership"
        );
    } else {
        info!("using local membership; UDP gossip and peer routing are disabled");
    }

    let mapterhorn = options
        .mapterhorn
        .map(MapterhornResolver::new)
        .map(Arc::new);

    // Build fallible non-membership dependencies before opening the gossip socket.
    let internal_transport = Arc::new(HttpInternalTransport::new()?);
    let clustered = options.clustered;
    let self_node_id = options.membership.node_id.clone();
    let (membership, membership_owner) = start_membership(clustered, options.membership).await?;
    let gossip_bootstrap_readiness =
        BootstrapReadinessGate::new(options.require_gossip_bootstrap, DEFAULT_BOOTSTRAP_GRACE);
    let metrics = NodeMetrics::new();
    let drain = DrainController::new();
    // Shared by tile reads and provider fetches so stores (connection pools and
    // credentials) are reused per bucket/host across both.
    // Process-global credential and object-store configuration belongs to the
    // production composition root, not `ishikari-core`.
    let object_store_registry = Arc::new(ObjectStoreRegistry::new(std::env::vars()));

    // The concrete reqwest-based internal transport is owned by the server and
    // injected into the core peer backend through the resolver config.
    let resource_resolver_result = ResourceResolver::new(ResourceResolverConfig {
        self_node_id,
        peer_directory: Arc::new(membership.clone()),
        transport: internal_transport,
        tileset_sources: options.tileset_sources,
        tuning: resolver_tuning,
        cache_capacities: ResourceCacheCapacities {
            resource_max_bytes: cache_capacities.resource_bytes(),
            archive_max_bytes: cache_capacities.archive_bytes(),
            leaf_max_bytes: cache_capacities.leaf_bytes(),
        },
        artificial_backend_delay_ms: options.artificial_backend_delay_ms,
        object_store_registry: object_store_registry.clone(),
        metrics: metrics.clone(),
    });
    let resource_resolver = match resource_resolver_result {
        Ok(resource_resolver) => Arc::new(resource_resolver),
        Err(error) => {
            return match shutdown_membership(membership_owner).await {
                Ok(()) => Err(error),
                Err(shutdown_error) => {
                    Err(error.context(format!("membership cleanup also failed: {shutdown_error}")))
                }
            };
        }
    };

    let stats_reporter = clustered.then(|| {
        spawn_stats_reporter(
            membership.clone(),
            resource_resolver.clone(),
            metrics.clone(),
        )
    });

    // Registry freshness must be visible on the scrape that also carries the
    // authorization outcomes it explains: during a registry outage the grants
    // in use are older than they look, and only this age says how much older.
    if let Some(auth) = auth.clone() {
        metrics.add_extra_metrics_source(Box::new(move || auth.gather_metrics()));
    }
    let app_state = AppState::new(
        membership.clone(),
        metrics,
        resource_resolver,
        drain.clone(),
        options.provider,
        object_store_registry,
        ServerRuntimeConfig {
            gossip_bootstrap_readiness,
            delivery_auth: auth,
            mapterhorn,
            cpu_work_concurrency: options.cpu_work_concurrency,
            cpu_work_max_inflight: options.cpu_work_max_inflight,
            derived_negative_ttl: resolver_tuning.tile_negative_ttl(),
            cache_capacities,
            provider_fetch_concurrency: options.provider_fetch_concurrency,
        },
    );
    let refresh_state = app_state.clone();
    membership
        .spawn_style_refresh_watcher(move |hint| {
            if crate::server::style::request_style_revalidation(&refresh_state, &hint.style_id)
                .is_ok()
            {
                tracing::info!(
                    hint_id = %hint.hint_id,
                    style_id = %hint.style_id,
                    "applied gossiped style refresh"
                );
            }
        })
        .await;

    let serve_result = run_http_server(
        app_state,
        options.http_listen_addr,
        options.internal_listen_addr,
        shutdown_signal(shutdown_requested, membership.clone(), drain),
    )
    .await;

    if let Some(stats_reporter) = stats_reporter {
        stats_reporter.abort();
        let _ = stats_reporter.await;
    }
    let membership_shutdown_result = shutdown_membership(membership_owner).await;
    serve_result?;
    membership_shutdown_result
}

async fn shutdown_signal<F>(shutdown_requested: F, membership: Membership, drain: DrainController)
where
    F: Future<Output = ()>,
{
    shutdown_requested.await;
    info!("shutdown signal received; draining");
    // Stop admitting new data/peer requests locally first, then announce
    // draining to peers before asking the HTTP listeners to finish in-flight work.
    drain.begin();
    if membership.is_clustered() {
        if !draining_publication_completes(membership.set_draining(true)).await {
            warn!(
                timeout_ms = DRAIN_PUBLICATION_TIMEOUT.as_millis(),
                "timed out publishing draining membership state; continuing shutdown"
            );
        }
        tokio::time::sleep(DRAINING_PROPAGATION_DELAY).await;
    }
}

async fn draining_publication_completes(publish: impl Future<Output = ()>) -> bool {
    tokio::time::timeout(DRAIN_PUBLICATION_TIMEOUT, publish)
        .await
        .is_ok()
}

async fn shutdown_membership(owner: Option<mmpf_cluster::ClusterOwner>) -> Result<()> {
    let Some(owner) = owner else {
        return Ok(());
    };
    match tokio::time::timeout(MEMBERSHIP_SHUTDOWN_TIMEOUT, owner.shutdown()).await {
        Ok(result) => {
            result.context("failed to stop chitchat")?;
            info!("membership shutdown completed gracefully");
            Ok(())
        }
        Err(_) => Err(anyhow::anyhow!(
            "timed out stopping chitchat after {} ms",
            MEMBERSHIP_SHUTDOWN_TIMEOUT.as_millis()
        )),
    }
}

async fn start_membership(
    clustered: bool,
    config: MembershipConfig,
) -> Result<(Membership, Option<mmpf_cluster::ClusterOwner>)> {
    if clustered {
        let (membership, owner) = Membership::spawn_cluster(config).await?;
        Ok((membership, Some(owner)))
    } else {
        Ok((Membership::local(config.node_id), None))
    }
}

fn spawn_stats_reporter(
    membership: Membership,
    resource_resolver: Arc<ResourceResolver>,
    metrics: NodeMetrics,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(STATS_REPORT_INTERVAL);
        loop {
            ticker.tick().await;
            membership
                .set_many(&[
                    (
                        "cache-tile-bytes",
                        resource_resolver.tile_cache_weighted_size().to_string(),
                    ),
                    (
                        "cache-chunk-bytes",
                        resource_resolver.chunk_cache_weighted_size().to_string(),
                    ),
                    (
                        "transfer-external-bytes",
                        metrics.egress_bytes().to_string(),
                    ),
                    (
                        "transfer-internal-bytes",
                        metrics.internal_bytes().to_string(),
                    ),
                    (
                        "transfer-backend-bytes",
                        resource_resolver.received_bytes().to_string(),
                    ),
                ])
                .await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_runtime_membership_does_not_open_the_configured_udp_socket() {
        let occupied = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = occupied.local_addr().unwrap();
        let config = MembershipConfig {
            node_id: "local-node".to_string(),
            gossip_endpoint: mmpf_cluster::GossipEndpoint::standalone(address, address),
            http_advertise_addr: "127.0.0.1:9090".parse().unwrap(),
            seed_nodes: Vec::new(),
            gossip_interval: Duration::from_millis(200),
        };

        let (membership, owner) = start_membership(false, config).await.unwrap();

        assert!(!membership.is_clustered());
        assert!(owner.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn draining_publication_timeout_is_bounded() {
        let started = tokio::time::Instant::now();

        assert!(
            !draining_publication_completes(std::future::pending::<()>()).await,
            "a stuck membership update must time out"
        );
        assert_eq!(started.elapsed(), DRAIN_PUBLICATION_TIMEOUT);
        assert!(draining_publication_completes(std::future::ready(())).await);
    }

    #[tokio::test(start_paused = true)]
    async fn local_shutdown_skips_gossip_publication_and_propagation_delay() {
        let membership = Membership::local("local-node".to_string());
        let drain = DrainController::new();
        let started = tokio::time::Instant::now();

        shutdown_signal(std::future::ready(()), membership, drain.clone()).await;

        assert!(drain.is_draining());
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[test]
    fn shutdown_budget_fits_deployment_termination_grace() {
        const DEPLOYMENT_TERMINATION_GRACE: Duration = Duration::from_secs(25);
        let bound = DRAIN_PUBLICATION_TIMEOUT
            + DRAINING_PROPAGATION_DELAY
            + crate::server::HTTP_SHUTDOWN_GRACE
            + MEMBERSHIP_SHUTDOWN_TIMEOUT;

        assert!(bound < DEPLOYMENT_TERMINATION_GRACE);
    }
}
