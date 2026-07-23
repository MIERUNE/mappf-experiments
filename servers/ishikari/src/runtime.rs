//! Production runtime assembly for Ishikari.

use std::{future::Future, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tracing::{info, warn};

use crate::drain::DrainController;
use crate::internal_transport::HttpInternalTransport;
use crate::membership::Membership;
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
        http_listen_addr = %options.http_listen_addr,
        internal_listen_addr = %options.internal_listen_addr,
        http_port = options.http_listen_addr.port(),
        gossip_bind = %options.membership.gossip_endpoint.listen_addr(),
        gossip_advertise_addr = %options.membership.gossip_endpoint.advertise_addr(),
        internal_http_advertise_addr = %options.membership.http_advertise_addr,
        seed_nodes = ?options.membership.seed_nodes,
        require_gossip_bootstrap = options.require_gossip_bootstrap,
        tileset_source_count = options.tileset_source_inventory.source_count(),
        tileset_source_default = options.tileset_source_inventory.has_default(),
        tileset_source_backends = ?options.tileset_source_inventory.backend_kinds(),
        chunk_size_bytes = resolver_tuning.chunk_size_bytes(),
        max_fetch_chunks = resolver_tuning.max_fetch_chunks(),
        chunk_fetch_merge_window_ms = resolver_tuning.chunk_fetch_merge_window().as_millis(),
        backend_fetch_concurrency = resolver_tuning.backend_fetch_concurrency(),
        backend_fetch_max_inflight = resolver_tuning.backend_fetch_max_inflight(),
        backend_max_active_body_bytes = options.backend_max_active_body_bytes,
        backend_active_body_budget_bytes = options.backend_active_body_budget_bytes,
        artificial_backend_delay_ms = options.artificial_backend_delay_ms,
        tile_cache_max_bytes = resolver_tuning.tile_cache_max_bytes(),
        chunk_cache_max_bytes = resolver_tuning.chunk_cache_max_bytes(),
        cache_weight_budget_bytes = cache_capacities.budget_bytes(),
        cache_configured_weight_bytes = cache_capacities.configured_weight_bytes(),
        cpu_work_concurrency = options.cpu_work_concurrency,
        delivery_auth_enabled = auth.is_some(),
        "starting ishikari"
    );

    let mapterhorn = options
        .mapterhorn
        .map(MapterhornResolver::new)
        .map(Arc::new);

    // Build fallible non-membership dependencies before opening the gossip socket.
    let internal_transport = Arc::new(HttpInternalTransport::new()?);
    let self_node_id = options.membership.node_id.clone();
    let (membership, membership_owner) = Membership::spawn(options.membership).await?;
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

    let stats_reporter = spawn_stats_reporter(
        membership.clone(),
        resource_resolver.clone(),
        metrics.clone(),
    );

    let serve_result = run_http_server(
        AppState::new(
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
            },
        ),
        options.http_listen_addr,
        options.internal_listen_addr,
        shutdown_signal(shutdown_requested, membership.clone(), drain),
    )
    .await;

    stats_reporter.abort();
    let _ = stats_reporter.await;
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
    if !draining_publication_completes(membership.set_draining(true)).await {
        warn!(
            timeout_ms = DRAIN_PUBLICATION_TIMEOUT.as_millis(),
            "timed out publishing draining membership state; continuing shutdown"
        );
    }
    tokio::time::sleep(DRAINING_PROPAGATION_DELAY).await;
}

async fn draining_publication_completes(publish: impl Future<Output = ()>) -> bool {
    tokio::time::timeout(DRAIN_PUBLICATION_TIMEOUT, publish)
        .await
        .is_ok()
}

async fn shutdown_membership(owner: mmpf_cluster::ClusterOwner) -> Result<()> {
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
