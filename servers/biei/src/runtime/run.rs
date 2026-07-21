//! Server entry point.

use std::future::Future;
use std::time::Duration;

use crate::options::Options;
use crate::runtime::Runtime;
use mmpf_cluster::{BootstrapReadinessGate, DEFAULT_BOOTSTRAP_GRACE};

const DRAIN_GRACE: Duration = Duration::from_secs(10);

/// Run a configured Biei node until the supplied shutdown future resolves.
pub(crate) async fn run<F>(options: Options, shutdown_requested: F) -> anyhow::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    tracing::info!(
        cluster = options.cluster,
        require_gossip_bootstrap = options.require_gossip_bootstrap,
        http_bind = %options.http_bind,
        node_id = %options.node_id,
        "starting biei"
    );
    // Must precede Runtime::spawn_*: the registration is process-global and
    // does not update MapLibre Native file sources that renderers have already
    // cached.
    if options.disable_mln_file_sources {
        tracing::warn!("Rust FileSources disabled; using MapLibre Native default resource loader");
    } else {
        mmpf_mln_filesource::register_file_sources(
            options.mln_resource_cache_capacity_bytes,
            options.mln_resource_private_hosts.clone(),
            mmpf_mln_filesource::FileSourceIoPermits {
                regular: options.mln_regular_permits,
                body: options.mln_body_permits,
            },
            crate::renderer::RESOURCE_USER_AGENT,
        )?;
    }
    crate::renderer::overlay::configure_pin_label_font_path(options.pin_label_font_path.clone())?;
    let (runtime, membership_owner) = if options.cluster {
        tracing::info!(
            internal_advertise_addr = %options.internal_advertise_addr,
            internal_bind = %options.internal_bind,
            gossip_bind = %options.gossip_endpoint.listen_addr(),
            gossip_advertise_addr = %options.gossip_endpoint.advertise_addr(),
            gossip_seeds = options.gossip_seeds.len(),
            "starting cluster runtime"
        );
        let (runtime, owner) = Runtime::spawn_cluster_node(&options).await?;
        (runtime, Some(owner))
    } else {
        tracing::info!("starting single-node runtime");
        (Runtime::spawn_single_node(&options)?, None)
    };
    let ingress = runtime.http_ingress(options.sla);
    let (shutdown, shutdown_task) = install_shutdown_handler(runtime.clone(), shutdown_requested);
    let shutdown_observer = shutdown.clone();
    let serve_result = if options.cluster {
        let internal_forward = crate::http::internal::InternalForwardEndpoint::with_drain_and_limit(
            runtime.node(),
            runtime.drain_controller(),
            runtime.internal_forward_concurrency_limit(),
        );
        crate::http::adapter::serve_with_shutdown_and_membership_and_internal_forward(
            ingress,
            options.http_bind,
            options.internal_bind,
            Some(shutdown),
            runtime
                .membership()
                .expect("cluster runtime must own membership"),
            BootstrapReadinessGate::new(options.require_gossip_bootstrap, DEFAULT_BOOTSTRAP_GRACE),
            Some(internal_forward),
        )
        .await
    } else {
        crate::http::adapter::serve_with_shutdown(ingress, options.http_bind, Some(shutdown)).await
    };
    finish_shutdown(shutdown_observer, shutdown_task).await;
    // HTTP is stopped and in-flight requests drained; now close render admission
    // and join the renderer workers within a bound. A native render still running
    // at the deadline is detached (never aborted) and reported, so the pod's
    // shutdown is bounded and its cleanliness is observable rather than silent.
    let worker_shutdown = runtime
        .node()
        .shutdown(tokio::time::Instant::now() + DRAIN_GRACE)
        .await;
    if worker_shutdown.is_complete() {
        tracing::info!(
            joined = worker_shutdown.joined,
            "renderer workers shut down cleanly"
        );
    } else {
        tracing::warn!(
            joined = worker_shutdown.joined,
            detached = worker_shutdown.timed_out,
            "renderer workers did not all finish within the shutdown grace; detaching in-flight renders"
        );
    }
    let membership_shutdown_result = if let Some(owner) = membership_owner {
        owner.shutdown().await
    } else {
        Ok(())
    };
    serve_result?;
    membership_shutdown_result.map_err(|error| anyhow::anyhow!("stop cluster membership: {error}"))
}

fn install_shutdown_handler<F>(
    runtime: Runtime,
    shutdown_requested: F,
) -> (
    crate::http::adapter::ShutdownSignal,
    tokio::task::JoinHandle<()>,
)
where
    F: Future<Output = ()> + Send + 'static,
{
    let (tx, signal) = crate::http::adapter::shutdown_channel();
    let task = tokio::spawn(async move {
        shutdown_requested.await;
        tracing::info!(
            drain_grace_ms = DRAIN_GRACE.as_millis(),
            "shutdown signal received"
        );
        runtime.begin_draining().await;
        tracing::info!("notifying HTTP listener to shut down");
        let _ = tx.send(true);
        tracing::info!(
            drain_grace_ms = DRAIN_GRACE.as_millis(),
            "waiting for in-flight requests to drain"
        );
        let drained = runtime.wait_for_drain(DRAIN_GRACE).await;
        if drained {
            tracing::info!("in-flight requests drained");
        } else {
            tracing::warn!("drain grace elapsed with in-flight requests remaining");
        }
    });
    (signal, task)
}

/// Keep the runtime and its drain accounting alive after the HTTP listeners
/// have stopped. A disconnected client can let hyper drop its handler while
/// the separately spawned, non-cancellable render still owns a drain permit.
async fn finish_shutdown(
    signal: crate::http::adapter::ShutdownSignal,
    mut task: tokio::task::JoinHandle<()>,
) {
    if signal.is_triggered() {
        if let Err(error) = (&mut task).await {
            tracing::error!(%error, "shutdown coordinator terminated unexpectedly");
        }
    } else {
        // Listener failure before SIGTERM must surface immediately rather than
        // waiting forever for a shutdown signal that may never arrive.
        task.abort();
        let _ = task.await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::finish_shutdown;

    #[tokio::test(start_paused = true)]
    async fn main_lifecycle_waits_for_drain_coordinator_after_http_stops() {
        let (tx, signal) = crate::http::adapter::shutdown_channel();
        tx.send(true).expect("trigger shutdown");
        let completed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let completed = Arc::clone(&completed);
            async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                completed.store(true, Ordering::Release);
            }
        });
        let finish = tokio::spawn(finish_shutdown(signal, task));

        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(!finish.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        finish.await.expect("finish task");
        assert!(completed.load(Ordering::Acquire));
    }
}
