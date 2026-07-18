//! Server entry point.

use std::time::Duration;

use crate::options::Options;
use crate::runtime::Runtime;

const DRAIN_GRACE: Duration = Duration::from_secs(10);

pub async fn run() -> anyhow::Result<()> {
    init_tracing();
    let options = Options::parse()?;
    tracing::info!(
        cluster = options.cluster,
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
        crate::renderer::file_source::register_file_sources(
            options.mln_resource_cache_capacity_bytes,
            options.mln_resource_private_hosts.clone(),
            crate::renderer::file_source::FileSourceIoPermits {
                regular: options.mln_regular_permits,
                body: options.mln_body_permits,
            },
        )?;
    }
    let runtime = if options.cluster {
        tracing::info!(
            internal_advertise_addr = %options.internal_advertise_addr,
            internal_bind = %options.internal_bind,
            gossip_bind = %options.gossip_bind,
            gossip_seeds = options.gossip_seeds.len(),
            "starting cluster runtime"
        );
        Runtime::spawn_cluster_node(&options).await?
    } else {
        tracing::info!("starting single-node runtime");
        Runtime::spawn_single_node(&options)?
    };
    let ingress = runtime.http_ingress(options.sla);
    let (shutdown, shutdown_task) = install_shutdown_handler(runtime.clone());
    let shutdown_observer = shutdown.clone();
    let serve_result = if options.cluster {
        let internal_forward =
            crate::http::internal::InternalForwardEndpoint::with_renderer_health_and_limit(
                runtime.node(),
                runtime.drain_controller(),
                ingress.renderer_supervisor(),
                runtime.internal_forward_concurrency_limit(),
            );
        crate::http::adapter::serve_with_shutdown_and_membership_and_internal_forward(
            ingress,
            options.http_bind,
            options.internal_bind,
            Some(shutdown),
            runtime.membership(),
            Some(internal_forward),
        )
        .await
    } else {
        crate::http::adapter::serve_with_shutdown(ingress, options.http_bind, Some(shutdown)).await
    };
    finish_shutdown(shutdown_observer, shutdown_task).await;
    serve_result
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // sharded Registry layer を経由しない FmtSubscriber 単体で init。
    // Registry の sharded storage が `span.enter()` の Drop guard を await 越しに
    // 持つコードで thread-local ID と不整合を起こす(tracing-subscriber 0.3.23 既知)。
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn install_shutdown_handler(
    runtime: Runtime,
) -> (
    crate::http::adapter::ShutdownSignal,
    tokio::task::JoinHandle<()>,
) {
    let (tx, signal) = crate::http::adapter::shutdown_channel();
    let task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
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

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let Ok(mut terminate) = signal(SignalKind::terminate()) else {
            tracing::warn!("failed to install SIGTERM handler; falling back to Ctrl-C only");
            let _ = tokio::signal::ctrl_c().await;
            return;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
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
