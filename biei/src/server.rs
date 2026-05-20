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
    let shutdown = install_shutdown_handler(runtime.clone());
    if options.cluster {
        let internal_forward = crate::http::internal::InternalForwardEndpoint::with_drain(
            runtime.node(),
            runtime.drain_controller(),
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
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // sharded Registry layer を経由しない FmtSubscriber 単体で init。
    // Registry の sharded storage が `span.enter()` の Drop guard を await 越しに
    // 持つコードで thread-local ID と不整合を起こす(tracing-subscriber 0.3.23 既知)。
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn install_shutdown_handler(runtime: Runtime) -> crate::http::adapter::ShutdownSignal {
    let (tx, signal) = crate::http::adapter::shutdown_channel();
    tokio::spawn(async move {
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
    signal
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
