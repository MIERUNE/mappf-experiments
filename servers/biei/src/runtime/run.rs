//! Server entry point.

use std::future::Future;
use std::time::Duration;

use crate::auth::DeliveryAuth;
use crate::options::Options;
use crate::runtime::Runtime;
use mmpf_cluster::{BootstrapReadinessGate, DEFAULT_BOOTSTRAP_GRACE};

/// GKE Spot leaves 22 seconds after the checked-in 3-second `preStop`.
/// Finish application-owned work in 21 seconds, reserving one second for
/// process and kubelet overhead before the 25-second platform force-kill.
const PROCESS_SHUTDOWN_BUDGET: Duration = Duration::from_secs(21);
const DRAIN_GRACE: Duration = Duration::from_secs(10);
const MEMBERSHIP_SHUTDOWN_RESERVE: Duration = Duration::from_secs(2);

/// Run a configured Biei node until the supplied shutdown future resolves.
pub(crate) async fn run<F>(
    options: Options,
    auth: Option<DeliveryAuth>,
    shutdown_requested: F,
) -> anyhow::Result<()>
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
    let ingress = runtime.http_ingress_with_auth(options.sla, auth);
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
    let process_deadline = finish_shutdown(shutdown_observer, shutdown_task)
        .await
        .unwrap_or_else(|| tokio::time::Instant::now() + PROCESS_SHUTDOWN_BUDGET);
    // HTTP is stopped and in-flight requests drained; now close render admission
    // and join the renderer workers within the process-wide deadline. Preserve a
    // small membership teardown window instead of letting worker cleanup consume
    // every remaining millisecond. A native render still running at its deadline
    // is detached (never aborted) and reported.
    let worker_shutdown = runtime
        .node()
        .shutdown(worker_shutdown_deadline(process_deadline))
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
        shutdown_membership(owner, process_deadline).await
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
    tokio::task::JoinHandle<tokio::time::Instant>,
)
where
    F: Future<Output = ()> + Send + 'static,
{
    let (tx, signal) = crate::http::adapter::shutdown_channel();
    let task = tokio::spawn(async move {
        shutdown_requested.await;
        let process_deadline = tokio::time::Instant::now() + PROCESS_SHUTDOWN_BUDGET;
        tx.begin(process_deadline);
        tracing::info!(
            shutdown_budget_ms = PROCESS_SHUTDOWN_BUDGET.as_millis(),
            "shutdown signal received"
        );
        runtime.begin_draining(process_deadline).await;
        tracing::info!("notifying HTTP listener to shut down");
        tx.stop_http();
        let drain_budget = process_deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .min(DRAIN_GRACE);
        tracing::info!(
            drain_budget_ms = drain_budget.as_millis(),
            "waiting for in-flight requests to drain"
        );
        let drained = runtime.wait_for_drain(drain_budget).await;
        if drained {
            tracing::info!("in-flight requests drained");
        } else {
            tracing::warn!("drain grace elapsed with in-flight requests remaining");
        }
        process_deadline
    });
    (signal, task)
}

/// Keep the runtime and its drain accounting alive after the HTTP listeners
/// have stopped. A disconnected client can let hyper drop its handler while
/// the separately spawned, non-cancellable render still owns a drain permit.
async fn finish_shutdown(
    signal: crate::http::adapter::ShutdownSignal,
    mut task: tokio::task::JoinHandle<tokio::time::Instant>,
) -> Option<tokio::time::Instant> {
    let Some(process_deadline) = signal.deadline() else {
        // Listener failure before SIGTERM must surface immediately rather than
        // waiting forever for a shutdown signal that may never arrive.
        task.abort();
        let _ = task.await;
        return None;
    };

    match tokio::time::timeout_at(process_deadline, &mut task).await {
        Ok(Ok(task_deadline)) => {
            debug_assert_eq!(task_deadline, process_deadline);
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "shutdown coordinator terminated unexpectedly");
        }
        Err(_) => {
            tracing::warn!("process shutdown deadline elapsed while waiting for drain coordinator");
            task.abort();
            let _ = task.await;
        }
    }
    Some(process_deadline)
}

fn worker_shutdown_deadline(process_deadline: tokio::time::Instant) -> tokio::time::Instant {
    let membership_deadline = process_deadline
        .checked_sub(MEMBERSHIP_SHUTDOWN_RESERVE)
        .unwrap_or(process_deadline);
    membership_deadline.min(tokio::time::Instant::now() + DRAIN_GRACE)
}

async fn shutdown_membership(
    owner: mmpf_cluster::ClusterOwner,
    process_deadline: tokio::time::Instant,
) -> anyhow::Result<()> {
    match tokio::time::timeout_at(process_deadline, owner.shutdown()).await {
        Ok(result) => result,
        Err(_) => {
            // Dropping ClusterOwner initiates shutdown even if its watcher did
            // not complete. Do not wait past the process-wide platform budget.
            tracing::warn!("process shutdown deadline elapsed while stopping cluster membership");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::{
        DRAIN_GRACE, MEMBERSHIP_SHUTDOWN_RESERVE, PROCESS_SHUTDOWN_BUDGET, finish_shutdown,
        worker_shutdown_deadline,
    };

    #[test]
    fn shutdown_budget_leaves_worker_and_platform_reserves() {
        let fixed_before_workers = super::super::MEMBERSHIP_DRAIN_PUBLISH_TIMEOUT
            + crate::http::adapter::HTTP_SHUTDOWN_GRACE;
        assert!(
            fixed_before_workers + MEMBERSHIP_SHUTDOWN_RESERVE < PROCESS_SHUTDOWN_BUDGET,
            "drain publication, HTTP, and membership reserve must leave worker time"
        );
        assert!(DRAIN_GRACE < PROCESS_SHUTDOWN_BUDGET);
    }

    #[tokio::test(start_paused = true)]
    async fn main_lifecycle_waits_for_drain_coordinator_after_http_stops() {
        let (tx, signal) = crate::http::adapter::shutdown_channel();
        let process_deadline = tokio::time::Instant::now() + PROCESS_SHUTDOWN_BUDGET;
        tx.begin(process_deadline);
        tx.stop_http();
        let completed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let completed = Arc::clone(&completed);
            async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                completed.store(true, Ordering::Release);
                process_deadline
            }
        });
        let finish = tokio::spawn(finish_shutdown(signal, task));

        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(!finish.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(finish.await.expect("finish task"), Some(process_deadline));
        assert!(completed.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn listener_failure_after_sigterm_keeps_original_deadline() {
        let (tx, signal) = crate::http::adapter::shutdown_channel();
        let process_deadline = tokio::time::Instant::now() + PROCESS_SHUTDOWN_BUDGET;
        tx.begin(process_deadline);
        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            process_deadline
        });
        let finish = tokio::spawn(finish_shutdown(signal, task));

        tokio::time::advance(Duration::from_secs(1)).await;

        assert_eq!(finish.await.expect("finish task"), Some(process_deadline));
    }

    #[tokio::test(start_paused = true)]
    async fn drain_coordinator_cannot_outlive_process_deadline() {
        let (tx, signal) = crate::http::adapter::shutdown_channel();
        let process_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        tx.begin(process_deadline);
        tx.stop_http();
        let completed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let completed = Arc::clone(&completed);
            async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                completed.store(true, Ordering::Release);
                process_deadline
            }
        });
        let finish = tokio::spawn(finish_shutdown(signal, task));

        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        assert_eq!(finish.await.expect("finish task"), Some(process_deadline));
        assert!(!completed.load(Ordering::Acquire));
    }

    #[tokio::test(start_paused = true)]
    async fn worker_deadline_preserves_membership_reserve() {
        let process_deadline = tokio::time::Instant::now() + PROCESS_SHUTDOWN_BUDGET;
        tokio::time::advance(Duration::from_secs(15)).await;

        assert_eq!(
            worker_shutdown_deadline(process_deadline),
            process_deadline - MEMBERSHIP_SHUTDOWN_RESERVE
        );
    }
}
