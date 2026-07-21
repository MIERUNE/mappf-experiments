//! Dual-listener serving harness shared by the MMPF servers.
//!
//! Both the `biei` and `ishikari` servers front a Gateway-exposed public
//! listener and a cluster-internal listener, driven by a single shutdown
//! signal. This module owns that common machinery: it fans one shutdown future
//! out to both listeners, serves each with graceful shutdown, `try_join!`s them
//! so one listener's error surfaces immediately, and — when a force grace is
//! configured — drops remaining connections once the grace elapses.
//!
//! The two callers differ only in whether they force-drop after a grace window
//! (`biei`) or wait indefinitely for pure graceful shutdown (`ishikari`); that
//! asymmetry is captured entirely by the `force_grace` parameter.

use std::future::{Future, IntoFuture};
use std::time::Duration;

use anyhow::Context as _;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

/// Serve a `public` and an `internal` `(listener, router)` pair, both driven by
/// a single `shutdown` future.
///
/// The `shutdown` future is awaited once and fanned out to both listeners'
/// graceful-shutdown futures. With `force_grace = Some(d)`, each listener races
/// graceful completion against a `d` timer that starts when `shutdown` fires and
/// force-drops surviving connections when it elapses. With `force_grace = None`,
/// both listeners shut down purely gracefully and wait indefinitely.
///
/// Uses `try_join!` so that if one listener errors, the error surfaces
/// immediately and the other listener is dropped rather than blocked on.
pub async fn serve_dual(
    public: (TcpListener, axum::Router),
    internal: (TcpListener, axum::Router),
    shutdown: impl Future<Output = ()> + Send + 'static,
    force_grace: Option<Duration>,
) -> anyhow::Result<()> {
    let (public_listener, public_router) = public;
    let (internal_listener, internal_router) = internal;

    // Fan the single shutdown signal out to both graceful-shutdown futures (and,
    // when a force grace is configured, to each listener's force-drop timer).
    // Every receiver is subscribed before the relay task can send.
    let (sd_tx, _) = broadcast::channel::<()>(1);
    let mut rx_public = sd_tx.subscribe();
    let mut rx_internal = sd_tx.subscribe();
    let public_force = force_grace.map(|grace| (sd_tx.subscribe(), grace));
    let internal_force = force_grace.map(|grace| (sd_tx.subscribe(), grace));
    let shutdown_relay = tokio::spawn(async move {
        shutdown.await;
        let _ = sd_tx.send(());
    });

    let public_server = axum::serve(public_listener, public_router)
        .with_graceful_shutdown(async move {
            let _ = rx_public.recv().await;
        })
        .into_future();
    let internal_server = axum::serve(internal_listener, internal_router)
        .with_graceful_shutdown(async move {
            let _ = rx_internal.recv().await;
        })
        .into_future();

    let serve_result = tokio::try_join!(
        drive(
            public_server,
            public_force,
            "public HTTP shutdown grace elapsed; dropping active connections",
            "serve public HTTP listener",
        ),
        drive(
            internal_server,
            internal_force,
            "internal HTTP shutdown grace elapsed; dropping active connections",
            "serve internal listener",
        ),
    );

    // On graceful shutdown the relay has already completed. If either listener
    // fails first, do not leave the shutdown future (often an OS signal waiter)
    // detached in an embedding runtime.
    if !shutdown_relay.is_finished() {
        shutdown_relay.abort();
    }
    let _ = shutdown_relay.await;

    serve_result.map(|_| ())
}

/// Drive one listener's serve future, optionally racing it against a force-drop
/// timer that starts when the shutdown signal fires.
async fn drive<F>(
    server: F,
    force: Option<(broadcast::Receiver<()>, Duration)>,
    warn_msg: &'static str,
    err_ctx: &'static str,
) -> anyhow::Result<()>
where
    F: Future<Output = std::io::Result<()>>,
{
    match force {
        Some((mut force_rx, grace)) => {
            tokio::select! {
                result = server => {
                    let result = result.context(err_ctx);
                    if result.is_ok() {
                        tracing::info!(listener = err_ctx, "HTTP listener shutdown completed gracefully");
                    }
                    result
                },
                () = async {
                    // The grace window starts only once shutdown fires, matching
                    // the "wait for signal, then sleep grace" force-drop timer.
                    let _ = force_rx.recv().await;
                    tokio::time::sleep(grace).await;
                } => {
                    tracing::warn!(grace_ms = grace.as_millis(), "{warn_msg}");
                    Ok(())
                }
            }
        }
        None => server.await.context(err_ctx),
    }
}

/// Resolves once a shutdown signal is received.
///
/// On Unix this awaits `SIGTERM` (installing the handler, falling back to
/// Ctrl-C alone if that install fails) or Ctrl-C, whichever fires first. On
/// non-Unix targets it awaits Ctrl-C. Shared by the `biei` and `ishikari`
/// servers as the `shutdown` future driving `serve_dual`.
pub async fn wait_for_shutdown_signal() {
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
