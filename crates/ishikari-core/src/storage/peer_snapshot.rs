//! The routable peer set: membership snapshots and the directory that
//! supplies them.
//!
//! Kept apart from the peer transport in `peer.rs`: this owns only the
//! one-gossip-tick cache semantics shared by production and simulation, and
//! has no knowledge of provider wire formats, routing, or retries.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{sync::watch, time::Instant};

use mmpf_common::sync::lock_unpoisoned;

/// Reachable peer information supplied by a runtime membership adapter.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct Peer {
    pub id: String,
    pub addr: SocketAddr,
}

pub type PeerFuture<'a> = Pin<Box<dyn Future<Output = Arc<[Peer]>> + Send + 'a>>;

/// Supplies the current routable peer set independently of gossip transport.
pub trait PeerDirectory: Send + Sync {
    fn peers(&self) -> PeerFuture<'_>;
}

/// One-gossip-tick cache of Ishikari's projected routable peer set.
///
/// Runtime adapters own cluster inspection, while this core type owns the
/// production/simulation cache semantics used by resource routing.
#[derive(Clone)]
pub struct PeerSnapshotCache {
    inner: Arc<PeerSnapshotCacheInner>,
}

struct PeerSnapshotCacheInner {
    state: Mutex<PeerSnapshotCacheState>,
    changed: watch::Sender<u64>,
    ttl: Duration,
}

#[derive(Default)]
struct PeerSnapshotCacheState {
    cached: Option<CachedPeerSnapshot>,
    loading: bool,
}

struct CachedPeerSnapshot {
    stored_at: Instant,
    peers: Arc<[Peer]>,
}

impl PeerSnapshotCache {
    pub fn new(ttl: Duration) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            inner: Arc::new(PeerSnapshotCacheInner {
                state: Mutex::new(PeerSnapshotCacheState::default()),
                changed,
                ttl,
            }),
        }
    }

    pub fn get(&self) -> Option<Arc<[Peer]>> {
        fresh_peer_snapshot(&lock_unpoisoned(&self.inner.state), self.inner.ttl)
    }

    fn store(&self, peers: Arc<[Peer]>) {
        let mut state = lock_unpoisoned(&self.inner.state);
        state.cached = Some(CachedPeerSnapshot {
            stored_at: Instant::now(),
            peers,
        });
        state.loading = false;
        drop(state);
        self.notify_changed();
    }

    pub async fn get_or_load<F, Fut>(&self, load: F) -> Arc<[Peer]>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Arc<[Peer]>>,
    {
        let mut load = Some(load);
        loop {
            if let Some(peers) = self.get() {
                return peers;
            }

            // Subscribe before the locked recheck so completion between the
            // first read and waiting cannot be lost.
            let mut changed = self.inner.changed.subscribe();
            let should_load = {
                let mut state = lock_unpoisoned(&self.inner.state);
                if let Some(peers) = fresh_peer_snapshot(&state, self.inner.ttl) {
                    return peers;
                }
                if state.loading {
                    false
                } else {
                    state.loading = true;
                    true
                }
            };

            if should_load {
                let guard = PeerSnapshotLoad::new(self);
                let peers = load.take().expect("peer snapshot loader called once")().await;
                guard.complete(peers.clone());
                return peers;
            }

            if changed.changed().await.is_err() {
                // The sender lives with `self`, so this is only defensive.
                continue;
            }
        }
    }

    fn notify_changed(&self) {
        self.inner.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

fn fresh_peer_snapshot(state: &PeerSnapshotCacheState, ttl: Duration) -> Option<Arc<[Peer]>> {
    state
        .cached
        .as_ref()
        .and_then(|snapshot| (snapshot.stored_at.elapsed() < ttl).then(|| snapshot.peers.clone()))
}

struct PeerSnapshotLoad<'a> {
    cache: &'a PeerSnapshotCache,
    complete: bool,
}

impl<'a> PeerSnapshotLoad<'a> {
    fn new(cache: &'a PeerSnapshotCache) -> Self {
        Self {
            cache,
            complete: false,
        }
    }

    fn complete(mut self, peers: Arc<[Peer]>) {
        self.cache.store(peers);
        self.complete = true;
    }
}

impl Drop for PeerSnapshotLoad<'_> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        lock_unpoisoned(&self.cache.inner.state).loading = false;
        self.cache.notify_changed();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{Barrier, Semaphore};

    use super::{Peer, PeerSnapshotCache};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn peer_snapshot_cache_reuses_live_snapshots_and_expires_zero_ttl() {
        let peers: Arc<[Peer]> = vec![Peer {
            id: "node-a".to_string(),
            addr: "127.0.0.1:9090".parse().unwrap(),
        }]
        .into();
        let cache = PeerSnapshotCache::new(Duration::from_secs(1));
        cache.store(peers.clone());

        let cached = cache.get().expect("live snapshot");
        assert!(Arc::ptr_eq(&cached, &peers));

        let expired = PeerSnapshotCache::new(Duration::ZERO);
        expired.store(peers);
        assert!(expired.get().is_none());
    }

    #[tokio::test]
    async fn peer_snapshot_cache_coalesces_concurrent_loads() {
        const CALLERS: usize = 8;
        let cache = PeerSnapshotCache::new(Duration::from_mins(1));
        let expected: Arc<[Peer]> = vec![Peer {
            id: "node-a".to_string(),
            addr: "127.0.0.1:9090".parse().unwrap(),
        }]
        .into();
        let loads = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(CALLERS + 1));
        let release = Arc::new(Semaphore::new(0));
        let mut tasks = Vec::new();

        for _ in 0..CALLERS {
            let cache = cache.clone();
            let expected = expected.clone();
            let loads = loads.clone();
            let start = start.clone();
            let release = release.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                cache
                    .get_or_load(|| async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        release.acquire().await.unwrap().forget();
                        expected
                    })
                    .await
            }));
        }

        start.wait().await;
        while loads.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        for _ in 0..CALLERS {
            tokio::task::yield_now().await;
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        release.add_permits(1);

        for task in tasks {
            let peers = task.await.unwrap();
            assert!(Arc::ptr_eq(&peers, &expected));
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_cancelled_peer_snapshot_load_wakes_the_waiter_it_left_behind() {
        let cache = PeerSnapshotCache::new(Duration::from_mins(1));
        let loading = Arc::new(Semaphore::new(0));

        // A loader that never finishes, so the next caller has to wait on it.
        let loader = {
            let cache = cache.clone();
            let loading = loading.clone();
            tokio::spawn(async move {
                cache
                    .get_or_load(|| async move {
                        loading.add_permits(1);
                        std::future::pending::<Arc<[Peer]>>().await
                    })
                    .await
            })
        };
        loading.acquire().await.unwrap().forget();

        let expected: Arc<[Peer]> = vec![Peer {
            id: "node-b".to_string(),
            addr: "127.0.0.1:9091".parse().unwrap(),
        }]
        .into();
        let waiter = {
            let cache = cache.clone();
            let expected = expected.clone();
            tokio::spawn(async move { cache.get_or_load(|| std::future::ready(expected)).await })
        };
        // The waiter must already be parked before the abort, or the abort would
        // only be observed by a caller arriving afterwards and the wake-up would
        // go untested. Each caller subscribes before parking, so a second
        // receiver proves this one is committed to waiting.
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.inner.changed.receiver_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second caller must subscribe before it parks");

        loader.abort();
        let _ = loader.await;

        let peers = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("a cancelled loader must wake the waiter it left behind")
            .expect("the waiting task must not panic");
        assert!(Arc::ptr_eq(&peers, &expected));
    }
}
