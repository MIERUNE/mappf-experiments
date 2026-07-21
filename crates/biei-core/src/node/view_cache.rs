use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use mmpf_common::sync::lock_unpoisoned;
use tokio::{sync::watch, time::Instant};

use crate::{gossip::GossipBus, types::ClusterView};

pub(super) const MAX_TTL: Duration = Duration::from_millis(100);
pub(super) const MIN_TTL: Duration = Duration::from_millis(1);

pub(super) struct ClusterViewCache {
    ttl: Duration,
    state: Mutex<ClusterViewCacheState>,
    changed: watch::Sender<u64>,
}

#[derive(Default)]
struct ClusterViewCacheState {
    cached: Option<CachedClusterView>,
    loading: bool,
}

struct CachedClusterView {
    expires_at: Instant,
    gossip_epoch: u64,
    view: Arc<ClusterView>,
}

impl ClusterViewCache {
    pub(super) fn new(ttl: Duration) -> Self {
        let (changed, _) = watch::channel(0);
        Self {
            ttl,
            state: Mutex::new(ClusterViewCacheState::default()),
            changed,
        }
    }

    pub(super) async fn get_or_load(
        &self,
        gossip: &dyn GossipBus,
        deadline: Instant,
    ) -> Option<Arc<ClusterView>> {
        loop {
            if Instant::now() >= deadline {
                return None;
            }
            // Avoid constructing a watch receiver on the normal fresh-cache
            // path. The second check below closes the completion race before
            // a caller can wait.
            if let Some(view) = {
                let state = lock_unpoisoned(&self.state);
                usable_cached_view(&state, self.ttl, gossip.view_epoch())
            } {
                return Some(view);
            }

            let mut changed = self.changed.subscribe();
            let should_load = {
                let mut state = lock_unpoisoned(&self.state);
                if let Some(view) = usable_cached_view(&state, self.ttl, gossip.view_epoch()) {
                    return Some(view);
                }
                if state.loading {
                    false
                } else {
                    state.loading = true;
                    true
                }
            };

            if should_load {
                let load = ClusterViewLoad::new(self);
                let gossip_epoch = gossip.view_epoch();
                let view = match tokio::time::timeout_at(deadline, gossip.view()).await {
                    Ok(view) => Arc::new(view),
                    Err(_) => {
                        drop(load);
                        return None;
                    }
                };
                if gossip.view_epoch() != gossip_epoch {
                    drop(load);
                    continue;
                }
                load.complete(Arc::clone(&view), gossip_epoch);
                return Some(view);
            }

            // `watch` remembers changes that happen after subscribe but
            // before this await, avoiding a lost wakeup on the initial load.
            if tokio::time::timeout_at(deadline, changed.changed())
                .await
                .is_err()
            {
                return None;
            }
        }
    }
}

fn usable_cached_view(
    state: &ClusterViewCacheState,
    stale_grace: Duration,
    gossip_epoch: u64,
) -> Option<Arc<ClusterView>> {
    let cached = state.cached.as_ref()?;
    if cached.gossip_epoch != gossip_epoch {
        return None;
    }
    // A bounded stale snapshot is preferable to making a request wait behind
    // the single refresh already in progress.
    let now = Instant::now();
    let bounded_stale = state.loading
        && cached
            .expires_at
            .checked_add(stale_grace)
            .is_some_and(|stale_until| stale_until > now);
    (cached.expires_at > now || bounded_stale).then(|| Arc::clone(&cached.view))
}

struct ClusterViewLoad<'a> {
    cache: &'a ClusterViewCache,
    complete: bool,
}

impl<'a> ClusterViewLoad<'a> {
    fn new(cache: &'a ClusterViewCache) -> Self {
        Self {
            cache,
            complete: false,
        }
    }

    fn complete(mut self, view: Arc<ClusterView>, gossip_epoch: u64) {
        let mut state = lock_unpoisoned(&self.cache.state);
        state.cached = Some(CachedClusterView {
            expires_at: Instant::now() + self.cache.ttl,
            gossip_epoch,
            view,
        });
        state.loading = false;
        self.complete = true;
        drop(state);
        self.cache.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

impl Drop for ClusterViewLoad<'_> {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        lock_unpoisoned(&self.cache.state).loading = false;
        self.cache.changed.send_modify(|version| {
            *version = version.wrapping_add(1);
        });
    }
}

pub(super) fn cluster_view_cache_ttl(publish_interval: Duration) -> Duration {
    publish_interval.min(MAX_TTL).max(MIN_TTL)
}
