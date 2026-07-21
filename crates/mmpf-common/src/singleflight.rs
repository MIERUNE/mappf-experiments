//! Cancellation-safe per-key single-flight coordination.

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex},
};

use tokio::sync::watch;

use crate::sync::lock_unpoisoned;

#[derive(Clone)]
pub struct SingleFlight<K, E> {
    entries: Arc<Mutex<HashMap<K, watch::Sender<Option<E>>>>>,
}

impl<K, E> Default for SingleFlight<K, E> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, E> SingleFlight<K, E>
where
    K: Clone + Eq + Hash,
    E: Clone,
{
    pub fn begin(&self, key: K) -> Flight<K, E> {
        let mut entries = lock_unpoisoned(&self.entries);
        if let Some(sender) = entries.get(&key) {
            return Flight::Follower(Follower {
                receiver: sender.subscribe(),
            });
        }

        let (sender, _) = watch::channel(None);
        entries.insert(key.clone(), sender);
        Flight::Leader(LeaderGuard {
            entries: Arc::clone(&self.entries),
            key,
            outcome: None,
        })
    }
}

pub enum Flight<K, E>
where
    K: Eq + Hash,
{
    Leader(LeaderGuard<K, E>),
    Follower(Follower<E>),
}

pub struct Follower<E> {
    receiver: watch::Receiver<Option<E>>,
}

impl<E> Follower<E>
where
    E: Clone,
{
    /// Waits for the leader to finish. A returned value is an outcome that
    /// cannot be recovered from the shared cache. `None` means cached success
    /// (read the cache) or cancellation (retry election).
    pub async fn wait(mut self) -> Option<E> {
        let _ = self.receiver.changed().await;
        self.receiver.borrow().clone()
    }
}

pub struct LeaderGuard<K, E>
where
    K: Eq + Hash,
{
    entries: Arc<Mutex<HashMap<K, watch::Sender<Option<E>>>>>,
    key: K,
    outcome: Option<E>,
}

impl<K, E> LeaderGuard<K, E>
where
    K: Eq + Hash,
{
    /// Publishes a leader result that cannot be recovered from the shared
    /// cache (for example an intentionally uncacheable successful response).
    pub fn complete_with(mut self, value: E) {
        self.outcome = Some(value);
    }

    /// Publishes a transient leader error to every current follower. The key is
    /// removed immediately on drop, so a later independent request can retry.
    pub fn complete_with_error(self, error: E) {
        self.complete_with(error);
    }
}

impl<K, E> Drop for LeaderGuard<K, E>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let sender = lock_unpoisoned(&self.entries).remove(&self.key);
        if let Some(sender) = sender
            && let Some(outcome) = self.outcome.take()
        {
            let _ = sender.send(Some(outcome));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{Flight, SingleFlight};

    #[tokio::test]
    async fn leader_drop_wakes_followers_and_releases_key() {
        let inflight = SingleFlight::<String, String>::default();
        let leader = match inflight.begin("archive".into()) {
            Flight::Leader(guard) => guard,
            Flight::Follower(_) => panic!("first fetch must lead"),
        };
        let follower = match inflight.begin("archive".into()) {
            Flight::Follower(receiver) => receiver,
            Flight::Leader(_) => panic!("concurrent fetch must follow"),
        };

        drop(leader);
        assert!(
            timeout(Duration::from_secs(1), follower.wait())
                .await
                .expect("leader drop must wake follower")
                .is_none()
        );

        assert!(matches!(
            inflight.begin("archive".into()),
            Flight::Leader(_)
        ));
    }

    #[tokio::test]
    async fn leader_error_is_shared_with_all_current_followers() {
        let inflight = SingleFlight::<String, String>::default();
        let leader = match inflight.begin("archive".into()) {
            Flight::Leader(guard) => guard,
            Flight::Follower(_) => panic!("first fetch must lead"),
        };
        let first = match inflight.begin("archive".into()) {
            Flight::Follower(follower) => follower,
            Flight::Leader(_) => panic!("concurrent fetch must follow"),
        };
        let second = match inflight.begin("archive".into()) {
            Flight::Follower(follower) => follower,
            Flight::Leader(_) => panic!("concurrent fetch must follow"),
        };

        leader.complete_with_error("backend unavailable".into());

        assert_eq!(first.wait().await.as_deref(), Some("backend unavailable"));
        assert_eq!(second.wait().await.as_deref(), Some("backend unavailable"));
        assert!(matches!(
            inflight.begin("archive".into()),
            Flight::Leader(_)
        ));
    }
}
