//! Cross-renderer single-flight state and cancellation-safe leader ownership.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use maplibre_native::file_source::{ResourceRequest, Response};
use tokio::sync::Notify;

use super::SharedResourceRequestKey;
use mmpf_common::sync::{lock_unpoisoned, wait_for_change};

/// Per-key serialization is enough; sharding prevents unrelated resource
/// misses from contending on one mutex.
pub(super) const FLIGHT_SHARDS: usize = 32;
const _: () = assert!(FLIGHT_SHARDS.is_power_of_two());

pub(super) type FlightMap = Mutex<HashMap<Arc<FlightKey>, Arc<Flight>>>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct FlightKey {
    pub(super) resource: SharedResourceRequestKey,
    pub(super) persistent: bool,
    pub(super) priority: &'static str,
    pub(super) semantics: FlightRequestSemantics,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub(super) struct FlightRequestSemantics {
    pub(super) cache_allowed: bool,
    pub(super) prior_etag: Option<String>,
    pub(super) prior_modified: Option<SystemTime>,
    pub(super) prior_expires: Option<SystemTime>,
    pub(super) has_prior_data: bool,
}

impl FlightRequestSemantics {
    pub(super) fn from_request(request: &ResourceRequest) -> Self {
        Self {
            cache_allowed: request.loading_methods.has_cache(),
            prior_etag: request.prior_etag.clone(),
            prior_modified: request.prior_modified,
            prior_expires: request.prior_expires,
            has_prior_data: request.prior_data.is_some(),
        }
    }
}

pub(super) struct Flight {
    state: Mutex<FlightState>,
    changed: Notify,
}

enum FlightState {
    Pending,
    Complete(Response),
    Aborted,
}

impl Flight {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(FlightState::Pending),
            changed: Notify::new(),
        }
    }

    pub(super) async fn wait(&self) -> Option<Response> {
        // A lost `complete`/`abort` here would wedge this waiter and the mbgl
        // render blocked on it (Still mode cannot finish an unfinished
        // resource); `wait_for_change` provides the register-before-check
        // discipline that prevents it.
        wait_for_change(&self.changed, || match &*lock_unpoisoned(&self.state) {
            FlightState::Pending => ControlFlow::Continue(()),
            FlightState::Complete(response) => ControlFlow::Break(Some(response.clone())),
            FlightState::Aborted => ControlFlow::Break(None),
        })
        .await
    }

    pub(super) fn complete(&self, response: Response) {
        *lock_unpoisoned(&self.state) = FlightState::Complete(response);
        self.changed.notify_waiters();
    }

    fn abort(&self) {
        *lock_unpoisoned(&self.state) = FlightState::Aborted;
        self.changed.notify_waiters();
    }
}

pub(super) struct FlightLeader<'a> {
    pub(super) flights: &'a FlightMap,
    pub(super) key: Arc<FlightKey>,
    pub(super) flight: Arc<Flight>,
    pub(super) completed: bool,
}

impl FlightLeader<'_> {
    pub(super) fn complete(mut self, response: Response) -> Response {
        let mut flights = lock_unpoisoned(self.flights);
        let map_owns_flight = flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight));
        let owned_refs = 1 + usize::from(map_owns_flight);

        // The response body can be several MiB. Only retain a cloned response
        // in the flight when another caller actually joined it.
        if Arc::strong_count(&self.flight) > owned_refs {
            self.flight.complete(response.clone());
        }
        if map_owns_flight {
            flights.remove(&self.key);
        }
        drop(flights);
        self.completed = true;
        response
    }
}

impl Drop for FlightLeader<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.flight.abort();
        remove_flight(self.flights, &self.key, &self.flight);
    }
}

fn remove_flight(flights: &FlightMap, key: &Arc<FlightKey>, flight: &Arc<Flight>) {
    let mut flights = lock_unpoisoned(flights);
    if flights
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, flight))
    {
        flights.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use maplibre_native::file_source::ErrorReason;
    use std::time::Duration;

    fn response() -> Response {
        Response::error(ErrorReason::NotFound, "singleflight test")
    }

    #[tokio::test]
    async fn concurrent_waiters_observe_completion_without_hanging() {
        let flight = Arc::new(Flight::new());
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let flight = Arc::clone(&flight);
                tokio::spawn(async move { flight.wait().await })
            })
            .collect();
        tokio::task::yield_now().await;
        flight.complete(response());
        for waiter in waiters {
            let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
                .await
                .expect("waiter must not hang")
                .expect("waiter task panicked");
            assert!(observed.is_some(), "waiter must observe the completion");
        }
    }

    #[tokio::test]
    async fn abort_resolves_waiters_to_none_without_hanging() {
        let flight = Arc::new(Flight::new());
        let waiter = {
            let flight = Arc::clone(&flight);
            tokio::spawn(async move { flight.wait().await })
        };
        tokio::task::yield_now().await;
        flight.abort();
        let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must not hang")
            .expect("waiter task panicked");
        assert!(observed.is_none(), "abort must resolve waiters to None");
    }
}
