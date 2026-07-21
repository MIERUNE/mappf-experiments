use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use std::time::Duration;

use tokio::time::Instant;

const WAITING: u8 = 0;
const PEER_SEEN: u8 = 1;
const GRACE_EXPIRED: u8 = 2;
const DISABLED: u8 = 3;

/// Default grace a seeded node waits to observe a peer before failing open.
///
/// Peer presence is a startup bootstrap check, not an ongoing quorum, so both
/// MMPF services share this fail-open deadline.
pub const DEFAULT_BOOTSTRAP_GRACE: Duration = Duration::from_secs(30);

/// State of a startup-only bootstrap readiness gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapReadinessState {
    /// Bootstrap gating was not enabled by the service.
    Disabled,
    /// The service is still waiting for its first peer observation or grace expiry.
    Waiting,
    /// A peer was observed and the gate permanently opened.
    PeerSeen,
    /// The grace period elapsed and the gate permanently failed open.
    GraceExpired,
}

impl BootstrapReadinessState {
    /// Returns whether this state permits the service to report ready.
    pub const fn is_ready(self) -> bool {
        !matches!(self, Self::Waiting)
    }
}

/// One-time transition that permanently opened a bootstrap readiness gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapReadinessTransition {
    PeerSeen,
    GraceExpired,
}

/// Result of observing the current bootstrap condition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BootstrapReadinessObservation {
    state: BootstrapReadinessState,
    transition: Option<BootstrapReadinessTransition>,
}

impl BootstrapReadinessObservation {
    /// Returns the gate's state after this observation.
    pub const fn state(self) -> BootstrapReadinessState {
        self.state
    }

    /// Returns the transition won by this caller, if it opened the gate.
    ///
    /// Concurrent callers observe the same terminal state, but only the caller
    /// that atomically performs the transition receives `Some`.
    pub const fn transition(self) -> Option<BootstrapReadinessTransition> {
        self.transition
    }

    /// Returns whether the gate permits the service to report ready.
    pub const fn is_ready(self) -> bool {
        self.state.is_ready()
    }
}

/// Cloneable, concurrency-safe startup-only bootstrap readiness gate.
///
/// A required gate begins in [`BootstrapReadinessState::Waiting`] and permanently
/// opens after either observing a peer or reaching its fail-open deadline. Once
/// open, later peer loss never closes it. A disabled gate is immediately open.
#[derive(Clone, Debug)]
pub struct BootstrapReadinessGate {
    state: Arc<AtomicU8>,
    deadline: Instant,
    grace: Duration,
}

impl BootstrapReadinessGate {
    /// Creates a gate whose deadline starts at this call.
    pub fn new(required: bool, grace: Duration) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(if required { WAITING } else { DISABLED })),
            deadline: Instant::now() + grace,
            grace,
        }
    }

    /// Returns the current latched state without evaluating the deadline.
    pub fn state(&self) -> BootstrapReadinessState {
        decode_state(self.state.load(Ordering::Acquire))
    }

    /// Observes peer presence and the Tokio clock, opening the gate if eligible.
    pub fn observe(&self, peer_seen: bool) -> BootstrapReadinessObservation {
        let current = self.state.load(Ordering::Acquire);
        if current != WAITING {
            return observation(decode_state(current), None);
        }

        let (next, transition) = if peer_seen {
            (PEER_SEEN, BootstrapReadinessTransition::PeerSeen)
        } else if Instant::now() >= self.deadline {
            (GRACE_EXPIRED, BootstrapReadinessTransition::GraceExpired)
        } else {
            return observation(BootstrapReadinessState::Waiting, None);
        };

        match self
            .state
            .compare_exchange(WAITING, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => observation(decode_state(next), Some(transition)),
            Err(actual) => observation(decode_state(actual), None),
        }
    }

    /// Observes the current bootstrap condition and logs any one-time transition
    /// that this caller wins, in a single call.
    ///
    /// The emitted log level, message, and structured fields are identical
    /// across MMPF services so bootstrap behavior reads the same everywhere.
    pub fn observe_with_logging(&self, peer_seen: bool) -> BootstrapReadinessObservation {
        let observed = self.observe(peer_seen);
        log_bootstrap_transition(observed.transition(), self.grace);
        observed
    }
}

/// Emits the shared bootstrap-transition log lines, preserving the exact
/// messages, levels, and `grace_seconds` field both services relied on.
fn log_bootstrap_transition(transition: Option<BootstrapReadinessTransition>, grace: Duration) {
    match transition {
        Some(BootstrapReadinessTransition::PeerSeen) => {
            tracing::info!("gossip bootstrap readiness opened after observing a peer");
        }
        Some(BootstrapReadinessTransition::GraceExpired) => {
            tracing::warn!(
                grace_seconds = grace.as_secs(),
                "gossip bootstrap grace expired without observing a peer; failing open"
            );
        }
        None => {}
    }
}

const fn observation(
    state: BootstrapReadinessState,
    transition: Option<BootstrapReadinessTransition>,
) -> BootstrapReadinessObservation {
    BootstrapReadinessObservation { state, transition }
}

fn decode_state(state: u8) -> BootstrapReadinessState {
    match state {
        WAITING => BootstrapReadinessState::Waiting,
        PEER_SEEN => BootstrapReadinessState::PeerSeen,
        GRACE_EXPIRED => BootstrapReadinessState::GraceExpired,
        DISABLED => BootstrapReadinessState::Disabled,
        _ => unreachable!("invalid bootstrap readiness state"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRACE: Duration = Duration::from_secs(30);

    #[test]
    fn disabled_gate_is_immediately_ready() {
        let gate = BootstrapReadinessGate::new(false, GRACE);

        let observed = gate.observe(false);
        assert_eq!(observed.state(), BootstrapReadinessState::Disabled);
        assert_eq!(observed.transition(), None);
        assert!(observed.is_ready());
    }

    #[test]
    fn required_gate_waits_before_peer_or_deadline() {
        let gate = BootstrapReadinessGate::new(true, GRACE);

        let observed = gate.observe(false);
        assert_eq!(observed.state(), BootstrapReadinessState::Waiting);
        assert_eq!(observed.transition(), None);
        assert!(!observed.is_ready());
    }

    #[test]
    fn peer_observation_latches_gate_open_across_clones() {
        let gate = BootstrapReadinessGate::new(true, GRACE);
        let clone = gate.clone();

        let observed = clone.observe(true);
        assert_eq!(observed.state(), BootstrapReadinessState::PeerSeen);
        assert_eq!(
            observed.transition(),
            Some(BootstrapReadinessTransition::PeerSeen)
        );
        assert!(observed.is_ready());

        let later = gate.observe(false);
        assert_eq!(later.state(), BootstrapReadinessState::PeerSeen);
        assert_eq!(later.transition(), None);
        assert!(later.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn grace_expiration_latches_gate_open() {
        let gate = BootstrapReadinessGate::new(true, GRACE);
        assert!(!gate.observe(false).is_ready());

        tokio::time::advance(GRACE).await;

        let observed = gate.observe(false);
        assert_eq!(observed.state(), BootstrapReadinessState::GraceExpired);
        assert_eq!(
            observed.transition(),
            Some(BootstrapReadinessTransition::GraceExpired)
        );
        assert!(observed.is_ready());
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_states_never_reclose_or_change_reason() {
        let peer_gate = BootstrapReadinessGate::new(true, GRACE);
        assert!(peer_gate.observe(true).is_ready());
        tokio::time::advance(GRACE).await;
        assert_eq!(
            peer_gate.observe(false).state(),
            BootstrapReadinessState::PeerSeen
        );

        let expired_gate = BootstrapReadinessGate::new(true, GRACE);
        tokio::time::advance(GRACE).await;
        assert!(expired_gate.observe(false).is_ready());
        assert_eq!(
            expired_gate.observe(true).state(),
            BootstrapReadinessState::GraceExpired
        );
    }
}
