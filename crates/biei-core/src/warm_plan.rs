//! Predictive warm handoff: decide which profile, if any, this node should
//! pre-warm while idle.
//!
//! A cold render is dominated by work that a previous render would have made
//! cheap — shader compilation under llvmpipe, style parsing, glyph download and
//! atlas construction. Under HRW routing a node can compute which profiles it
//! will own, so it can pay that cost before a user asks instead of during their
//! request.
//!
//! # Candidate source
//!
//! An idle node pulls a bounded recommendation list from other live nodes. A
//! recommendation is deliberately only a working-set hint: the receiver resolves
//! the current style revision and recomputes HRW ownership locally before it may
//! spend work. A peer therefore cannot command another node to warm. The likely
//! successor learns what the primary is serving while both are healthy, then may
//! prepare the same profile when it has spare capacity.
//!
//! # Trust
//!
//! Gossip and internal forwarding share one cluster boundary that is trusted by
//! topology, not cryptographically authenticated. The bounds here contain bugs
//! and unexpected cardinality; they do not make a compromised internal peer
//! harmless, and such a peer can already submit ordinary render work over the
//! internal forwarding interface. Deployments that do not trust that network need
//! an authenticated peer transport such as mTLS.
//!
//! Version one is limited to anonymous-access styles: [`WorkerProfile`] omits the
//! credential cache partition that renderer warmth actually depends on, and
//! credentials must not enter gossip.
//!
//! This module only chooses a candidate. A future executor must use a dedicated
//! warm command that does not populate the output cache or enter foreground
//! latency metrics. It must preserve foreground headroom in both the execution
//! and native-render permit pools, and stop warming after a timeout or provider
//! failure rather than amplify an outage. A successfully warmed profile is then
//! published normally; that is how another likely owner learns it early.

use std::collections::HashSet;

use crate::{
    hrw::hrw_weight,
    types::{ClusterView, NodeId, StyleId, WorkerProfile, WorkerView},
};

/// Largest peer working set this node will consider in one evaluation.
///
/// Under cardinality pressure the planner warms nothing rather than selecting a
/// subset whose contents depend on observation order.
pub const MAX_CANDIDATE_PROFILES: usize = 64;

/// How deep into the HRW ranking this node warms.
///
/// Rank 1 is the profile's primary owner. Rank 2 is the likely successor when
/// the primary drains or is preempted.
pub const WARM_HRW_DEPTH: usize = 2;

/// Why an evaluation declined to warm. Carried into metrics so that an
/// unexpectedly hot skip reason — `CandidateSetTooLarge` above all — is visible
/// rather than silent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarmSkip {
    /// This node is shutting down; warming would waste work and slow the drain.
    Draining,
    /// This node already told the cluster it is not taking new renders.
    NotAcceptingRenders,
    /// This node already has a warm operation in flight.
    WarmInFlight,
    /// Real work is queued. Checked at planning time; executors must check again
    /// before admission. A native render is not cancellable once started, so an
    /// executor must retain foreground execution and native-render permits.
    QueuesBusy,
    /// No worker slot is free beyond those held back for real traffic. Warming
    /// must never evict a loaded profile: displacing a hot singleton to warm a
    /// prediction is a straight downgrade.
    NoSpareSlot,
    /// The peer working set exceeded [`MAX_CANDIDATE_PROFILES`].
    CandidateSetTooLarge,
    /// Nothing left after removing profiles already loaded here, styles the
    /// catalog cannot resolve, and profiles this node does not own.
    NoEligibleCandidate,
}

/// Local conditions required before any warming is considered.
#[derive(Clone, Copy, Debug)]
pub struct WarmReadiness {
    pub draining: bool,
    pub accepts_new_renders: bool,
    pub warm_in_flight: bool,
    /// Slots kept free for real traffic. Warming may only use capacity beyond
    /// this reserve.
    pub reserved_slots: usize,
}

/// Chooses at most one profile to warm.
///
/// `peer_recommendations` is a bounded union of profile hints pulled from live
/// peers. Only `(style, mode, scale)` is trusted; the latest local revision is
/// resolved when planning, so advice made before a style refresh can still
/// prepare the revision that a new foreground request will use.
///
/// `resolve_latest` returns the revision the local catalog currently resolves for
/// a style id, i.e. `StyleCatalog::resolve_latest`. The planner constructs the
/// candidate from that version instead of trusting the peer's possibly stale
/// revision. Taking a resolver rather than a predicate also makes accidentally
/// substituting the permissive `StyleCatalog::accepts_revision` a type error.
///
/// This is not an authorization boundary: a catch-all style template can resolve a
/// previously unseen bootstrap revision.
///
/// An executor must re-resolve the revision immediately before native admission
/// *and* after the render completes. A refresh landing during a non-cancellable
/// warm would otherwise publish obsolete warmth; a bootstrap revision superseded
/// by its first content observation is the most direct example.
pub fn plan_warm<F>(
    local: &NodeId,
    cluster: &ClusterView,
    peer_recommendations: &[WorkerProfile],
    local_workers: &[WorkerView],
    readiness: WarmReadiness,
    resolve_latest: F,
) -> Result<WorkerProfile, WarmSkip>
where
    F: Fn(&StyleId) -> Option<u64>,
{
    if readiness.draining {
        return Err(WarmSkip::Draining);
    }
    if !readiness.accepts_new_renders {
        return Err(WarmSkip::NotAcceptingRenders);
    }
    if readiness.warm_in_flight {
        return Err(WarmSkip::WarmInFlight);
    }
    if local_workers.iter().any(|worker| worker.queue_depth > 0) {
        return Err(WarmSkip::QueuesBusy);
    }

    let free_slots = local_workers
        .iter()
        .filter(|worker| worker.loaded_profile.is_none())
        .count();
    if free_slots <= readiness.reserved_slots {
        return Err(WarmSkip::NoSpareSlot);
    }

    let already_loaded: HashSet<&WorkerProfile> = local_workers
        .iter()
        .filter_map(|worker| worker.loaded_profile.as_ref())
        .collect();

    let mut candidate_keys = Vec::new();
    let mut seen = HashSet::new();
    for profile in peer_recommendations {
        let key = (profile.style.id.clone(), profile.render_mode, profile.scale);
        if seen.insert(key.clone()) {
            candidate_keys.push(key);
        }
    }

    // Fail closed on raw cardinality, before any filtering, so an inflated view
    // cannot be narrowed into an observation-order-dependent warming target.
    if candidate_keys.len() > MAX_CANDIDATE_PROFILES {
        return Err(WarmSkip::CandidateSetTooLarge);
    }

    // Predict steady-state HRW ownership over nodes that accept new renders.
    // Unlike Tier 2 dispatch, this deliberately ignores transient queue capacity:
    // a momentarily busy primary is still the profile's likely future owner.
    // Draining nodes are absent, so their profiles resolve to a successor. The
    // local node is always a member of the set because it passed the gates above.
    let mut owners: Vec<&NodeId> = cluster
        .members
        .iter()
        .filter(|node_id| {
            node_id == &local
                || cluster
                    .states
                    .get(*node_id)
                    .is_some_and(|state| state.accepts_new_renders)
        })
        .collect();
    if !owners.contains(&local) {
        owners.push(local);
    }

    let mut best: Option<(usize, u64, WorkerProfile)> = None;
    for (style_id, render_mode, scale) in candidate_keys {
        let Some(version) = resolve_latest(&style_id) else {
            continue;
        };
        let profile = WorkerProfile {
            style: crate::types::StyleRevision {
                id: style_id,
                version,
            },
            render_mode,
            scale,
        };
        if already_loaded.contains(&profile) {
            continue;
        }
        let local_weight = hrw_weight(&profile, local);
        // Equal weights retain membership order, matching the dispatcher's
        // stable insertion order rather than inventing a second tie-breaker.
        let local_position = owners
            .iter()
            .position(|node| *node == local)
            .expect("the local node is always included in owners");
        let rank = 1 + owners
            .iter()
            .enumerate()
            .filter(|(position, node)| {
                let weight = hrw_weight(&profile, node);
                weight > local_weight || (weight == local_weight && *position < local_position)
            })
            .count();
        if rank > WARM_HRW_DEPTH {
            continue;
        }
        // Prefer primary ownership, then the strongest claim, then a stable key
        // so every node reaches the same decision from the same view.
        let key = (rank, u64::MAX - local_weight, profile);
        let better = match &best {
            None => true,
            Some((best_rank, best_inv_weight, best_profile)) => {
                (key.0, key.1, sort_key(&key.2))
                    < (*best_rank, *best_inv_weight, sort_key(best_profile))
            }
        };
        if better {
            best = Some(key);
        }
    }

    best.map(|(_, _, profile)| profile)
        .ok_or(WarmSkip::NoEligibleCandidate)
}

/// Deterministic total order over profiles, which do not implement `Ord`.
fn sort_key(profile: &WorkerProfile) -> (String, u8, u8) {
    (
        profile.style.to_gossip_value(),
        profile.render_mode as u8,
        profile.scale as u8,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NodeStateView, RenderMode, Scale, StyleId, StyleRevision, WorkerId};
    use std::collections::HashMap;
    use tokio::time::Instant;

    fn profile(style: &str) -> WorkerProfile {
        WorkerProfile {
            style: StyleRevision {
                id: StyleId(style.to_string()),
                version: 1,
            },
            render_mode: RenderMode::Static,
            scale: Scale::X1,
        }
    }

    fn worker(id: WorkerId, loaded: Option<WorkerProfile>, queue_depth: usize) -> WorkerView {
        WorkerView {
            id,
            loaded_profile: loaded,
            queue_depth,
        }
    }

    fn node(id: &str, accepts: bool, workers: Vec<WorkerView>) -> NodeStateView {
        NodeStateView {
            id: NodeId::new(id),
            accepts_new_renders: accepts,
            workers,
        }
    }

    fn cluster(states: Vec<NodeStateView>) -> ClusterView {
        ClusterView {
            members: states.iter().map(|state| state.id.clone()).collect(),
            states: states
                .into_iter()
                .map(|state| (state.id.clone(), state))
                .collect::<HashMap<_, _>>(),
            generated_at: Instant::now(),
        }
    }

    /// Presents the supplied profiles as worker state on one live peer, then
    /// calls the production planner. Keeping this adapter in tests makes the
    /// candidate source explicit while retaining compact table-style cases.
    fn plan_warm<F>(
        local: &NodeId,
        cluster: &ClusterView,
        peer_profiles: &[WorkerProfile],
        local_workers: &[WorkerView],
        readiness: WarmReadiness,
        resolve_latest: F,
    ) -> Result<WorkerProfile, WarmSkip>
    where
        F: Fn(&StyleId) -> Option<u64>,
    {
        super::plan_warm(
            local,
            cluster,
            peer_profiles,
            local_workers,
            readiness,
            resolve_latest,
        )
    }

    /// Resolver for tests whose profiles all carry version 1, so every candidate
    /// is the latest revision of its style. Tests about obsolete revisions supply
    /// their own resolver instead.
    fn always_latest(_: &StyleId) -> Option<u64> {
        Some(1)
    }

    fn ready() -> WarmReadiness {
        WarmReadiness {
            draining: false,
            accepts_new_renders: true,
            warm_in_flight: false,
            reserved_slots: 1,
        }
    }

    /// A profile advertised by another live node is enough to seed its likely
    /// successor; no separate demand-history protocol is required.
    #[test]
    fn warms_a_profile_loaded_by_a_live_peer() {
        let held = profile("carto/voyager");
        let local = NodeId::new("a");
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let view = cluster(vec![node("a", true, local_workers.clone())]);

        let planned = plan_warm(
            &local,
            &view,
            std::slice::from_ref(&held),
            &local_workers,
            ready(),
            always_latest,
        )
        .expect("a likely successor should warm the peer's loaded profile");
        assert_eq!(planned, held);
    }

    /// Peer state may lag a refresh. The peer supplies identity/mode/scale while
    /// the local catalog supplies the exact revision to prepare.
    #[test]
    fn resolves_a_peer_profile_to_the_latest_local_revision() {
        let style = "carto/voyager";
        let revision = |version: u64| WorkerProfile {
            style: StyleRevision {
                id: StyleId(style.to_string()),
                version,
            },
            render_mode: RenderMode::Static,
            scale: Scale::X1,
        };
        let obsolete = revision(100);
        let latest = revision(99);
        let local = NodeId::new("a");
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let view = cluster(vec![node("a", true, local_workers.clone())]);
        let resolves_latest = |_: &StyleId| Some(99);

        let planned = plan_warm(
            &local,
            &view,
            std::slice::from_ref(&obsolete),
            &local_workers,
            ready(),
            resolves_latest,
        )
        .expect("the latest revision is warmable");
        assert_eq!(planned, latest);
    }

    /// Demand for a revision the local catalog no longer accepts must not be
    /// warmed.
    #[test]
    fn declines_when_the_style_is_absent_from_the_local_catalog() {
        let stale = profile("carto/old-revision");
        let local = NodeId::new("a");
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let view = cluster(vec![node("a", true, local_workers.clone())]);

        // Sanity: it would otherwise be warmed, so the catalog gate is load-bearing.
        assert!(
            plan_warm(
                &local,
                &view,
                std::slice::from_ref(&stale),
                &local_workers,
                ready(),
                always_latest,
            )
            .is_ok()
        );

        assert_eq!(
            plan_warm(
                &local,
                &view,
                std::slice::from_ref(&stale),
                &local_workers,
                ready(),
                |_: &StyleId| None,
            ),
            Err(WarmSkip::NoEligibleCandidate)
        );
    }

    /// Cardinality pressure must fail closed, not degrade into an arbitrary pick.
    #[test]
    fn fails_closed_when_the_demand_set_is_too_large() {
        let local = NodeId::new("a");
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let flood: Vec<WorkerProfile> = (0..=MAX_CANDIDATE_PROFILES)
            .map(|i| profile(&format!("style/{i}")))
            .collect();
        assert!(flood.len() > MAX_CANDIDATE_PROFILES);
        let view = cluster(vec![node("a", true, local_workers.clone())]);

        assert_eq!(
            plan_warm(
                &local,
                &view,
                &flood,
                &local_workers,
                ready(),
                always_latest
            ),
            Err(WarmSkip::CandidateSetTooLarge)
        );
    }

    #[test]
    fn declines_while_real_work_is_queued() {
        let held = profile("carto/voyager");
        let local = NodeId::new("a");
        let busy = vec![worker(0, None, 1), worker(1, None, 0)];
        let view = cluster(vec![node("a", true, busy.clone())]);

        assert_eq!(
            plan_warm(
                &local,
                &view,
                std::slice::from_ref(&held),
                &busy,
                ready(),
                always_latest,
            ),
            Err(WarmSkip::QueuesBusy)
        );
    }

    /// Warming may only consume capacity beyond the reserve, and never evicts.
    #[test]
    fn declines_without_a_slot_beyond_the_reserve() {
        let held = profile("carto/voyager");
        let other = profile("carto/positron");
        let local = NodeId::new("a");
        // One free slot, one reserved -> nothing spare.
        let tight = vec![worker(0, Some(other), 0), worker(1, None, 0)];
        let view = cluster(vec![node("a", true, tight.clone())]);

        assert_eq!(
            plan_warm(
                &local,
                &view,
                std::slice::from_ref(&held),
                &tight,
                ready(),
                always_latest,
            ),
            Err(WarmSkip::NoSpareSlot)
        );
    }

    #[test]
    fn declines_when_draining_or_not_accepting_or_already_warming() {
        let held = profile("carto/voyager");
        let local = NodeId::new("a");
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let view = cluster(vec![node("a", true, local_workers.clone())]);

        for (readiness, expected) in [
            (
                WarmReadiness {
                    draining: true,
                    ..ready()
                },
                WarmSkip::Draining,
            ),
            (
                WarmReadiness {
                    accepts_new_renders: false,
                    ..ready()
                },
                WarmSkip::NotAcceptingRenders,
            ),
            (
                WarmReadiness {
                    warm_in_flight: true,
                    ..ready()
                },
                WarmSkip::WarmInFlight,
            ),
        ] {
            assert_eq!(
                plan_warm(
                    &local,
                    &view,
                    std::slice::from_ref(&held),
                    &local_workers,
                    readiness,
                    always_latest,
                ),
                Err(expected)
            );
        }
    }

    #[test]
    fn never_rewarms_a_profile_already_loaded_here() {
        let held = profile("carto/voyager");
        let local = NodeId::new("a");
        let local_workers = vec![
            worker(0, Some(held.clone()), 0),
            worker(1, None, 0),
            worker(2, None, 0),
        ];
        let view = cluster(vec![node("a", true, local_workers.clone())]);

        assert_eq!(
            plan_warm(
                &local,
                &view,
                std::slice::from_ref(&held),
                &local_workers,
                ready(),
                always_latest,
            ),
            Err(WarmSkip::NoEligibleCandidate)
        );
    }

    /// Only the top `WARM_HRW_DEPTH` owners warm, so a large cluster does not
    /// have every node render every profile.
    #[test]
    fn only_the_top_ranked_owners_warm_a_profile() {
        let held = profile("carto/voyager");
        let names = ["a", "b", "c", "d", "e", "f", "g"];
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let states: Vec<NodeStateView> = names
            .iter()
            .map(|name| node(name, true, local_workers.clone()))
            .collect();
        let view = cluster(states);

        let warming: Vec<&str> = names
            .iter()
            .filter(|name| {
                plan_warm(
                    &NodeId::new(**name),
                    &view,
                    std::slice::from_ref(&held),
                    &local_workers,
                    ready(),
                    always_latest,
                )
                .is_ok()
            })
            .copied()
            .collect();

        assert_eq!(
            warming.len(),
            WARM_HRW_DEPTH,
            "expected exactly the top {WARM_HRW_DEPTH} owners to warm, got {warming:?}"
        );
    }

    /// Same view in, same decision out: nodes must not oscillate.
    #[test]
    fn decision_is_deterministic_across_repeated_evaluations() {
        let local = NodeId::new("a");
        let local_workers = vec![worker(0, None, 0), worker(1, None, 0)];
        let held: Vec<WorkerProfile> = (0..5)
            .map(|i| profile(&format!("carto/style-{i}")))
            .collect();
        let view = cluster(vec![node("a", true, local_workers.clone())]);

        let first = plan_warm(&local, &view, &held, &local_workers, ready(), always_latest);
        for _ in 0..10 {
            assert_eq!(
                plan_warm(&local, &view, &held, &local_workers, ready(), always_latest),
                first
            );
        }
    }
}
