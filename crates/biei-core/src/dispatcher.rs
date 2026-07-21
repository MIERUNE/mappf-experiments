//! Tier 1/2/3 routing decision + drain-ETA SLA check.

use std::cmp::Reverse;
use std::collections::HashMap;
use std::time::Duration;

use mmpf_common::rng::splitmix64_finalize;
use rand::{RngExt, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;
use tokio::time::Instant;

use crate::config::{CostConfig, RoutingConfig, Tier1Strategy};
use crate::hrw::hrw_weight;
use crate::types::{
    ClusterView, Decision, ForwardCandidate, InternalTask, NodeId, NodeStateView, RejectionReason,
    RouteTier, WorkerProfile, WorkerView,
};

const DEFAULT_FORWARD_CANDIDATES: usize = 2;

type Tier3SortKey = (bool, bool, Reverse<usize>);

struct WarmCandidate<'a> {
    node: &'a NodeStateView,
    warm_count: u32,
    style_spare_bl: usize,
}

/// Dispatcher-facing cluster state with an optional authoritative local entry.
/// Remote state remains borrowed directly from the gossip-derived view.
struct DispatchView<'a> {
    cluster: &'a ClusterView,
    local: Option<NodeStateView>,
}

impl<'a> DispatchView<'a> {
    fn new(cluster: &'a ClusterView, local: Option<NodeStateView>) -> Self {
        let local = local.filter(|state| cluster.members.contains(&state.id));
        Self { cluster, local }
    }

    fn members(&self) -> &[NodeId] {
        &self.cluster.members
    }

    fn state(&self, id: &NodeId) -> Option<&NodeStateView> {
        self.local
            .as_ref()
            .filter(|local| &local.id == id)
            .or_else(|| self.cluster.states.get(id))
    }

    fn states(&self) -> impl Iterator<Item = &NodeStateView> {
        self.cluster
            .states
            .iter()
            .filter(move |(id, _)| self.local.as_ref().is_none_or(|local| &local.id != *id))
            .map(|(_, state)| state)
            .chain(self.local.iter())
    }

    fn state_entries(&self) -> impl Iterator<Item = (&NodeId, &NodeStateView)> {
        self.cluster
            .states
            .iter()
            .filter(move |(id, _)| self.local.as_ref().is_none_or(|local| &local.id != *id))
            .chain(self.local.iter().map(|state| (&state.id, state)))
    }
}

pub(crate) struct Dispatcher {
    pub node_id: NodeId,
    pub config: RoutingConfig,
    pub costs: CostConfig,
    pub bl_capacity: usize,
    pub queue_capacity: usize,
    seed: u64,
}

pub(crate) struct DispatcherSpawn {
    pub node_id: NodeId,
    pub config: RoutingConfig,
    pub costs: CostConfig,
    pub bl_capacity: usize,
    pub queue_capacity: usize,
    pub resolved_seed: u64,
}

impl Dispatcher {
    pub(crate) fn new(spec: DispatcherSpawn) -> Self {
        let DispatcherSpawn {
            node_id,
            config,
            costs,
            bl_capacity,
            queue_capacity,
            resolved_seed,
        } = spec;

        Self {
            node_id,
            config,
            costs,
            bl_capacity,
            queue_capacity: queue_capacity.max(bl_capacity),
            seed: resolved_seed,
        }
    }

    /// Estimate drain ETA at this worker for a task with given target profile.
    /// Pessimistic: assumes each queued task ahead pays a profile swap + render.
    fn estimate_drain_eta(&self, w: &WorkerView, task_profile: &WorkerProfile) -> Duration {
        let warm_p = self.costs.warm_render_cost().mid();
        let first_p = self.costs.first_render_cost().mid();
        let s = self.costs.style_setup_cost.mid();
        let per_ahead = first_p + s;
        let queue_wait = per_ahead
            .checked_mul(w.queue_depth as u32)
            .unwrap_or(Duration::MAX);
        let own = if w.loaded_profile.as_ref() == Some(task_profile) {
            warm_p
        } else {
            s + first_p
        };
        queue_wait.saturating_add(own)
    }

    /// Warm renderers for `profile` across the propagated cluster view. The four
    /// tier-1 selection strategies iterate this identically, so the filter lives
    /// in one place — the sampling indices rely on all passes seeing the same set.
    fn warm_candidates<'a>(
        &self,
        view: &'a DispatchView<'_>,
        profile: &'a WorkerProfile,
    ) -> impl Iterator<Item = WarmCandidate<'a>> {
        let bl_capacity = self.bl_capacity;
        view.states()
            .filter_map(move |node| warm_candidate(node, profile, bl_capacity))
    }

    fn select_warm_target(
        &self,
        task: &InternalTask,
        view: &DispatchView<'_>,
        profile: &WorkerProfile,
    ) -> Option<NodeId> {
        let mut rng = Xoshiro256PlusPlus::seed_from_u64(routing_seed(self.seed, task.id));
        match self.config.tier1_strategy {
            Tier1Strategy::WeightedRandom => {
                let total_weight: u64 = self
                    .warm_candidates(view, profile)
                    .map(|candidate| u64::from(candidate.warm_count))
                    .sum();
                if total_weight == 0 {
                    return None;
                }

                let mut ticket = rng.random_range(0..total_weight);
                for candidate in self.warm_candidates(view, profile) {
                    let weight = u64::from(candidate.warm_count);
                    if ticket < weight {
                        return Some(candidate.node.id.clone());
                    }
                    ticket -= weight;
                }
                unreachable!("positive warm weight must select a node")
            }
            Tier1Strategy::PowerOfTwo => {
                let candidate_count = self.warm_candidates(view, profile).count();
                if candidate_count == 0 {
                    return None;
                }
                if candidate_count == 1 {
                    return self
                        .warm_candidates(view, profile)
                        .next()
                        .map(|candidate| candidate.node.id.clone());
                }

                let first_idx = rng.random_range(0..candidate_count);
                let mut second_idx = rng.random_range(0..candidate_count - 1);
                if second_idx >= first_idx {
                    second_idx += 1;
                }
                let mut first = None;
                let mut second = None;
                for (idx, candidate) in self.warm_candidates(view, profile).enumerate() {
                    if idx == first_idx {
                        first = Some(candidate);
                    } else if idx == second_idx {
                        second = Some(candidate);
                    }
                }
                let first = first.expect("sampled warm candidate exists");
                let second = second.expect("sampled warm candidate exists");
                Some(
                    if first.style_spare_bl >= second.style_spare_bl {
                        first.node
                    } else {
                        second.node
                    }
                    .id
                    .clone(),
                )
            }
        }
    }

    pub(crate) fn decide_with_local_state(
        &self,
        task: &InternalTask,
        cluster: &ClusterView,
        local: NodeStateView,
    ) -> Decision {
        self.decide_view(task, &DispatchView::new(cluster, Some(local)))
    }

    #[cfg(test)]
    fn decide(&self, task: &InternalTask, cluster: &ClusterView) -> Decision {
        self.decide_view(task, &DispatchView::new(cluster, None))
    }

    fn decide_view(&self, task: &InternalTask, view: &DispatchView<'_>) -> Decision {
        let profile = task.worker_profile();

        // ---- Tier 1: warm tracking (over propagated states only) ----
        if let Some(target_id) = self.select_warm_target(task, view, &profile) {
            // Keep one HRW fallback behind the warm target. This is normally a
            // queue-race escape hatch; it is also what lets a node survive the
            // short gossip window where the selected warm renderer has already
            // stopped admission but still appears healthy in this snapshot.
            let mut candidates = vec![ForwardCandidate {
                node_id: target_id.clone(),
                drain_worker: None,
            }];
            candidates.extend(
                top_hrw_candidates(view.members(), &profile, DEFAULT_FORWARD_CANDIDATES, |id| {
                    id != &target_id
                        && view
                            .state(id)
                            .map(|node| node.has_capacity(self.bl_capacity))
                            .unwrap_or(false)
                })
                .into_iter()
                .take(DEFAULT_FORWARD_CANDIDATES.saturating_sub(1)),
            );
            return self.materialize_candidates(task, RouteTier::Tier1WarmTracking, candidates);
        }

        // ---- Tier 2: HRW over members with complete current state ----
        // HRW input is stable style id + render mode + scale (not the style
        // revision version) so version bumps do not reshuffle routing. Tier 4
        // reuses the same ordering.
        let tier2_candidates =
            top_hrw_candidates(view.members(), &profile, DEFAULT_FORWARD_CANDIDATES, |id| {
                view.state(id)
                    .map(|n| n.has_capacity(self.bl_capacity))
                    .unwrap_or(false)
            });
        if !tier2_candidates.is_empty() {
            return self.materialize_candidates(task, RouteTier::Tier2HrwBl, tier2_candidates);
        }

        // ---- Tier 3: drain-and-swap ----
        if self.config.tier3_enabled {
            let tier3_candidates =
                self.tier3_candidates_view(view, task, DEFAULT_FORWARD_CANDIDATES);
            if !tier3_candidates.is_empty() {
                return self.materialize_candidates(
                    task,
                    RouteTier::Tier3DrainSwap,
                    tier3_candidates,
                );
            }
        }

        // ---- Tier 4: overflow queue admission ----
        let tier4_candidates =
            top_hrw_candidates(view.members(), &profile, DEFAULT_FORWARD_CANDIDATES, |id| {
                view.state(id)
                    .map(|n| n.has_admission_capacity(self.queue_capacity))
                    .unwrap_or(false)
            });
        if !tier4_candidates.is_empty() {
            return self.materialize_candidates(task, RouteTier::Tier4Overflow, tier4_candidates);
        }

        Decision::Reject {
            reason: RejectionReason::NoCapacity,
        }
    }

    #[cfg(test)]
    fn tier3_candidates(
        &self,
        cluster: &ClusterView,
        task: &InternalTask,
        limit: usize,
    ) -> Vec<ForwardCandidate> {
        self.tier3_candidates_view(&DispatchView::new(cluster, None), task, limit)
    }

    fn tier3_candidates_view(
        &self,
        view: &DispatchView<'_>,
        task: &InternalTask,
        limit: usize,
    ) -> Vec<ForwardCandidate> {
        let now = Instant::now();
        let drain_max = self.config.drain_max_queue;
        let sla_deadline = task.arrived_at.checked_add(self.costs.sla);

        let task_profile = task.worker_profile();
        // Count by WorkerProfile (style revision + render mode + scale), so
        // Static/Tile and @1x/@2x allocations stay independent.
        let mut cluster_counts: HashMap<WorkerProfile, usize> = HashMap::new();
        let mut candidates: Vec<(NodeId, &WorkerView)> = Vec::new();
        for (nid, node) in view.state_entries() {
            if !node.accepts_new_renders {
                continue;
            }
            for worker in &node.workers {
                if let Some(profile) = &worker.loaded_profile {
                    *cluster_counts.entry(profile.clone()).or_insert(0) += 1;
                }
                let is_candidate = worker.queue_depth < drain_max
                    && match &worker.loaded_profile {
                        None => true,
                        // Tier 3 is an eviction path. Do not pick an already
                        // warm worker for the incoming profile.
                        Some(profile) if profile == &task_profile => false,
                        Some(_) => true,
                    };
                if is_candidate {
                    candidates.push((nid.clone(), worker));
                }
            }
        }

        if candidates.is_empty() {
            return Vec::new();
        }

        // Prefer over-allocated profiles first. Within that group, reuse the
        // same renderer shape (mode + scale), then use queue depth and stable
        // identity so the worker used for the SLA estimate is the one hinted
        // to the owner.
        candidates.sort_by(|(a_nid, a), (b_nid, b)| {
            tier3_sort_key(a.loaded_profile.as_ref(), &task_profile, &cluster_counts)
                .cmp(&tier3_sort_key(
                    b.loaded_profile.as_ref(),
                    &task_profile,
                    &cluster_counts,
                ))
                // Break ties only on cluster-visible, deterministic state so every
                // dispatcher with the same `ClusterView` picks the same eviction
                // target regardless of local request history: least-queued worker
                // first, then a stable (node, worker) identity order.
                .then_with(|| a.queue_depth.cmp(&b.queue_depth))
                .then_with(|| a_nid.cmp(b_nid))
                .then_with(|| a.id.cmp(&b.id))
        });

        let mut selected = Vec::new();
        for (nid, w) in &candidates {
            let eta = self.estimate_drain_eta(w, &task_profile);
            if sla_deadline.is_none_or(|deadline| {
                now.checked_add(eta)
                    .is_some_and(|candidate_deadline| candidate_deadline <= deadline)
            }) {
                selected.push(ForwardCandidate {
                    node_id: nid.clone(),
                    drain_worker: Some(w.id),
                });
                if selected.len() >= limit {
                    break;
                }
            }
        }
        selected
    }

    fn materialize_candidates(
        &self,
        task: &InternalTask,
        tier: RouteTier,
        candidates: Vec<ForwardCandidate>,
    ) -> Decision {
        let Some(first) = candidates.first() else {
            return Decision::Reject {
                reason: RejectionReason::NoCapacity,
            };
        };
        let first_node_id = first.node_id.clone();
        let first_worker = first.drain_worker;

        if first_node_id == self.node_id {
            let fallback_candidates = if task.forwarding_hops >= 1 {
                Vec::new()
            } else {
                candidates
                    .into_iter()
                    .skip(1)
                    .filter(|candidate| candidate.node_id != self.node_id)
                    .collect()
            };
            Decision::Local {
                route_tier: tier,
                worker_hint: first_worker,
                fallback_candidates,
            }
        } else if task.forwarding_hops >= 1 {
            // Chained forwarding banned — process locally; cannot honor the
            // cluster-wide worker hint here.
            Decision::Local {
                route_tier: tier,
                worker_hint: None,
                fallback_candidates: Vec::new(),
            }
        } else {
            let candidates = candidates
                .into_iter()
                .filter(|candidate| candidate.node_id != self.node_id)
                .collect();
            Decision::Forward {
                route_tier: tier,
                candidates,
            }
        }
    }
}

fn routing_seed(seed: u64, task_id: u64) -> u64 {
    let value = seed ^ task_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    splitmix64_finalize(value)
}

fn warm_candidate<'a>(
    node: &'a NodeStateView,
    profile: &WorkerProfile,
    bl: usize,
) -> Option<WarmCandidate<'a>> {
    if !node.accepts_new_renders {
        return None;
    }
    let mut warm_count = 0;
    let mut style_spare_bl = 0;
    let mut has_usable_capacity = false;

    for worker in &node.workers {
        if worker.loaded_profile.as_ref() == Some(profile) {
            warm_count += 1;
            style_spare_bl += bl.saturating_sub(worker.queue_depth);
            has_usable_capacity |= worker.queue_depth < bl;
        } else if worker.loaded_profile.is_none() {
            // A fresh worker can expand an already-warm profile without
            // evicting unrelated renderer state.
            has_usable_capacity = true;
        }
    }

    (warm_count > 0 && has_usable_capacity).then_some(WarmCandidate {
        node,
        warm_count,
        style_spare_bl,
    })
}

fn top_hrw_candidates<F>(
    members: &[NodeId],
    profile: &WorkerProfile,
    limit: usize,
    mut eligible: F,
) -> Vec<ForwardCandidate>
where
    F: FnMut(&NodeId) -> bool,
{
    let mut top: Vec<(u64, &NodeId)> = Vec::with_capacity(limit);

    for node_id in members.iter().filter(|node_id| eligible(node_id)) {
        let weight = hrw_weight(profile, node_id);
        // Equal weights retain member order, matching stable `sort_by_key`.
        let insert_at = top
            .iter()
            .position(|(existing, _)| weight > *existing)
            .unwrap_or(top.len());
        if insert_at < limit {
            top.insert(insert_at, (weight, node_id));
            top.truncate(limit);
        }
    }

    top.into_iter()
        .map(|(_, node_id)| ForwardCandidate {
            node_id: node_id.clone(),
            drain_worker: None,
        })
        .collect()
}

/// Tier-3 eviction ranking key derived only from cluster-visible state (so it is
/// identical on every dispatcher observing the same `ClusterView`). Lower sorts
/// first: protect singleton profiles, prefer a matching renderer shape, then
/// favor more over-allocated profiles. Ties are broken by the caller using
/// queue depth and stable node/worker identity.
fn tier3_sort_key(
    loaded_profile: Option<&WorkerProfile>,
    task_profile: &WorkerProfile,
    cluster_counts: &HashMap<WorkerProfile, usize>,
) -> Tier3SortKey {
    let Some(profile) = loaded_profile else {
        // Fresh workers have no profile to protect.
        return (false, true, Reverse(usize::MAX));
    };
    let count = cluster_counts.get(profile).copied().unwrap_or(0);
    let shape_mismatch =
        profile.render_mode != task_profile.render_mode || profile.scale != task_profile.scale;
    (count <= 1, shape_mismatch, Reverse(count))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::{CostRange, RoutingConfig, Tier1Strategy};
    use crate::types::{
        ClusterView, ImageFormat, NodeStateView, PixelRatio, RenderMode, RenderRequest, Scale,
        StyleId, StyleRevision, WorkerProfile, WorkerView,
    };

    use super::*;

    fn rev(id: u32) -> StyleRevision {
        StyleRevision {
            id: StyleId(format!("style-{}", id)),
            version: 0,
        }
    }

    fn profile(id: u32) -> WorkerProfile {
        profile_with(id, RenderMode::Tile, Scale::X1)
    }

    fn profile_with(id: u32, render_mode: RenderMode, scale: Scale) -> WorkerProfile {
        WorkerProfile {
            style: rev(id),
            render_mode,
            scale,
        }
    }

    fn dispatcher() -> Dispatcher {
        dispatcher_with_caps(1, 1)
    }

    fn dispatcher_with_caps(bl_capacity: usize, queue_capacity: usize) -> Dispatcher {
        Dispatcher::new(DispatcherSpawn {
            node_id: NodeId::from_index(0),
            config: RoutingConfig {
                tier1_strategy: Tier1Strategy::WeightedRandom,
                tier3_enabled: true,
                drain_max_queue: 10,
            },
            costs: CostConfig {
                style_setup_cost: CostRange::fixed(Duration::from_millis(100)),
                source_load_cost: CostRange::fixed(Duration::ZERO),
                render_cpu_cost: CostRange::fixed(Duration::from_millis(10)),
                render_resource_cost: CostRange::fixed(Duration::ZERO),
                first_render_resource_cost: CostRange::fixed(Duration::ZERO),
                hop_latency: Duration::ZERO,
                sla: Duration::from_secs(10),
            },
            bl_capacity,
            queue_capacity,
            resolved_seed: 0,
        })
    }

    fn view(workers: Vec<WorkerView>) -> ClusterView {
        let node_id = NodeId::from_index(0);
        ClusterView {
            members: vec![node_id.clone()],
            states: [(
                node_id.clone(),
                NodeStateView {
                    id: node_id,
                    accepts_new_renders: true,
                    workers,
                },
            )]
            .into_iter()
            .collect(),
            generated_at: Instant::now(),
        }
    }

    #[test]
    fn dispatch_view_replaces_only_the_local_member_state() {
        let local_id = NodeId::from_index(0);
        let remote_id = NodeId::from_index(1);
        let cluster = ClusterView {
            members: vec![local_id.clone(), remote_id.clone()],
            states: [
                (
                    local_id.clone(),
                    NodeStateView {
                        id: local_id.clone(),
                        accepts_new_renders: false,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(1)),
                            queue_depth: 9,
                        }],
                    },
                ),
                (
                    remote_id.clone(),
                    NodeStateView {
                        id: remote_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 4,
                            loaded_profile: Some(profile(2)),
                            queue_depth: 3,
                        }],
                    },
                ),
            ]
            .into_iter()
            .collect(),
            generated_at: Instant::now(),
        };
        let dispatch = DispatchView::new(
            &cluster,
            Some(NodeStateView {
                id: local_id.clone(),
                accepts_new_renders: true,
                workers: vec![WorkerView {
                    id: 0,
                    loaded_profile: Some(profile(3)),
                    queue_depth: 1,
                }],
            }),
        );

        let local = dispatch.state(&local_id).expect("local state is present");
        assert!(local.accepts_new_renders);
        assert_eq!(local.workers[0].loaded_profile.as_ref(), Some(&profile(3)));
        assert_eq!(local.workers[0].queue_depth, 1);

        let remote = dispatch.state(&remote_id).expect("remote state is present");
        assert!(remote.accepts_new_renders);
        assert_eq!(remote.workers[0].id, 4);
        assert_eq!(remote.workers[0].loaded_profile.as_ref(), Some(&profile(2)));
        assert_eq!(remote.workers[0].queue_depth, 3);
        assert_eq!(
            dispatch.states().count(),
            2,
            "local state is not duplicated"
        );
    }

    #[test]
    fn dispatch_view_does_not_add_local_state_outside_membership() {
        let local_id = NodeId::from_index(0);
        let remote_id = NodeId::from_index(1);
        let cluster = ClusterView {
            members: vec![remote_id.clone()],
            states: [(
                remote_id.clone(),
                NodeStateView {
                    id: remote_id,
                    accepts_new_renders: true,
                    workers: Vec::new(),
                },
            )]
            .into_iter()
            .collect(),
            generated_at: Instant::now(),
        };
        let dispatch = DispatchView::new(
            &cluster,
            Some(NodeStateView {
                id: local_id.clone(),
                accepts_new_renders: true,
                workers: vec![WorkerView {
                    id: 0,
                    loaded_profile: None,
                    queue_depth: 0,
                }],
            }),
        );

        assert!(dispatch.state(&local_id).is_none());
        assert_eq!(dispatch.states().count(), 1);
    }

    #[test]
    fn authoritative_local_state_suppresses_stale_warm_routing() {
        let dispatcher = dispatcher_with_caps(1, 1);
        let local_id = NodeId::from_index(0);
        let remote_id = NodeId::from_index(1);
        let task = make_task(9, Instant::now());
        let cluster = ClusterView {
            members: vec![local_id.clone(), remote_id.clone()],
            states: [
                (
                    local_id.clone(),
                    NodeStateView {
                        id: local_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(task.worker_profile()),
                            queue_depth: 0,
                        }],
                    },
                ),
                (
                    remote_id.clone(),
                    NodeStateView {
                        id: remote_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: None,
                            queue_depth: 0,
                        }],
                    },
                ),
            ]
            .into_iter()
            .collect(),
            generated_at: Instant::now(),
        };
        let local = NodeStateView {
            id: local_id,
            accepts_new_renders: true,
            workers: vec![WorkerView {
                id: 0,
                loaded_profile: Some(profile(7)),
                queue_depth: 1,
            }],
        };

        let decision = dispatcher.decide_with_local_state(&task, &cluster, local);
        let Decision::Forward {
            route_tier,
            candidates,
        } = decision
        else {
            panic!("stale local warmth must not beat current remote capacity");
        };
        assert_eq!(route_tier, RouteTier::Tier2HrwBl);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].node_id, remote_id);
    }

    #[test]
    fn authoritative_local_state_exposes_new_local_capacity() {
        let dispatcher = dispatcher_with_caps(1, 1);
        let local_id = NodeId::from_index(0);
        let task = make_task(9, Instant::now());
        let cluster = ClusterView {
            members: vec![local_id.clone()],
            states: [(
                local_id.clone(),
                NodeStateView {
                    id: local_id.clone(),
                    accepts_new_renders: true,
                    workers: vec![WorkerView {
                        id: 0,
                        loaded_profile: Some(profile(7)),
                        queue_depth: 1,
                    }],
                },
            )]
            .into_iter()
            .collect(),
            generated_at: Instant::now(),
        };
        let local = NodeStateView {
            id: local_id,
            accepts_new_renders: true,
            workers: vec![WorkerView {
                id: 0,
                loaded_profile: None,
                queue_depth: 0,
            }],
        };

        assert!(matches!(
            dispatcher.decide_with_local_state(&task, &cluster, local),
            Decision::Local {
                route_tier: RouteTier::Tier2HrwBl,
                ..
            }
        ));
    }

    #[test]
    fn tier3_uses_live_local_admission_without_duplicating_stale_state() {
        let dispatcher = dispatcher_with_caps(1, 1);
        let local_id = NodeId::from_index(0);
        let remote_id = NodeId::from_index(1);
        let task = make_task(9, Instant::now());
        let cluster = ClusterView {
            members: vec![local_id.clone(), remote_id.clone()],
            states: [
                (
                    local_id.clone(),
                    NodeStateView {
                        id: local_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(7)),
                            queue_depth: 0,
                        }],
                    },
                ),
                (
                    remote_id.clone(),
                    NodeStateView {
                        id: remote_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(8)),
                            queue_depth: 1,
                        }],
                    },
                ),
            ]
            .into_iter()
            .collect(),
            generated_at: Instant::now(),
        };
        let local = NodeStateView {
            id: local_id,
            accepts_new_renders: false,
            workers: vec![WorkerView {
                id: 0,
                loaded_profile: Some(profile(7)),
                queue_depth: 0,
            }],
        };

        let decision = dispatcher.decide_with_local_state(&task, &cluster, local);
        let Decision::Forward {
            route_tier,
            candidates,
        } = decision
        else {
            panic!("Tier 3 must use the remote candidate after live local admission closes");
        };
        assert_eq!(route_tier, RouteTier::Tier3DrainSwap);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].node_id, remote_id);
    }

    #[test]
    fn routing_seed_keeps_task_id_splitmix_contract() {
        const TASK_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

        assert_eq!(routing_seed(0, 0), splitmix64_finalize(0));
        assert_eq!(routing_seed(0, 1), splitmix64_finalize(TASK_MIX));
        assert_eq!(
            routing_seed(7, 2),
            splitmix64_finalize(7 ^ TASK_MIX.wrapping_mul(2))
        );
    }

    fn make_task(style_index: u32, arrived_at: Instant) -> InternalTask {
        InternalTask {
            id: 1,
            request_id: crate::types::RequestId::from_string("dispatcher-test"),
            style: rev(style_index),
            source: None,
            request: RenderRequest::Tile {
                z: 14,
                x: 0,
                y: 0,
                tile_size: 256,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Png,
            arrived_at,
            deadline: arrived_at + Duration::from_secs(10),
            forwarding_hops: 0,
        }
    }

    #[test]
    fn tier1_ignores_capacity_owned_by_an_unrelated_loaded_profile() {
        let target = profile(9);
        let node = NodeStateView {
            id: NodeId::from_index(0),
            accepts_new_renders: true,
            workers: vec![
                WorkerView {
                    id: 0,
                    loaded_profile: Some(target.clone()),
                    queue_depth: 2,
                },
                WorkerView {
                    id: 1,
                    loaded_profile: Some(profile(1)),
                    queue_depth: 0,
                },
            ],
        };

        assert!(warm_candidate(&node, &target, 2).is_none());
    }

    #[test]
    fn tier1_can_expand_a_warm_profile_into_a_fresh_worker() {
        let target = profile(9);
        let node = NodeStateView {
            id: NodeId::from_index(0),
            accepts_new_renders: true,
            workers: vec![
                WorkerView {
                    id: 0,
                    loaded_profile: Some(target.clone()),
                    queue_depth: 2,
                },
                WorkerView {
                    id: 1,
                    loaded_profile: None,
                    queue_depth: 0,
                },
            ],
        };

        let candidate = warm_candidate(&node, &target, 2).expect("fresh capacity is usable");
        assert_eq!(candidate.warm_count, 1);
        assert_eq!(candidate.style_spare_bl, 0);
    }

    #[test]
    fn top_hrw_candidates_matches_stable_full_sort() {
        let target = profile(9);
        let members: Vec<_> = (0..8).map(NodeId::from_index).collect();
        let eligible = |node_id: &NodeId| node_id != &NodeId::from_index(3);
        let actual = top_hrw_candidates(&members, &target, 2, eligible);

        let mut expected: Vec<_> = members.iter().filter(|node_id| eligible(node_id)).collect();
        expected.sort_by_key(|node_id| Reverse(hrw_weight(&target, node_id)));

        assert_eq!(
            actual
                .iter()
                .map(|candidate| &candidate.node_id)
                .collect::<Vec<_>>(),
            expected.into_iter().take(2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn tier3_allows_eviction_when_cluster_has_multiple_workers() {
        let now = Instant::now();
        let d = dispatcher();

        let task = make_task(9, now);
        let picked = d.tier3_candidates(
            &view(vec![
                WorkerView {
                    id: 0,
                    loaded_profile: Some(profile(1)),
                    queue_depth: 1,
                },
                WorkerView {
                    id: 1,
                    loaded_profile: Some(profile(1)),
                    queue_depth: 1,
                },
                WorkerView {
                    id: 2,
                    loaded_profile: Some(profile(2)),
                    queue_depth: 1,
                },
            ]),
            &task,
            1,
        );

        assert_eq!(
            picked,
            vec![ForwardCandidate {
                node_id: NodeId::from_index(0),
                drain_worker: Some(0),
            }]
        );
    }

    #[test]
    fn tier3_does_not_pick_incoming_profile_as_eviction_candidate() {
        let now = Instant::now();
        let d = dispatcher();

        let task = make_task(9, now);
        let picked = d.tier3_candidates(
            &view(vec![
                WorkerView {
                    id: 0,
                    loaded_profile: Some(profile(9)),
                    queue_depth: 1,
                },
                WorkerView {
                    id: 1,
                    loaded_profile: Some(profile(9)),
                    queue_depth: 1,
                },
            ]),
            &task,
            1,
        );

        assert!(picked.is_empty());
    }

    #[test]
    fn tier3_can_fall_back_to_single_worker_other_style() {
        let now = Instant::now();
        let d = dispatcher();

        let task = make_task(9, now);
        let picked = d.tier3_candidates(
            &view(vec![WorkerView {
                id: 0,
                loaded_profile: Some(profile(1)),
                queue_depth: 1,
            }]),
            &task,
            1,
        );

        assert_eq!(
            picked,
            vec![ForwardCandidate {
                node_id: NodeId::from_index(0),
                drain_worker: Some(0),
            }]
        );
    }

    #[test]
    fn tier3_prefers_same_renderer_shape_within_over_allocated_profiles() {
        let now = Instant::now();
        let d = dispatcher();

        let task = make_task(9, now);
        let picked = d.tier3_candidates(
            &view(vec![
                WorkerView {
                    id: 0,
                    loaded_profile: Some(profile_with(1, RenderMode::Static, Scale::X1)),
                    queue_depth: 1,
                },
                WorkerView {
                    id: 1,
                    loaded_profile: Some(profile(2)),
                    queue_depth: 1,
                },
                WorkerView {
                    id: 2,
                    loaded_profile: Some(profile_with(1, RenderMode::Static, Scale::X1)),
                    queue_depth: 1,
                },
                WorkerView {
                    id: 3,
                    loaded_profile: Some(profile(2)),
                    queue_depth: 1,
                },
            ]),
            &task,
            1,
        );

        assert_eq!(
            picked,
            vec![ForwardCandidate {
                node_id: NodeId::from_index(0),
                drain_worker: Some(1),
            }]
        );
    }

    #[test]
    fn decide_uses_overflow_when_tier3_is_too_slow_but_hard_capacity_remains() {
        let now = Instant::now();
        let d = dispatcher_with_caps(1, 4);

        let task = make_task(9, now);

        let decision = d.decide(
            &task,
            &view(vec![WorkerView {
                id: 0,
                loaded_profile: Some(profile(9)),
                queue_depth: 1,
            }]),
        );

        assert!(matches!(
            decision,
            Decision::Local {
                route_tier: RouteTier::Tier4Overflow,
                worker_hint: None,
                fallback_candidates,
            }
            if fallback_candidates.is_empty()
        ));
    }

    #[test]
    fn explicit_renderer_degradation_excludes_node_from_every_routing_tier() {
        let now = Instant::now();
        let dispatcher = dispatcher_with_caps(2, 8);
        let task = make_task(9, now);
        let mut cluster = view(vec![WorkerView {
            id: 0,
            loaded_profile: Some(profile(9)),
            queue_depth: 0,
        }]);
        cluster
            .states
            .get_mut(&NodeId::from_index(0))
            .expect("node state")
            .accepts_new_renders = false;

        assert!(matches!(
            dispatcher.decide(&task, &cluster),
            Decision::Reject {
                reason: RejectionReason::NoCapacity
            }
        ));
    }

    #[test]
    fn decide_forwards_top_two_candidates_when_remote_capacity_exists() {
        let now = Instant::now();
        let d = dispatcher_with_caps(2, 8);
        let task = make_task(9, now);
        let view = ClusterView {
            members: vec![
                NodeId::from_index(1),
                NodeId::from_index(2),
                NodeId::from_index(3),
            ],
            states: [
                (
                    NodeId::from_index(1),
                    NodeStateView {
                        id: NodeId::from_index(1),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(1)),
                            queue_depth: 0,
                        }],
                    },
                ),
                (
                    NodeId::from_index(2),
                    NodeStateView {
                        id: NodeId::from_index(2),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(2)),
                            queue_depth: 0,
                        }],
                    },
                ),
                (
                    NodeId::from_index(3),
                    NodeStateView {
                        id: NodeId::from_index(3),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(3)),
                            queue_depth: 0,
                        }],
                    },
                ),
            ]
            .into_iter()
            .collect(),
            generated_at: now,
        };

        let decision = d.decide(&task, &view);
        let Decision::Forward {
            route_tier,
            candidates,
        } = decision
        else {
            panic!("expected forward decision");
        };
        assert_eq!(route_tier, RouteTier::Tier2HrwBl);
        assert_eq!(candidates.len(), DEFAULT_FORWARD_CANDIDATES);
        assert!(candidates.iter().all(|c| c.drain_worker.is_none()));
    }

    #[test]
    fn local_decision_retains_remaining_remote_candidates() {
        let dispatcher = dispatcher();
        let task = make_task(9, Instant::now());
        let remote = ForwardCandidate {
            node_id: NodeId::from_index(1),
            drain_worker: Some(2),
        };

        let decision = dispatcher.materialize_candidates(
            &task,
            RouteTier::Tier2HrwBl,
            vec![
                ForwardCandidate {
                    node_id: NodeId::from_index(0),
                    drain_worker: Some(0),
                },
                remote.clone(),
            ],
        );

        let Decision::Local {
            fallback_candidates,
            ..
        } = decision
        else {
            panic!("expected local decision");
        };
        assert_eq!(fallback_candidates, vec![remote]);
    }

    #[test]
    fn tier1_warm_local_route_keeps_remote_fallback_for_stale_health() {
        let dispatcher = dispatcher();
        let now = Instant::now();
        let task = make_task(9, now);
        let local_id = NodeId::from_index(0);
        let remote_id = NodeId::from_index(1);
        let cluster = ClusterView {
            members: vec![local_id.clone(), remote_id.clone()],
            states: HashMap::from([
                (
                    local_id.clone(),
                    NodeStateView {
                        id: local_id,
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: Some(profile(9)),
                            queue_depth: 0,
                        }],
                    },
                ),
                (
                    remote_id.clone(),
                    NodeStateView {
                        id: remote_id.clone(),
                        accepts_new_renders: true,
                        workers: vec![WorkerView {
                            id: 0,
                            loaded_profile: None,
                            queue_depth: 0,
                        }],
                    },
                ),
            ]),
            generated_at: now,
        };

        let Decision::Local {
            route_tier,
            fallback_candidates,
            ..
        } = dispatcher.decide(&task, &cluster)
        else {
            panic!("warm local worker should remain the primary route");
        };
        assert_eq!(route_tier, RouteTier::Tier1WarmTracking);
        assert_eq!(
            fallback_candidates,
            vec![ForwardCandidate {
                node_id: remote_id,
                drain_worker: None,
            }]
        );
    }
}
