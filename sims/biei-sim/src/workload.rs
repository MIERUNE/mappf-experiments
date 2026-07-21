//! Workload generator: Poisson per-tick task arrivals + style/source sampling
//! (Zipf, Burst, PeriodicRefresh source pools, etc.).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, ensure};
use rand::distr::{Distribution, weighted::WeightedIndex};
use rand::{Rng, RngExt, SeedableRng};
use rand_distr::{Exp, Poisson, Zipf};
use rand_xoshiro::Xoshiro256PlusPlus;
use tokio::task::{Id, JoinError, JoinSet};
use tokio::time::{Instant, sleep_until};

use crate::churn::ChurnTracker;
use crate::config::{
    BurstPattern, SourcePattern, SourceProvider, StyleDist, StyleShift, WorkloadConfig,
};
use crate::harness::WorkloadCluster;
use crate::metrics::{MetricsCollector, MetricsObservation};
use biei_core::types::{
    CachePolicy, ImageFormat, PixelRatio, Positioning, RenderRequest, Scale, SourceHash, SourceRef,
    StyleId, StyleRevision, TaskSpec,
};

pub(crate) struct WorkloadSummary {
    pub submitted_total: u64,
    pub submitted_measured: u64,
}

#[derive(Default)]
struct RequestTasks {
    tasks: JoinSet<()>,
    request_ids: HashMap<Id, u64>,
    failures: Vec<RequestTaskFailure>,
}

struct RequestTaskFailure {
    request_id: Option<u64>,
    error: JoinError,
}

impl RequestTasks {
    fn spawn<F>(&mut self, request_id: u64, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task_id = self.tasks.spawn(task).id();
        let previous = self.request_ids.insert(task_id, request_id);
        debug_assert!(previous.is_none());
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    async fn join_one(&mut self) -> bool {
        let result = self
            .tasks
            .join_next_with_id()
            .await
            .expect("request task set was non-empty");
        self.record_join(result)
    }

    fn reap_ready(&mut self) -> bool {
        let failures_before = self.failures.len();
        while let Some(result) = self.tasks.try_join_next_with_id() {
            self.record_join(result);
        }
        self.failures.len() != failures_before
    }

    async fn drain(&mut self) {
        let mut aborting = !self.failures.is_empty();
        if aborting {
            self.tasks.abort_all();
        }

        while let Some(result) = self.tasks.join_next_with_id().await {
            if aborting
                && let Err(error) = &result
                && error.is_cancelled()
            {
                let request_id = self.request_ids.remove(&error.id());
                debug_assert!(request_id.is_some());
                continue;
            }

            if self.record_join(result) && !aborting {
                aborting = true;
                self.tasks.abort_all();
            }
        }
        debug_assert!(self.request_ids.is_empty());
    }

    fn record_join(&mut self, result: std::result::Result<(Id, ()), JoinError>) -> bool {
        match result {
            Ok((task_id, ())) => {
                let request_id = self.request_ids.remove(&task_id);
                debug_assert!(request_id.is_some());
                false
            }
            Err(error) => {
                let request_id = self.request_ids.remove(&error.id());
                self.failures.push(RequestTaskFailure { request_id, error });
                true
            }
        }
    }

    fn into_result(self) -> Result<()> {
        if self.failures.is_empty() {
            return Ok(());
        }

        let failures = self
            .failures
            .iter()
            .map(|failure| match failure.request_id {
                Some(request_id) => {
                    format!("workload request {request_id}: {}", failure.error)
                }
                None => format!("unknown workload request: {}", failure.error),
            })
            .collect::<Vec<_>>()
            .join("; ");
        anyhow::bail!(
            "{} workload request task(s) failed to join: {failures}",
            self.failures.len()
        )
    }
}

fn reconcile_measured_outcomes(
    submitted_measured: u64,
    outcomes: &MetricsObservation,
) -> Result<()> {
    ensure!(
        outcomes.total == outcomes.completed + outcomes.rejected + outcomes.failed,
        "measured workload outcomes do not reconcile: total={}, completed={}, rejected={}, failed={}",
        outcomes.total,
        outcomes.completed,
        outcomes.rejected,
        outcomes.failed
    );
    ensure!(
        submitted_measured == outcomes.total as u64,
        "measured workload submissions do not reconcile with terminal outcomes: submitted={submitted_measured}, outcomes={}",
        outcomes.total
    );
    Ok(())
}

pub(crate) async fn run_workload(
    config: WorkloadConfig,
    cluster: &mut WorkloadCluster,
    metrics: Arc<MetricsCollector>,
    task_budget: Duration,
    seed: u64,
    mut churn: Option<&mut ChurnTracker>,
) -> Result<WorkloadSummary> {
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed);
    let start = Instant::now();
    let deadline = start + config.duration;
    let record_after = start + config.warmup;
    let mut next_id: u64 = 0;
    let mut measured_next_id: u64 = 0;
    let mut style_count = config.style_count.max(1);

    let mut next_new_style_at: Option<Instant> = if config.new_style_rate > 0.0 {
        let dt = Exp::new(config.new_style_rate)
            .expect("new_style_rate must be > 0")
            .sample(&mut rng);
        Some(start + Duration::from_secs_f64(dt))
    } else {
        None
    };

    let mut source_state = SourceGenState::init(config.source_pattern.as_ref(), start, &mut rng);
    let mut style_sampler = CompiledDistribution::new(&config.style_distribution, style_count);

    let mut inflight = RequestTasks::default();

    // Tokio's paused-time sleep has coarse granularity (~2-3ms on observed
    // setups), so Exp-distributed sub-millisecond interarrivals get clipped
    // at high rates. Instead we tick periodically and Poisson-sample arrivals
    // scaled by the *actual* elapsed since the last tick. This keeps the
    // configured rate honored regardless of how the runtime rounds sleeps.
    let tick = Duration::from_millis(1);
    let mut last_tick = start;
    let mut next_tick = start + tick;
    let mut generation_error = None;

    'generation: loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        if inflight.is_empty() {
            sleep_until(next_tick).await;
        } else {
            tokio::select! {
                join_failed = inflight.join_one() => {
                    if join_failed {
                        break 'generation;
                    }
                    continue;
                }
                _ = sleep_until(next_tick) => {}
            }
        }

        let now = Instant::now();
        next_tick = now + tick;
        let actual_elapsed = now.duration_since(last_tick).as_secs_f64();
        last_tick = now;

        // How many tasks land in this tick? Scale by actual elapsed.
        let rate = current_rate(now, start, config.total_rate, &config.burst_pattern);
        let expected = rate * actual_elapsed;
        let n_tasks: u32 = if expected <= 0.0 {
            0
        } else if expected < 0.05 {
            // Bernoulli for rare events to avoid Poisson degeneracy.
            if rng.random::<f64>() < expected { 1 } else { 0 }
        } else {
            Poisson::new(expected)
                .expect("rate must be > 0")
                .sample(&mut rng) as u32
        };

        let previous_style_count = style_count;
        while let Some(t) = next_new_style_at {
            if t <= now {
                style_count += 1;
                let dt = Exp::new(config.new_style_rate)
                    .expect("rate")
                    .sample(&mut rng);
                next_new_style_at = Some(t + Duration::from_secs_f64(dt));
            } else {
                break;
            }
        }
        if style_count != previous_style_count {
            style_sampler = CompiledDistribution::new(&config.style_distribution, style_count);
        }

        for _ in 0..n_tasks {
            if inflight.reap_ready() {
                break 'generation;
            }

            let measured = now >= record_after;
            let mut submission_epoch = 0;
            if measured && let Some(tracker) = churn.as_deref_mut() {
                match tracker
                    .before_request(cluster, &metrics, measured_next_id)
                    .await
                {
                    Ok(epoch) => submission_epoch = epoch,
                    Err(error) => {
                        generation_error = Some(error);
                        break 'generation;
                    }
                }
            }
            let style = sample_style(
                now,
                start,
                &style_sampler,
                &config.burst_pattern,
                &config.style_shift,
                &mut rng,
            );

            let source = match config.source_pattern.as_ref() {
                Some(p) => source_state.sample(p, now, &mut rng),
                None => None,
            };
            let request = render_request_for_style(style, config.tile_style_count);
            let pixel_ratio = PixelRatio::from(Scale::X2);

            // Simulator StyleIds are lazily resolved by StyleCatalog through
            // the same template path as production. In-place style updates
            // are not modelled, so all generated styles use the initial
            // catalog version.
            let style_revision = StyleRevision {
                id: StyleId(format!("style-{}", style)),
                version: 1,
            };
            let task = TaskSpec {
                id: next_id,
                request_id: request_id_from_task_id(next_id),
                style: style_revision,
                source,
                request,
                pixel_ratio,
                output_format: ImageFormat::Png,
            }
            .start(now, task_budget);
            next_id += 1;
            if measured {
                measured_next_id += 1;
            }

            let request_id = next_id - 1;
            let (node, counters) = cluster.select(&mut rng);
            counters.submit(measured);
            if measured {
                metrics.submit(submission_epoch);
            }
            let m = metrics.clone();
            inflight.spawn(request_id, async move {
                let outcome = node.handle_incoming(task).await;
                counters.record(&outcome, measured);
                if measured {
                    m.record(outcome, submission_epoch);
                }
            });
        }
    }

    inflight.drain().await;
    let join_result = inflight.into_result();

    match (generation_error, join_result) {
        (Some(error), Ok(())) => return Err(error),
        (None, Err(error)) => return Err(error),
        (Some(generation_error), Err(join_error)) => {
            anyhow::bail!(
                "workload generation failed: {generation_error:#}; request task draining also failed: {join_error:#}"
            );
        }
        (None, Ok(())) => {}
    }

    if let Some(tracker) = churn {
        tracker.after_workload(cluster, &metrics).await?;
    }

    reconcile_measured_outcomes(measured_next_id, &metrics.observation())?;

    Ok(WorkloadSummary {
        submitted_total: next_id,
        submitted_measured: measured_next_id,
    })
}

fn render_request_for_style(style: u32, tile_style_count: usize) -> RenderRequest {
    if (style as usize) < tile_style_count {
        RenderRequest::Tile {
            z: 14,
            x: 0,
            y: 0,
            tile_size: 512,
        }
    } else {
        RenderRequest::StaticImage {
            positioning: Positioning::Center {
                lon: 139.767,
                lat: 35.681,
                zoom: 12.0,
                bearing: 0.0,
                pitch: 0.0,
            },
            width: 512,
            height: 512,
            overlays: Vec::new(),
            before_layer: None,
            padding: biei_core::types::Padding::default(),
            addlayer: None,
        }
    }
}

fn in_burst(now: Instant, start: Instant, burst: &BurstPattern) -> bool {
    let period = burst.period.as_secs_f64();
    if period == 0.0 {
        return false;
    }
    let elapsed = now.duration_since(start).as_secs_f64();
    let cycle = elapsed % period;
    cycle < burst.duration.as_secs_f64()
}

fn current_rate(now: Instant, start: Instant, base: f64, burst: &Option<BurstPattern>) -> f64 {
    match burst {
        Some(b) if in_burst(now, start, b) => base * b.multiplier,
        _ => base,
    }
}

fn request_id_from_task_id(task_id: u64) -> biei_core::types::RequestId {
    biei_core::types::RequestId::try_new(format!("{task_id:032x}"))
        .expect("lowercase hexadecimal task IDs are valid request IDs")
}

fn sample_style(
    now: Instant,
    start: Instant,
    sampler: &CompiledDistribution,
    burst: &Option<BurstPattern>,
    shift: &Option<StyleShift>,
    rng: &mut impl Rng,
) -> u32 {
    if let Some(b) = burst
        && in_burst(now, start, b)
        && let Some(focus) = b.style_focus
    {
        return focus;
    }
    let id = sampler.sample(rng) as u32;
    apply_style_shift(id, now, start, shift)
}

/// Once `start + shift.at` is reached, swap rank-0 ↔ `shift.with` so the
/// previously-top style stops receiving traffic and a mid-rank style
/// suddenly dominates.
fn apply_style_shift(id: u32, now: Instant, start: Instant, shift: &Option<StyleShift>) -> u32 {
    if let Some(s) = shift
        && now >= start + s.at
    {
        if id == 0 {
            return s.with;
        }
        if id == s.with {
            return 0;
        }
    }
    id
}

enum CompiledDistribution {
    Uniform { len: usize },
    Zipf { distribution: Zipf<f64>, len: usize },
    Custom(WeightedIndex<f64>),
}

impl CompiledDistribution {
    fn new(distribution: &StyleDist, len: usize) -> Self {
        let len = len.max(1);
        match distribution {
            StyleDist::Uniform => Self::Uniform { len },
            StyleDist::Zipf { alpha } => Self::Zipf {
                distribution: Zipf::new(len as f64, *alpha)
                    .expect("Zipf requires alpha > 0 and n >= 1"),
                len,
            },
            StyleDist::Custom(weights) => {
                let effective = if weights.len() >= len {
                    &weights[..len]
                } else {
                    weights.as_slice()
                };
                Self::Custom(WeightedIndex::new(effective).expect("non-empty positive weights"))
            }
        }
    }

    fn sample(&self, rng: &mut impl Rng) -> usize {
        match self {
            Self::Uniform { len } => rng.random_range(0..*len),
            Self::Zipf { distribution, len } => {
                let value: f64 = distribution.sample(rng);
                (value as usize).saturating_sub(1).min(*len - 1)
            }
            Self::Custom(distribution) => distribution.sample(rng),
        }
    }
}

/// Compiled source-provider tree plus mutable state for periodic and one-shot
/// sources. Probability distributions are constructed once and retain the same
/// sampling operations as their configuration-driven counterparts.
struct SourceGenState {
    provider: Option<CompiledSourceProvider>,
    periodic_pools: Vec<PeriodicPool>,
    oneshot_counter: u64,
}

enum CompiledSourceProvider {
    Shared {
        distribution: CompiledDistribution,
        namespace: SourceHash,
    },
    PeriodicRefresh {
        pool_index: usize,
        interval: Duration,
        jitter: Duration,
    },
    OneShot,
    Mixed {
        distribution: WeightedIndex<f64>,
        choices: Vec<CompiledSourceProvider>,
    },
}

struct PeriodicPool {
    pool: Vec<SourceHash>,
    next_refresh: Vec<Instant>,
    next_due: Option<Instant>,
}

impl SourceGenState {
    fn init(pattern: Option<&SourcePattern>, start: Instant, rng: &mut impl Rng) -> Self {
        let mut periodic_pools = Vec::new();
        let provider = pattern.map(|pattern| {
            CompiledSourceProvider::new(
                &pattern.provider,
                &mut Vec::new(),
                &mut periodic_pools,
                start,
                rng,
            )
        });
        Self {
            provider,
            periodic_pools,
            oneshot_counter: 0,
        }
    }

    fn sample(
        &mut self,
        pattern: &SourcePattern,
        now: Instant,
        rng: &mut impl Rng,
    ) -> Option<SourceRef> {
        if rng.random::<f64>() > pattern.probability {
            return None;
        }
        self.provider.as_ref()?.sample(
            &mut self.periodic_pools,
            &mut self.oneshot_counter,
            now,
            rng,
        )
    }
}

impl CompiledSourceProvider {
    fn new(
        provider: &SourceProvider,
        path: &mut Vec<usize>,
        periodic_pools: &mut Vec<PeriodicPool>,
        start: Instant,
        rng: &mut impl Rng,
    ) -> Self {
        match provider {
            SourceProvider::Shared {
                source_count,
                distribution,
            } => Self::Shared {
                distribution: CompiledDistribution::new(distribution, *source_count),
                namespace: path_namespaced(path, 0),
            },
            SourceProvider::PeriodicRefresh {
                source_count,
                interval,
                jitter,
            } => {
                let pool_index = periodic_pools.len();
                let mut pool = Vec::with_capacity(*source_count);
                let mut next_refresh = Vec::with_capacity(*source_count);
                for _ in 0..*source_count {
                    pool.push(rng.random::<u64>() & !(1u64 << 63));
                    next_refresh.push(start + sample_refresh_delay(*interval, *jitter, rng));
                }
                let next_due = next_refresh.iter().copied().min();
                periodic_pools.push(PeriodicPool {
                    pool,
                    next_refresh,
                    next_due,
                });
                Self::PeriodicRefresh {
                    pool_index,
                    interval: *interval,
                    jitter: *jitter,
                }
            }
            SourceProvider::OneShot => Self::OneShot,
            SourceProvider::Mixed(choices) => {
                let distribution = WeightedIndex::new(choices.iter().map(|(weight, _)| *weight))
                    .expect("mixed weights must be positive");
                let choices = choices
                    .iter()
                    .enumerate()
                    .map(|(index, (_, child))| {
                        path.push(index);
                        let compiled = Self::new(child, path, periodic_pools, start, rng);
                        path.pop();
                        compiled
                    })
                    .collect();
                Self::Mixed {
                    distribution,
                    choices,
                }
            }
        }
    }

    fn sample(
        &self,
        periodic_pools: &mut [PeriodicPool],
        oneshot_counter: &mut u64,
        now: Instant,
        rng: &mut impl Rng,
    ) -> Option<SourceRef> {
        match self {
            Self::Shared {
                distribution,
                namespace,
            } => Some(SourceRef {
                hash: *namespace | ((distribution.sample(rng) as u64) & 0x00FF_FFFF_FFFF_FFFF),
                policy: CachePolicy::Cacheable,
            }),
            Self::PeriodicRefresh {
                pool_index,
                interval,
                jitter,
            } => {
                let pool_state = periodic_pools.get_mut(*pool_index)?;
                refresh_pool(pool_state, *interval, *jitter, now, rng);
                if pool_state.pool.is_empty() {
                    return None;
                }
                let pick = rng.random_range(0..pool_state.pool.len());
                Some(SourceRef {
                    hash: pool_state.pool[pick],
                    policy: CachePolicy::Cacheable,
                })
            }
            Self::OneShot => {
                *oneshot_counter = oneshot_counter.wrapping_add(1);
                Some(SourceRef {
                    hash: *oneshot_counter | (1u64 << 63),
                    policy: CachePolicy::OneShot,
                })
            }
            Self::Mixed {
                distribution,
                choices,
            } => {
                choices[distribution.sample(rng)].sample(periodic_pools, oneshot_counter, now, rng)
            }
        }
    }
}

fn refresh_pool(
    pool_state: &mut PeriodicPool,
    interval: Duration,
    jitter: Duration,
    now: Instant,
    rng: &mut impl Rng,
) {
    if pool_state.next_due.is_some_and(|next_due| now < next_due) {
        return;
    }

    for i in 0..pool_state.pool.len() {
        if now >= pool_state.next_refresh[i] {
            pool_state.pool[i] = rng.random::<u64>() & !(1u64 << 63);
            pool_state.next_refresh[i] = now + sample_refresh_delay(interval, jitter, rng);
        }
    }
    pool_state.next_due = pool_state.next_refresh.iter().copied().min();
}

/// Samples one `interval ± jitter` refresh delay with symmetric signed jitter,
/// used for both the initial schedule and each recurring refresh so the realized
/// mean is the configured interval (not `interval + jitter/2`). `jitter` is
/// capped at `interval` so the lower bound stays non-negative, matching the
/// documented `interval ± jitter`.
fn sample_refresh_delay(interval: Duration, jitter: Duration, rng: &mut impl Rng) -> Duration {
    let jitter = jitter.min(interval);
    let jitter_secs = jitter.as_secs_f64();
    let signed = if jitter_secs > 0.0 {
        rng.random_range(-jitter_secs..jitter_secs)
    } else {
        0.0
    };
    Duration::from_secs_f64((interval.as_secs_f64() + signed).max(0.0))
}

/// Namespace a Shared source hash by the path through the provider tree so
/// pools in different `Mixed` branches don't collide.
fn path_namespaced(path: &[usize], source_idx: u64) -> SourceHash {
    // Fold the path indices (each capped to 8 bits) into the top byte,
    // leaving bit 63 reserved for OneShot.
    let mut stamp: u64 = 0;
    for &p in path.iter().take(7) {
        stamp = (stamp << 8) | ((p as u64) & 0xFF);
    }
    let stamp = (stamp & 0x7F) << 56;
    stamp | (source_idx & 0x00FF_FFFF_FFFF_FFFF)
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use rand::distr::{Distribution, weighted::WeightedIndex};
    use rand::{Rng, RngExt, SeedableRng};
    use rand_distr::Zipf;
    use rand_xoshiro::Xoshiro256PlusPlus;

    use super::{
        CompiledDistribution, PeriodicPool, RequestTasks, SourceGenState, path_namespaced,
        reconcile_measured_outcomes, refresh_pool, request_id_from_task_id,
    };
    use crate::config::{SourcePattern, SourceProvider, StyleDist};
    use crate::metrics::MetricsObservation;

    fn legacy_sample_from_dist(dist: &StyleDist, len: usize, rng: &mut impl Rng) -> usize {
        let len = len.max(1);
        match dist {
            StyleDist::Uniform => rng.random_range(0..len),
            StyleDist::Zipf { alpha } => {
                let distribution = Zipf::new(len as f64, *alpha).expect("valid Zipf");
                let value: f64 = distribution.sample(rng);
                (value as usize).saturating_sub(1).min(len - 1)
            }
            StyleDist::Custom(weights) => {
                let effective = if weights.len() >= len {
                    &weights[..len]
                } else {
                    weights.as_slice()
                };
                WeightedIndex::new(effective)
                    .expect("valid weights")
                    .sample(rng)
            }
        }
    }

    fn distribution_sequence(dist: &StyleDist, len: usize, seed: u64) -> Vec<usize> {
        let distribution = CompiledDistribution::new(dist, len);
        let mut compiled_rng = Xoshiro256PlusPlus::seed_from_u64(seed);
        let mut legacy_rng = Xoshiro256PlusPlus::seed_from_u64(seed);
        let sequence = (0..16)
            .map(|_| distribution.sample(&mut compiled_rng))
            .collect::<Vec<_>>();
        let legacy = (0..16)
            .map(|_| legacy_sample_from_dist(dist, len, &mut legacy_rng))
            .collect::<Vec<_>>();
        assert_eq!(sequence, legacy, "compiled sampler changed RNG sequencing");
        assert_eq!(
            compiled_rng.random::<u64>(),
            legacy_rng.random::<u64>(),
            "compiled sampler changed the final RNG state"
        );
        sequence
    }

    #[test]
    fn compiled_distributions_match_golden_sequences() {
        assert_eq!(
            distribution_sequence(&StyleDist::Uniform, 7, 0x1234),
            vec![3, 4, 1, 6, 3, 0, 1, 6, 4, 1, 1, 6, 6, 4, 5, 5]
        );
        assert_eq!(
            distribution_sequence(&StyleDist::Zipf { alpha: 1.2 }, 7, 0x1234),
            vec![1, 0, 1, 0, 1, 0, 5, 2, 3, 2, 3, 0, 1, 0, 4, 6]
        );
        assert_eq!(
            distribution_sequence(&StyleDist::Custom(vec![1.0, 4.0, 2.0, 8.0, 3.0]), 5, 0x1234,),
            vec![3, 3, 1, 4, 3, 1, 1, 4, 3, 1, 1, 4, 4, 3, 3, 4]
        );
    }

    fn legacy_sample_provider(
        provider: &SourceProvider,
        path: &mut Vec<usize>,
        oneshot_counter: &mut u64,
        rng: &mut impl Rng,
    ) -> u64 {
        match provider {
            SourceProvider::Shared {
                source_count,
                distribution,
            } => path_namespaced(
                path,
                legacy_sample_from_dist(distribution, *source_count, rng) as u64,
            ),
            SourceProvider::OneShot => {
                *oneshot_counter = oneshot_counter.wrapping_add(1);
                *oneshot_counter | (1_u64 << 63)
            }
            SourceProvider::Mixed(choices) => {
                let pick = WeightedIndex::new(choices.iter().map(|(weight, _)| *weight))
                    .expect("valid mixed weights")
                    .sample(rng);
                path.push(pick);
                let hash = legacy_sample_provider(&choices[pick].1, path, oneshot_counter, rng);
                path.pop();
                hash
            }
            SourceProvider::PeriodicRefresh { .. } => {
                panic!("periodic providers are not used by this golden test")
            }
        }
    }

    #[test]
    fn nested_mixed_provider_matches_golden_sequence() {
        let pattern = SourcePattern {
            probability: 1.0,
            provider: SourceProvider::Mixed(vec![
                (
                    2.0,
                    Box::new(SourceProvider::Shared {
                        source_count: 3,
                        distribution: StyleDist::Uniform,
                    }),
                ),
                (
                    3.0,
                    Box::new(SourceProvider::Mixed(vec![
                        (
                            1.0,
                            Box::new(SourceProvider::Shared {
                                source_count: 4,
                                distribution: StyleDist::Zipf { alpha: 1.2 },
                            }),
                        ),
                        (
                            4.0,
                            Box::new(SourceProvider::Shared {
                                source_count: 3,
                                distribution: StyleDist::Custom(vec![1.0, 3.0, 2.0]),
                            }),
                        ),
                    ])),
                ),
            ]),
        };
        let now = tokio::time::Instant::now();
        let mut compiled_rng = Xoshiro256PlusPlus::seed_from_u64(0x5678);
        let mut legacy_rng = Xoshiro256PlusPlus::seed_from_u64(0x5678);
        let mut state = SourceGenState::init(Some(&pattern), now, &mut compiled_rng);
        let compiled = (0..16)
            .map(|_| state.sample(&pattern, now, &mut compiled_rng).unwrap().hash)
            .collect::<Vec<_>>();
        let mut oneshot_counter = 0;
        let legacy = (0..16)
            .map(|_| {
                assert!(legacy_rng.random::<f64>() <= pattern.probability);
                legacy_sample_provider(
                    &pattern.provider,
                    &mut Vec::new(),
                    &mut oneshot_counter,
                    &mut legacy_rng,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(compiled, legacy, "compiled tree changed RNG sequencing");
        assert_eq!(
            compiled_rng.random::<u64>(),
            legacy_rng.random::<u64>(),
            "compiled tree changed the final RNG state"
        );
        assert_eq!(
            compiled,
            vec![
                0,
                72_057_594_037_927_938,
                72_057_594_037_927_937,
                72_057_594_037_927_936,
                72_057_594_037_927_936,
                2,
                72_057_594_037_927_937,
                0,
                0,
                2,
                72_057_594_037_927_938,
                1,
                72_057_594_037_927_937,
                0,
                0,
                1,
            ]
        );
    }

    #[test]
    fn dynamic_style_growth_matches_golden_sequence() {
        let distribution = StyleDist::Zipf { alpha: 1.1 };
        let style_counts = [3, 3, 4, 4, 5, 8, 8, 9, 12, 12, 13, 21];
        let mut compiled_rng = Xoshiro256PlusPlus::seed_from_u64(0x9abc);
        let mut legacy_rng = Xoshiro256PlusPlus::seed_from_u64(0x9abc);
        let mut current_count = style_counts[0];
        let mut sampler = CompiledDistribution::new(&distribution, current_count);
        let mut compiled = Vec::new();
        let mut legacy = Vec::new();
        for style_count in style_counts {
            if style_count != current_count {
                current_count = style_count;
                sampler = CompiledDistribution::new(&distribution, current_count);
            }
            compiled.push(sampler.sample(&mut compiled_rng));
            legacy.push(legacy_sample_from_dist(
                &distribution,
                style_count,
                &mut legacy_rng,
            ));
        }

        assert_eq!(compiled, legacy, "style growth changed RNG sequencing");
        assert_eq!(
            compiled_rng.random::<u64>(),
            legacy_rng.random::<u64>(),
            "style growth changed the final RNG state"
        );
        assert_eq!(compiled, vec![0, 0, 1, 0, 1, 2, 3, 0, 2, 0, 5, 0]);
    }

    #[test]
    fn request_ids_are_valid_unique_fixed_width_task_ids() {
        let ids = [0, 1, 2, u64::MAX]
            .map(request_id_from_task_id)
            .map(|id| id.as_str().to_owned());

        assert_eq!(ids[0], "00000000000000000000000000000000");
        assert_eq!(ids[1], "00000000000000000000000000000001");
        assert_eq!(ids[3], "0000000000000000ffffffffffffffff");
        assert!(ids.iter().all(|id| id.len() == 32));
        assert!(ids.iter().all(|id| {
            id.bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        }));
        let unique = ids.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(unique.len(), ids.len());
        for id in ids {
            biei_core::types::RequestId::try_new(id).expect("generated ID must be valid");
        }
    }

    #[test]
    fn periodic_refresh_fast_path_preserves_due_order_and_rng() {
        let start = tokio::time::Instant::now();
        let next_refresh = [
            start + Duration::from_secs(10),
            start + Duration::from_secs(5),
            start + Duration::from_secs(5),
        ];
        let make_pool = || PeriodicPool {
            pool: vec![11, 22, 33],
            next_refresh: next_refresh.to_vec(),
            next_due: next_refresh.iter().copied().min(),
        };
        let interval = Duration::from_secs(20);
        let jitter = Duration::from_secs(3);
        let mut fast_pool = make_pool();
        let mut fast_rng = Xoshiro256PlusPlus::seed_from_u64(0xdef0);
        let mut untouched_rng = Xoshiro256PlusPlus::seed_from_u64(0xdef0);

        refresh_pool(
            &mut fast_pool,
            interval,
            jitter,
            start + Duration::from_secs(4),
            &mut fast_rng,
        );
        assert_eq!(fast_pool.pool, [11, 22, 33]);
        assert_eq!(
            fast_rng.clone().random::<u64>(),
            untouched_rng.random::<u64>()
        );

        let mut scanned_pool = make_pool();
        let mut scanned_rng = Xoshiro256PlusPlus::seed_from_u64(0xdef0);
        let due = start + Duration::from_secs(5);
        refresh_pool(&mut fast_pool, interval, jitter, due, &mut fast_rng);
        for index in 0..scanned_pool.pool.len() {
            if due >= scanned_pool.next_refresh[index] {
                scanned_pool.pool[index] = scanned_rng.random::<u64>() & !(1_u64 << 63);
                scanned_pool.next_refresh[index] =
                    due + super::sample_refresh_delay(interval, jitter, &mut scanned_rng);
            }
        }

        assert_eq!(fast_pool.pool, scanned_pool.pool);
        assert_eq!(fast_pool.next_refresh, scanned_pool.next_refresh);
        assert_eq!(fast_rng.random::<u64>(), scanned_rng.random::<u64>());
    }

    #[test]
    fn refresh_delay_is_symmetric_and_converges_to_the_interval() {
        use super::sample_refresh_delay;
        use rand::{SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(42);
        let interval = Duration::from_secs(10);
        let jitter = Duration::from_secs(4);
        let (mut below, mut above, mut sum) = (0u32, 0u32, 0.0_f64);
        let samples = 5_000u32;
        for _ in 0..samples {
            let delay = sample_refresh_delay(interval, jitter, &mut rng).as_secs_f64();
            assert!(delay >= interval.saturating_sub(jitter).as_secs_f64() - 1e-9);
            assert!(delay <= (interval + jitter).as_secs_f64() + 1e-9);
            if delay < interval.as_secs_f64() {
                below += 1;
            } else {
                above += 1;
            }
            sum += delay;
        }
        // Symmetric signed jitter yields delays on both sides of the interval...
        assert!(
            below > 0 && above > 0,
            "expected delays below and above the interval"
        );
        // ...and the mean converges to the interval, not `interval + jitter/2`.
        let mean = sum / f64::from(samples);
        assert!(
            (mean - interval.as_secs_f64()).abs() < 0.2,
            "mean {mean} should converge to the interval"
        );
    }

    #[test]
    fn refresh_delay_caps_jitter_and_stays_non_negative() {
        use super::sample_refresh_delay;
        use rand::{SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(7);
        let interval = Duration::from_secs(2);
        // Jitter beyond the interval is capped to it, so a delay can never be
        // negative and never exceeds `2 * interval`.
        let jitter = Duration::from_secs(100);
        for _ in 0..1_000 {
            let delay = sample_refresh_delay(interval, jitter, &mut rng);
            assert!(delay <= Duration::from_secs(4));
        }
    }

    struct CleanupFlag(Arc<AtomicBool>);

    impl Drop for CleanupFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn request_task_panic_aborts_non_terminating_tasks_and_runs_cleanup() {
        let mut tasks = RequestTasks::default();
        let cleaned_up = Arc::new(AtomicBool::new(false));

        tasks.spawn(87, async {
            panic!("synthetic request panic 87");
        });
        let cleaned_up_by_task = Arc::clone(&cleaned_up);
        tasks.spawn(89, async move {
            let _cleanup = CleanupFlag(cleaned_up_by_task);
            pending::<()>().await;
        });

        assert!(tasks.join_one().await);
        tokio::time::timeout(Duration::from_secs(1), tasks.drain())
            .await
            .expect("panic cleanup must not wait for a non-terminating request");

        let error = tasks
            .into_result()
            .expect_err("panic must fail the workload");
        let message = error.to_string();
        assert!(message.contains("1 workload request task(s) failed to join"));
        assert!(message.contains("workload request 87"));
        assert!(message.contains("synthetic request panic 87"));
        assert!(cleaned_up.load(Ordering::SeqCst));
    }

    #[test]
    fn reconciliation_treats_normal_failures_as_terminal_outcomes() {
        let outcomes = MetricsObservation {
            total: 3,
            completed: 1,
            rejected: 1,
            failed: 1,
            ..MetricsObservation::default()
        };

        reconcile_measured_outcomes(3, &outcomes).expect("all terminal outcomes reconcile");
    }

    #[test]
    fn reconciliation_rejects_missing_terminal_outcomes() {
        let outcomes = MetricsObservation {
            total: 2,
            completed: 1,
            rejected: 1,
            ..MetricsObservation::default()
        };

        let error = reconcile_measured_outcomes(3, &outcomes)
            .expect_err("one measured submission has no terminal outcome");
        assert!(error.to_string().contains("submitted=3, outcomes=2"));
    }
}
