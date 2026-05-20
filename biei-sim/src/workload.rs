//! Workload generator: Poisson per-tick task arrivals + style/source sampling
//! (Zipf, Burst, PeriodicRefresh source pools, etc.).

use std::sync::Arc;
use std::time::Duration;

use rand::distr::{Distribution, weighted::WeightedIndex};
use rand::{Rng, RngExt, SeedableRng};
use rand_distr::{Exp, Poisson, Zipf};
use rand_xoshiro::Xoshiro256PlusPlus;
use tokio::task::JoinSet;
use tokio::time::{Instant, sleep};

use crate::config::{
    BurstPattern, SourcePattern, SourceProvider, StyleDist, StyleShift, WorkloadConfig,
};
use crate::metrics::MetricsCollector;
use biei::activity::ProfileActivityTracker;
use biei::node::Node;
use biei::types::{
    CachePolicy, ImageFormat, InternalTask, PixelRatio, Positioning, RenderRequest, Scale,
    SourceHash, SourceRef, StyleId, StyleRevision,
};

pub async fn run_workload(
    config: WorkloadConfig,
    nodes: Vec<Node>,
    metrics: Arc<MetricsCollector>,
    activity: Arc<ProfileActivityTracker>,
    seed: u64,
) {
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(seed);
    let start = Instant::now();
    let deadline = start + config.duration;
    let record_after = start + config.warmup;
    let mut next_id: u64 = 0;
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

    let mut inflight: JoinSet<()> = JoinSet::new();

    // Tokio's paused-time sleep has coarse granularity (~2-3ms on observed
    // setups), so Exp-distributed sub-millisecond interarrivals get clipped
    // at high rates. Instead we tick periodically and Poisson-sample arrivals
    // scaled by the *actual* elapsed since the last tick. This keeps the
    // configured rate honored regardless of how the runtime rounds sleeps.
    let tick = Duration::from_millis(1);
    let mut last_tick = start;

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        sleep(tick).await;

        let now = Instant::now();
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

        while let Some(t) = next_new_style_at {
            if t <= now {
                style_count += 1;
                let dt = Exp::new(config.new_style_rate)
                    .expect("rate")
                    .sample(&mut rng);
                next_new_style_at = Some(now + Duration::from_secs_f64(dt));
            } else {
                break;
            }
        }

        for _ in 0..n_tasks {
            let style = sample_style(
                now,
                start,
                style_count,
                &config.style_distribution,
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
            // Deadline is forward-looking; the simulator does not yet enforce
            // it. 段 5/6 will wire it into CostConfig.sla.
            let task = InternalTask {
                id: next_id,
                request_id: biei::types::RequestId::new_random(),
                style: style_revision,
                source,
                request,
                pixel_ratio,
                output_format: ImageFormat::Png,
                arrived_at: now,
                deadline: now + Duration::from_secs(30),
                forwarding_hops: 0,
            };
            activity.record(task.worker_profile(), now);
            next_id += 1;

            let node_idx = rng.random_range(0..nodes.len());
            let node = nodes[node_idx].clone();
            let m = metrics.clone();
            let task_arrived_at = task.arrived_at;
            inflight.spawn(async move {
                let outcome = node.handle_incoming(task).await;
                if task_arrived_at >= record_after {
                    m.record(outcome);
                }
            });
        }
    }

    while inflight.join_next().await.is_some() {}
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
            padding: biei::types::Padding::default(),
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

fn sample_style(
    now: Instant,
    start: Instant,
    style_count: usize,
    dist: &StyleDist,
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
    let id = sample_from_dist(dist, style_count, rng) as u32;
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

fn sample_from_dist(dist: &StyleDist, n: usize, rng: &mut impl Rng) -> usize {
    let n = n.max(1);
    match dist {
        StyleDist::Uniform => rng.random_range(0..n),
        StyleDist::Zipf { alpha } => {
            let z = Zipf::new(n as f64, *alpha).expect("Zipf requires alpha > 0 and n >= 1");
            let v: f64 = z.sample(rng);
            (v as usize).saturating_sub(1).min(n - 1)
        }
        StyleDist::Custom(weights) => {
            let effective = if weights.len() >= n {
                &weights[..n]
            } else {
                weights.as_slice()
            };
            let d = WeightedIndex::new(effective).expect("non-empty positive weights");
            d.sample(rng)
        }
    }
}

/// State held across calls to satisfy `PeriodicRefresh` (one hash pool per
/// occurrence of that provider in the pattern). Keyed by a deterministic
/// path in the pattern tree.
struct SourceGenState {
    /// Pools keyed by provider path. Most provider trees only have one
    /// `PeriodicRefresh` so this is usually 0 or 1 entries.
    periodic_pools: Vec<PeriodicPool>,
    oneshot_counter: u64,
}

struct PeriodicPool {
    path: Vec<usize>,
    pool: Vec<SourceHash>,
    next_refresh: Vec<Instant>,
}

impl SourceGenState {
    fn init(pattern: Option<&SourcePattern>, start: Instant, rng: &mut impl Rng) -> Self {
        let mut pools = Vec::new();
        if let Some(p) = pattern {
            walk_for_init(&p.provider, &mut Vec::new(), &mut pools, start, rng);
        }
        Self {
            periodic_pools: pools,
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
        self.sample_from_provider(&pattern.provider, &mut Vec::new(), now, rng)
    }

    fn sample_from_provider(
        &mut self,
        provider: &SourceProvider,
        path: &mut Vec<usize>,
        now: Instant,
        rng: &mut impl Rng,
    ) -> Option<SourceRef> {
        match provider {
            SourceProvider::Shared {
                source_count,
                distribution,
            } => {
                let idx = sample_from_dist(distribution, *source_count, rng);
                let hash = path_namespaced(path, idx as u64);
                Some(SourceRef {
                    hash,
                    policy: CachePolicy::Cacheable,
                })
            }
            SourceProvider::PeriodicRefresh {
                interval, jitter, ..
            } => {
                let pool_idx = self.periodic_pools.iter().position(|p| p.path == *path)?;
                refresh_pool(
                    &mut self.periodic_pools[pool_idx],
                    *interval,
                    *jitter,
                    now,
                    rng,
                );
                let pool = &self.periodic_pools[pool_idx].pool;
                if pool.is_empty() {
                    return None;
                }
                let pick = rng.random_range(0..pool.len());
                Some(SourceRef {
                    hash: pool[pick],
                    policy: CachePolicy::Cacheable,
                })
            }
            SourceProvider::OneShot => {
                self.oneshot_counter = self.oneshot_counter.wrapping_add(1);
                Some(SourceRef {
                    hash: self.oneshot_counter | (1u64 << 63),
                    policy: CachePolicy::OneShot,
                })
            }
            SourceProvider::Mixed(choices) => {
                if choices.is_empty() {
                    return None;
                }
                let weights: Vec<f64> = choices.iter().map(|(w, _)| *w).collect();
                let d = WeightedIndex::new(&weights).expect("mixed weights must be positive");
                let pick = d.sample(rng);
                path.push(pick);
                let result = self.sample_from_provider(&choices[pick].1, path, now, rng);
                path.pop();
                result
            }
        }
    }
}

fn walk_for_init(
    provider: &SourceProvider,
    path: &mut Vec<usize>,
    pools: &mut Vec<PeriodicPool>,
    start: Instant,
    rng: &mut impl Rng,
) {
    match provider {
        SourceProvider::PeriodicRefresh {
            source_count,
            interval,
            jitter,
        } => {
            let mut pool = Vec::with_capacity(*source_count);
            let mut next = Vec::with_capacity(*source_count);
            for _ in 0..*source_count {
                pool.push(rng.random::<u64>() & !(1u64 << 63));
                let j = if jitter.as_secs_f64() > 0.0 {
                    rng.random_range(0.0..jitter.as_secs_f64())
                } else {
                    0.0
                };
                next.push(start + *interval + Duration::from_secs_f64(j));
            }
            pools.push(PeriodicPool {
                path: path.clone(),
                pool,
                next_refresh: next,
            });
        }
        SourceProvider::Mixed(choices) => {
            for (i, (_, child)) in choices.iter().enumerate() {
                path.push(i);
                walk_for_init(child, path, pools, start, rng);
                path.pop();
            }
        }
        _ => {}
    }
}

fn refresh_pool(
    pool_state: &mut PeriodicPool,
    interval: Duration,
    jitter: Duration,
    now: Instant,
    rng: &mut impl Rng,
) {
    for i in 0..pool_state.pool.len() {
        if now >= pool_state.next_refresh[i] {
            pool_state.pool[i] = rng.random::<u64>() & !(1u64 << 63);
            let j = if jitter.as_secs_f64() > 0.0 {
                rng.random_range(0.0..jitter.as_secs_f64())
            } else {
                0.0
            };
            pool_state.next_refresh[i] = now + interval + Duration::from_secs_f64(j);
        }
    }
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
