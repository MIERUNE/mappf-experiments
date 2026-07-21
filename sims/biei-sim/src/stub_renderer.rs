//! `StubRenderer` — sleep-based fake renderer driven by `CostRange` samples.

use std::sync::Arc;

use async_trait::async_trait;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;
use tokio::sync::Semaphore;

use biei_core::config::CostRange;
use biei_core::renderer::{PreparedProfile, Renderer, RendererOutput};
use biei_core::types::{InternalTask, RenderMode, RenderOutput, RendererError, Scale, SourceHash};

use crate::calibrated_costs::{CalibrationRenderState, EmpiricalCostModel};

pub struct StubRenderer {
    pub style_setup_cost: CostRange,
    pub source_load_cost: CostRange,
    pub render_resource_cost: CostRange,
    pub first_render_resource_cost: CostRange,
    pub render_cpu_cost: CostRange,
    cpu_cores: Arc<Semaphore>,
    calibration: Option<Arc<EmpiricalCostModel>>,
    has_profile: bool,
    first_render_after_setup: bool,
    next_render_state: CalibrationRenderState,
    current_render_mode: Option<RenderMode>,
    current_scale: Option<Scale>,
    rng: Xoshiro256PlusPlus,
}

impl StubRenderer {
    pub fn new(
        style_setup_cost: CostRange,
        source_load_cost: CostRange,
        render_resource_cost: CostRange,
        first_render_resource_cost: CostRange,
        render_cpu_cost: CostRange,
        cpu_cores: Arc<Semaphore>,
        seed: u64,
    ) -> Self {
        Self {
            style_setup_cost,
            source_load_cost,
            render_resource_cost,
            first_render_resource_cost,
            render_cpu_cost,
            cpu_cores,
            calibration: None,
            has_profile: false,
            first_render_after_setup: true,
            next_render_state: CalibrationRenderState::Cold,
            current_render_mode: None,
            current_scale: None,
            rng: Xoshiro256PlusPlus::seed_from_u64(seed),
        }
    }

    pub fn with_calibration_model(mut self, model: Option<Arc<EmpiricalCostModel>>) -> Self {
        self.calibration = model;
        self
    }

    fn sample_style_setup(
        &mut self,
        task: &InternalTask,
        state: CalibrationRenderState,
    ) -> std::time::Duration {
        if let Some(model) = &self.calibration
            && let Some(sample) = model.sample_style_setup(task, state, &mut self.rng)
        {
            return sample;
        }
        self.style_setup_cost.sample(&mut self.rng)
    }

    fn sample_source_setup(&mut self) -> std::time::Duration {
        if let (Some(model), Some(render_mode), Some(scale)) = (
            &self.calibration,
            self.current_render_mode,
            self.current_scale,
        ) && let Some(sample) = model.sample_source_setup(render_mode, scale, &mut self.rng)
        {
            return sample;
        }
        self.source_load_cost.sample(&mut self.rng)
    }

    fn sample_render_costs(
        &mut self,
        task: Option<&InternalTask>,
        state: CalibrationRenderState,
        first_render: bool,
    ) -> (std::time::Duration, std::time::Duration) {
        let sampled_cpu = if let (Some(task), Some(model)) = (task, &self.calibration) {
            model.sample_render_cpu(task, &mut self.rng)
        } else {
            None
        }
        .unwrap_or_else(|| self.render_cpu_cost.sample(&mut self.rng));
        let empirical_total = if let (Some(task), Some(model)) = (task, &self.calibration) {
            model.sample_render(task, state, &mut self.rng)
        } else {
            None
        };
        if let Some(total) = empirical_total {
            let cpu = sampled_cpu.min(total);
            return (total.saturating_sub(cpu), cpu);
        }
        let resource_range = if first_render {
            self.first_render_resource_cost
        } else {
            self.render_resource_cost
        };
        (resource_range.sample(&mut self.rng), sampled_cpu)
    }

    async fn spend_render_phases(
        &self,
        resource_wait: std::time::Duration,
        cpu_cost: std::time::Duration,
    ) {
        if !resource_wait.is_zero() {
            tokio::time::sleep(resource_wait).await;
        }

        // The native-render permit bounds whole render residency, including
        // the wait above. This separate semaphore represents actual cores,
        // which the resource wait must not consume.
        let _cpu_core = self
            .cpu_cores
            .acquire()
            .await
            .expect("simulated CPU core semaphore is never closed");
        if !cpu_cost.is_zero() {
            tokio::time::sleep(cpu_cost).await;
        }
    }

    #[cfg(test)]
    async fn spend_render_cost(&mut self) {
        let first_render = std::mem::take(&mut self.first_render_after_setup);
        let state = if first_render {
            self.next_render_state
        } else {
            CalibrationRenderState::Warm
        };
        let (resource_wait, cpu_cost) = self.sample_render_costs(None, state, first_render);
        self.spend_render_phases(resource_wait, cpu_cost).await;
    }
}

#[async_trait]
impl Renderer for StubRenderer {
    async fn setup_profile(
        &mut self,
        task: &InternalTask,
        _prepared: Option<PreparedProfile>,
    ) -> Result<(), RendererError> {
        let state = if self.has_profile {
            CalibrationRenderState::Swap
        } else {
            CalibrationRenderState::Cold
        };
        let d = self.sample_style_setup(task, state);
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
        self.has_profile = true;
        self.first_render_after_setup = true;
        self.next_render_state = state;
        self.current_render_mode = Some(task.request.render_mode());
        self.current_scale = Some(task.pixel_ratio.to_scale());
        Ok(())
    }

    async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
        let d = self.sample_source_setup();
        if !d.is_zero() {
            tokio::time::sleep(d).await;
        }
        Ok(())
    }

    async fn render(&mut self, task: &InternalTask) -> Result<RendererOutput, RendererError> {
        let first_render = std::mem::take(&mut self.first_render_after_setup);
        let state = if first_render {
            self.next_render_state
        } else {
            CalibrationRenderState::Warm
        };
        let (resource_wait, cpu_cost) = self.sample_render_costs(Some(task), state, first_render);
        self.spend_render_phases(resource_wait, cpu_cost).await;
        Ok(RenderOutput {
            bytes: bytes::Bytes::new(),
            format: task.output_format,
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use biei_core::config::CostRange;
    use tokio::sync::Semaphore;
    use tokio::time::Instant;

    use super::StubRenderer;

    fn renderer(
        warm_resource: Duration,
        first_resource: Duration,
        cpu: Duration,
        cpu_cores: Arc<Semaphore>,
        seed: u64,
    ) -> StubRenderer {
        StubRenderer::new(
            CostRange::fixed(Duration::ZERO),
            CostRange::fixed(Duration::ZERO),
            CostRange::fixed(warm_resource),
            CostRange::fixed(first_resource),
            CostRange::fixed(cpu),
            cpu_cores,
            seed,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn first_render_resource_wait_is_distinct_from_warm_render() {
        let mut renderer = renderer(
            Duration::from_millis(20),
            Duration::from_millis(100),
            Duration::from_millis(10),
            Arc::new(Semaphore::new(1)),
            1,
        );

        let started = Instant::now();
        renderer.spend_render_cost().await;
        assert_eq!(started.elapsed(), Duration::from_millis(110));

        let started = Instant::now();
        renderer.spend_render_cost().await;
        assert_eq!(started.elapsed(), Duration::from_millis(30));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_cost_render_still_acquires_the_modeled_cpu_core() {
        let cpu_cores = Arc::new(Semaphore::new(0));
        let mut renderer = renderer(
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            cpu_cores.clone(),
            1,
        );

        let render = tokio::spawn(async move {
            renderer.spend_render_cost().await;
        });
        tokio::task::yield_now().await;
        assert!(!render.is_finished());

        cpu_cores.add_permits(1);
        render.await.expect("zero-cost render task");
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_resource_waits_do_not_consume_the_modeled_cpu_core() {
        let cpu_cores = Arc::new(Semaphore::new(1));
        let mut first = renderer(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(20),
            cpu_cores.clone(),
            1,
        );
        let mut second = renderer(
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(20),
            cpu_cores,
            2,
        );

        let started = Instant::now();
        tokio::join!(first.spend_render_cost(), second.spend_render_cost());

        // Both 100 ms resource waits overlap. Only the two 20 ms CPU phases
        // serialize on the single modeled core.
        assert_eq!(started.elapsed(), Duration::from_millis(140));
    }
}
