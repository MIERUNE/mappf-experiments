//! `StubRenderer` — sleep-based fake renderer driven by `CostRange` samples.

use std::sync::Mutex;

use async_trait::async_trait;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;

use biei::config::CostRange;
use biei::renderer::{PreparedProfile, Renderer};
use biei::types::{InternalTask, RenderOutput, RendererError, SourceHash};

pub struct StubRenderer {
    pub style_setup_cost: CostRange,
    pub source_load_cost: CostRange,
    pub render_cost: CostRange,
    rng: Mutex<Xoshiro256PlusPlus>,
}

impl StubRenderer {
    pub fn new(
        style_setup_cost: CostRange,
        source_load_cost: CostRange,
        render_cost: CostRange,
        seed: u64,
    ) -> Self {
        Self {
            style_setup_cost,
            source_load_cost,
            render_cost,
            rng: Mutex::new(Xoshiro256PlusPlus::seed_from_u64(seed)),
        }
    }

    fn sample(&self, range: &CostRange) -> std::time::Duration {
        let mut rng = self.rng.lock().expect("renderer rng mutex poisoned");
        range.sample(&mut *rng)
    }
}

#[async_trait]
impl Renderer for StubRenderer {
    async fn setup_profile(
        &mut self,
        _task: &InternalTask,
        _prepared: Option<PreparedProfile>,
    ) -> Result<(), RendererError> {
        let d = self.sample(&self.style_setup_cost);
        tokio::time::sleep(d).await;
        Ok(())
    }

    async fn ensure_source(&mut self, _hash: SourceHash) -> Result<(), RendererError> {
        let d = self.sample(&self.source_load_cost);
        tokio::time::sleep(d).await;
        Ok(())
    }

    async fn render(&mut self, task: &InternalTask) -> Result<RenderOutput, RendererError> {
        let d = self.sample(&self.render_cost);
        tokio::time::sleep(d).await;
        Ok(RenderOutput {
            bytes: bytes::Bytes::new(),
            format: task.output_format,
        })
    }
}
