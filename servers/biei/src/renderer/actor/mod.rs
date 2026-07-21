//! Dedicated blocking renderer actor for production MapLibre integration.
//!
//! MapLibre Native rendering is treated as thread-affine blocking work. This
//! actor owns the backend on one OS thread and exposes async request/reply
//! methods to worker tasks.

mod addlayer;
mod backend;
mod camera;
mod encode;
mod protocol;
mod supervisor;

pub(crate) use protocol::{
    BlockingRenderBackend, RenderTaskView, RendererActor, RendererActorConfig, ResolvedStyle,
};
pub(crate) use supervisor::RendererActorSupervisor;
#[cfg(test)]
pub(crate) use supervisor::RendererHealth;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use biei_core::types::{
        ImageFormat, Padding, PixelRatio, Positioning, RenderOutput, RenderRequest, RendererError,
        StyleId, StyleRevision,
    };
    use tokio::time::Instant;

    use super::*;
    use crate::renderer::RendererOutput;

    struct FakeBackend;

    impl BlockingRenderBackend for FakeBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    struct SlowBackend;

    impl BlockingRenderBackend for SlowBackend {
        fn load_profile(
            &mut self,
            _style: &ResolvedStyle,
            _task: &RenderTaskView,
        ) -> Result<(), RendererError> {
            Ok(())
        }

        fn render(&mut self, task: &RenderTaskView) -> Result<RendererOutput, RendererError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            Ok(RenderOutput {
                bytes: vec![task.id as u8].into(),
                format: task.output_format,
            }
            .into())
        }
    }

    fn resolved_style() -> ResolvedStyle {
        ResolvedStyle {
            revision: StyleRevision {
                id: StyleId("carto/voyager".to_string()),
                version: 1,
            },
            style_json: Arc::from(r#"{"version":8,"sources":{},"layers":[]}"#),
        }
    }

    fn task_view(style: StyleRevision) -> RenderTaskView {
        RenderTaskView {
            id: 7,
            style,
            request: RenderRequest::StaticImage {
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
                padding: Padding::default(),
                addlayer: None,
            },
            pixel_ratio: PixelRatio::X1,
            output_format: ImageFormat::Png,
            deadline: Instant::now() + std::time::Duration::from_secs(1),
        }
    }

    #[tokio::test]
    async fn abandoned_actor_threads_are_bounded_and_released_on_exit() {
        let supervisor = RendererActorSupervisor::new(1);
        let actor = RendererActor::spawn_with_backend_supervised(
            RendererActorConfig {
                worker_id: 17,
                ambient_cache_path: None,
            },
            supervisor.clone(),
            SlowBackend,
        )
        .expect("actor spawns");
        let style = resolved_style();
        let mut task = task_view(style.revision.clone());
        actor
            .load_profile(style, task.clone())
            .await
            .expect("profile loads");
        task.deadline = Instant::now() + std::time::Duration::from_millis(5);
        assert!(matches!(
            actor.render(task).await,
            Err(RendererError::Timeout)
        ));
        actor.retire_after_current();
        assert!(actor.try_abandon());
        assert_eq!(supervisor.snapshot().orphaned_threads, 1);

        let second = RendererActor::spawn_with_backend_supervised(
            RendererActorConfig {
                worker_id: 18,
                ambient_cache_path: None,
            },
            supervisor.clone(),
            FakeBackend,
        )
        .expect("second actor spawns");
        assert!(
            !second.try_abandon(),
            "orphan budget must prevent unbounded detached threads"
        );
        drop(second);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while supervisor.snapshot().orphaned_threads != 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(supervisor.snapshot().orphaned_threads, 0);
    }
}
