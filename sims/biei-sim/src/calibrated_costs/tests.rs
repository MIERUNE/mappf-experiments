use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::Result;
use rand::SeedableRng;
use rand_xoshiro::Xoshiro256PlusPlus;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::*;
use crate::calibration::{
    CALIBRATION_PROFILE_KIND, CALIBRATION_PROFILE_SCHEMA_VERSION, CalibrationBucket,
    CalibrationCollection, CalibrationHistogram, CalibrationProfile, CalibrationProvenance,
    CalibrationSeries,
};
use crate::config::SimConfig;
use crate::stub_renderer::StubRenderer;
use biei_core::config::{CostConfig, CostRange};
use biei_core::renderer::Renderer;
use biei_core::types::{ImageFormat, InternalTask, RenderMode, RenderRequest, Scale};

fn series(state: &str, render_mode: &str, buckets: &[(Option<f64>, f64)]) -> CalibrationSeries {
    let sample_count = buckets.iter().map(|(_, count)| count).sum();
    CalibrationSeries {
        labels: BTreeMap::from([
            ("state".to_owned(), state.to_owned()),
            ("render_mode".to_owned(), render_mode.to_owned()),
            ("scale".to_owned(), "1x".to_owned()),
            ("format".to_owned(), "png".to_owned()),
            ("size".to_owned(), "le_512px".to_owned()),
        ]),
        sample_count,
        buckets: buckets
            .iter()
            .map(|&(upper_bound_seconds, count)| CalibrationBucket {
                upper_bound_seconds,
                count,
            })
            .collect(),
    }
}

fn profile(mut histograms: Vec<CalibrationHistogram>) -> CalibrationProfile {
    if !histograms
        .iter()
        .any(|histogram| histogram.metric == "biei_render_timeout_lower_bound_seconds")
    {
        histograms.push(CalibrationHistogram {
            metric: "biei_render_timeout_lower_bound_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![CalibrationSeries {
                labels: BTreeMap::new(),
                sample_count: 0.0,
                buckets: vec![
                    CalibrationBucket {
                        upper_bound_seconds: Some(10.0),
                        count: 0.0,
                    },
                    CalibrationBucket {
                        upper_bound_seconds: None,
                        count: 0.0,
                    },
                ],
            }],
        });
    }
    CalibrationProfile {
        schema_version: CALIBRATION_PROFILE_SCHEMA_VERSION,
        kind: CALIBRATION_PROFILE_KIND.to_owned(),
        exporter: "test".to_owned(),
        exporter_version: "0".to_owned(),
        exported_at_unix_seconds: 0,
        collection: CalibrationCollection {
            prometheus_url: "http://prometheus.test/api/v1/query".to_owned(),
            start_unix_seconds: 0,
            end_unix_seconds: 900,
            window_seconds: 900,
            evaluation_unix_seconds: 900,
            match_labels: BTreeMap::new(),
        },
        provenance: CalibrationProvenance {
            deployment_revision: "test".to_owned(),
            architecture: "arm64".to_owned(),
            hardware_profile: "m1-pro metal".to_owned(),
            cpu_cores_per_node: 2,
            renderer_slots_per_node: 3,
            execution_permits_per_node: 2,
            native_render_permits_per_node: 2,
            capture_concurrency: Some(1),
            notes: None,
        },
        histograms,
        warnings: Vec::new(),
    }
}

/// Upstream fetch-attempt histogram: `attempts` regular-lane attempts plus
/// a low-priority refresh series that warmth verification must ignore.
fn upstream_histogram(attempts: f64) -> CalibrationHistogram {
    let lane = |priority: &str, count: f64| CalibrationSeries {
        labels: BTreeMap::from([
            ("kind".to_owned(), "tile".to_owned()),
            ("priority".to_owned(), priority.to_owned()),
        ]),
        sample_count: count,
        buckets: vec![
            CalibrationBucket {
                upper_bound_seconds: Some(0.1),
                count,
            },
            CalibrationBucket {
                upper_bound_seconds: None,
                count: 0.0,
            },
        ],
    };
    CalibrationHistogram {
        metric: "mmpf_mln_resource_upstream_attempt_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![lane("regular", attempts), lane("low", 500.0)],
    }
}

fn render_histogram() -> CalibrationHistogram {
    CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![
            // Two warm series merge: 30 in (0, 0.05], 45 in (0.05, 0.2],
            // 25 in (0.2, 0.5].
            series(
                "warm",
                "tile",
                &[
                    (Some(0.05), 20.0),
                    (Some(0.2), 40.0),
                    (Some(0.5), 20.0),
                    (None, 0.0),
                ],
            ),
            series(
                "warm",
                "static",
                &[
                    (Some(0.05), 10.0),
                    (Some(0.2), 5.0),
                    (Some(0.5), 5.0),
                    (None, 0.0),
                ],
            ),
            series(
                "cold",
                "tile",
                &[(Some(0.5), 5.0), (Some(2.0), 5.0), (None, 0.0)],
            ),
            series(
                "swap",
                "tile",
                &[(Some(0.5), 2.0), (Some(2.0), 2.0), (None, 0.0)],
            ),
        ],
    }
}

fn base_costs() -> CostConfig {
    CostConfig {
        style_setup_cost: CostRange::new(Duration::from_millis(200), Duration::from_millis(300)),
        source_load_cost: CostRange::new(Duration::from_millis(30), Duration::from_millis(70)),
        render_cpu_cost: CostRange::new(Duration::from_millis(5), Duration::from_millis(30)),
        render_resource_cost: CostRange::new(Duration::from_millis(1), Duration::from_millis(10)),
        first_render_resource_cost: CostRange::new(
            Duration::from_millis(50),
            Duration::from_millis(400),
        ),
        hop_latency: Duration::from_millis(5),
        sla: Duration::from_millis(1_000),
    }
}

/// Exercise independent stage fallback behavior without reintroducing a
/// public single-window calibration path. Production always supplies this
/// tuple from a verified CPU-reference profile.
fn derive_with_base_cpu_for_test(
    profile: &CalibrationProfile,
    base: &CostConfig,
) -> Result<CalibratedCosts> {
    ensure_uncensored_render_tail(profile, "test profile")?;
    derive_with_cpu_reference(
        profile,
        base,
        (
            base.render_cpu_cost,
            base.render_cpu_cost.mid(),
            CalibrationStageCoverage::default(),
        ),
        Vec::new(),
    )
}

fn tile_task(scale: Scale, format: ImageFormat, tile_size: u16) -> InternalTask {
    InternalTask {
        id: 1,
        request_id: biei_core::types::RequestId::from_string("calibration-test"),
        style: biei_core::types::StyleRevision {
            id: biei_core::types::StyleId("style-0".to_owned()),
            version: 1,
        },
        source: None,
        request: RenderRequest::Tile {
            z: 10,
            x: 0,
            y: 0,
            tile_size,
        },
        pixel_ratio: scale.into(),
        output_format: format,
        arrived_at: Instant::now(),
        deadline: Instant::now() + Duration::from_secs(1),
        forwarding_hops: 0,
    }
}

fn shaped_series(
    labels: &[(&str, &str)],
    upper_bound_seconds: f64,
    count: f64,
) -> CalibrationSeries {
    CalibrationSeries {
        labels: labels
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        sample_count: count,
        buckets: vec![
            CalibrationBucket {
                upper_bound_seconds: Some(upper_bound_seconds),
                count,
            },
            CalibrationBucket {
                upper_bound_seconds: None,
                count: 0.0,
            },
        ],
    }
}

#[test]
fn derives_costs_with_documented_bands_and_fallbacks() {
    let profile = profile(vec![render_histogram()]);
    let base = base_costs();

    let derived = derive_with_base_cpu_for_test(&profile, &base).expect("derive");

    // CPU comes only from a separate verified reference. This test helper
    // intentionally supplies the base range while testing the other stages.
    assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
    assert_eq!(derived.costs.render_cpu_cost.max, base.render_cpu_cost.max);
    // Cold+swap walls reach 2s, so the first-render resource wait must
    // exceed the warm resource wait.
    assert!(derived.costs.first_render_resource_cost.max > derived.costs.render_resource_cost.max);
    // No setup histograms in the profile: base ranges with notes.
    assert_eq!(
        derived.costs.style_setup_cost.min,
        base.style_setup_cost.min
    );
    assert_eq!(
        derived.costs.style_setup_cost.max,
        base.style_setup_cost.max
    );
    assert_eq!(
        derived.costs.source_load_cost.min,
        base.source_load_cost.min
    );
    assert_eq!(
        derived.costs.source_load_cost.max,
        base.source_load_cost.max
    );
    assert!(
        derived
            .notes
            .iter()
            .any(|note| note.contains("style_setup kept from base"))
    );
    assert!(
        derived
            .notes
            .iter()
            .any(|note| note.contains("collapsed 2 bounded render shapes"))
    );
    // hop latency and SLA always come from the base config.
    assert_eq!(derived.costs.hop_latency, base.hop_latency);
    assert_eq!(derived.costs.sla, base.sla);
}

#[test]
fn calibration_rejects_right_censored_render_timeouts() {
    let timeout_histogram = CalibrationHistogram {
        metric: "biei_render_timeout_lower_bound_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![CalibrationSeries {
            labels: BTreeMap::new(),
            sample_count: 1.0,
            buckets: vec![
                CalibrationBucket {
                    upper_bound_seconds: Some(5.0),
                    count: 1.0,
                },
                CalibrationBucket {
                    upper_bound_seconds: None,
                    count: 0.0,
                },
            ],
        }],
    };
    let profile = profile(vec![render_histogram(), timeout_histogram]);

    let error = match derive_with_base_cpu_for_test(&profile, &base_costs()) {
        Ok(_) => panic!("successful render samples are incomplete when a timeout was censored"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("right-censored"));
}

#[test]
fn calibration_rejects_empty_render_timeout_instrumentation() {
    let timeout_histogram = CalibrationHistogram {
        metric: "biei_render_timeout_lower_bound_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: Vec::new(),
    };
    let profile = profile(vec![render_histogram(), timeout_histogram]);

    let error = match derive_with_base_cpu_for_test(&profile, &base_costs()) {
        Ok(_) => panic!("an empty family cannot prove that the timeout count was zero"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("missing instrumentation"));
}

#[test]
fn empirical_render_sampling_prefers_exact_shape_then_state_fallback() {
    let render = CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![
            shaped_series(
                &[
                    ("render_mode", "tile"),
                    ("scale", "2x"),
                    ("format", "png"),
                    ("size", "le_1024px"),
                    ("state", "warm"),
                ],
                0.01,
                10.0,
            ),
            shaped_series(
                &[
                    ("render_mode", "static"),
                    ("scale", "1x"),
                    ("format", "webp"),
                    ("size", "le_512px"),
                    ("state", "warm"),
                ],
                1.0,
                20.0,
            ),
        ],
    };
    let model = EmpiricalCostModel::from_profile(&profile(vec![render]));
    let coverage = model.coverage();
    assert_eq!(coverage.render_exact_shapes, 2);
    assert_eq!(coverage.render_state_fallbacks, 1);
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(7);

    let exact = model
        .sample_render(
            &tile_task(Scale::X2, ImageFormat::Png, 512),
            CalibrationRenderState::Warm,
            &mut rng,
        )
        .expect("exact shape sample");
    assert!(exact <= Duration::from_millis(10));

    let fallback = model.sample_render(
        &tile_task(Scale::X1, ImageFormat::Jpeg, 256),
        CalibrationRenderState::Warm,
        &mut rng,
    );
    assert!(fallback.is_some(), "30 warm samples form state fallback");
}

#[test]
fn empirical_setup_sampling_uses_available_partial_families() {
    let style = CalibrationHistogram {
        metric: "biei_style_setup_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![shaped_series(
            &[("render_mode", "tile"), ("scale", "2x"), ("state", "cold")],
            0.2,
            10.0,
        )],
    };
    let source = CalibrationHistogram {
        metric: "biei_source_setup_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![shaped_series(
            &[("render_mode", "tile"), ("scale", "2x")],
            0.05,
            10.0,
        )],
    };
    let model = EmpiricalCostModel::from_profile(&profile(vec![style, source]));
    let mut rng = Xoshiro256PlusPlus::seed_from_u64(11);
    let task = tile_task(Scale::X2, ImageFormat::Png, 512);

    assert!(
        model
            .sample_style_setup(&task, CalibrationRenderState::Cold, &mut rng)
            .is_some()
    );
    assert!(
        model
            .sample_source_setup(RenderMode::Tile, Scale::X2, &mut rng)
            .is_some()
    );
    assert!(
        model
            .sample_render(&task, CalibrationRenderState::Warm, &mut rng)
            .is_none()
    );
}

#[tokio::test(start_paused = true)]
async fn stub_renderer_uses_shape_conditioned_render_distribution() {
    let task = tile_task(Scale::X2, ImageFormat::Png, 512);
    let render = CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![shaped_series(
            &[
                ("render_mode", "tile"),
                ("scale", "2x"),
                ("format", "png"),
                ("size", "le_1024px"),
                ("state", "cold"),
            ],
            0.1,
            10.0,
        )],
    };
    let model = EmpiricalCostModel::from_profile(&profile(vec![render]));
    let mut renderer = StubRenderer::new(
        CostRange::fixed(Duration::ZERO),
        CostRange::fixed(Duration::ZERO),
        CostRange::fixed(Duration::from_secs(5)),
        CostRange::fixed(Duration::from_secs(5)),
        CostRange::fixed(Duration::ZERO),
        std::sync::Arc::new(Semaphore::new(1)),
        19,
    )
    .with_calibration_model(Some(std::sync::Arc::new(model)));

    renderer
        .setup_profile(&task, None)
        .await
        .expect("setup profile");
    let started = Instant::now();
    renderer.render(&task).await.expect("render");

    assert!(started.elapsed() <= Duration::from_millis(100));
}

#[test]
fn two_window_fusion_takes_cpu_from_reference_and_walls_from_traffic() {
    // Reference: verified-warm (0 regular fetches), walls all in (0, 0.05].
    let reference_render = CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![
            series("warm", "tile", &[(Some(0.05), 80.0), (None, 0.0)]),
            series("warm", "static", &[(Some(0.05), 20.0), (None, 0.0)]),
        ],
    };
    let reference = profile(vec![reference_render, upstream_histogram(0.0)]);
    // Traffic: heavily I/O-contaminated (400 fetches / 114 renders) —
    // rejected alone, but valid as the resource-wait source in fusion.
    let traffic = profile(vec![render_histogram(), upstream_histogram(400.0)]);
    let base = base_costs();

    let derived = derive_costs_with_cpu_reference(&traffic, &reference, &base).expect("fusion");

    // CPU comes from the reference window's representative band (≤ 50ms),
    // not the traffic window's contaminated walls.
    assert!(derived.costs.render_cpu_cost.max <= Duration::from_millis(50));
    // Resource waits reflect traffic walls (up to 0.5s warm / 2s first)
    // minus the small reference CPU.
    assert!(derived.costs.render_resource_cost.max >= Duration::from_millis(300));
    assert!(derived.costs.first_render_resource_cost.max > derived.costs.render_resource_cost.max);
    assert!(
        derived
            .notes
            .iter()
            .any(|note| note.contains("verified resource-warm, shape-conditioned"))
    );
    assert_eq!(derived.sampling_model.coverage().render_cpu_exact_shapes, 2);
    assert!(
        derived
            .notes
            .iter()
            .any(|note| note.contains("traffic window saw"))
    );
}

#[test]
fn fusion_rejects_unverified_reference_and_hardware_mismatch() {
    let clean_render = CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![
            series("warm", "tile", &[(Some(0.05), 80.0), (None, 0.0)]),
            series("warm", "static", &[(Some(0.05), 20.0), (None, 0.0)]),
        ],
    };
    let traffic = profile(vec![render_histogram(), upstream_histogram(400.0)]);
    let base = base_costs();

    let mut concurrent_reference = profile(vec![clean_render.clone(), upstream_histogram(0.0)]);
    concurrent_reference.provenance.capture_concurrency = Some(4);
    let Err(err) = derive_costs_with_cpu_reference(&traffic, &concurrent_reference, &base) else {
        panic!("a contended service-wall reference must not become CPU demand");
    };
    assert!(err.to_string().contains("capture_concurrency=1"));

    // Reference without upstream evidence cannot prove warmth.
    let unverified = profile(vec![clean_render.clone()]);
    assert!(derive_costs_with_cpu_reference(&traffic, &unverified, &base).is_err());

    // An exporter snapshot contains optional histogram entries even when
    // Prometheus returned no series. That is absence of evidence, not a
    // verified zero-fetch window.
    let empty_upstream = CalibrationHistogram {
        metric: "mmpf_mln_resource_upstream_attempt_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: Vec::new(),
    };
    let empty_evidence = profile(vec![clean_render.clone(), empty_upstream]);
    let Err(err) = derive_costs_with_cpu_reference(&traffic, &empty_evidence, &base) else {
        panic!("empty upstream evidence must fail");
    };
    assert!(err.to_string().contains("empty upstream fetch histogram"));

    // Reference with render-blocking fetches is not a CPU measurement.
    let contaminated = profile(vec![clean_render.clone(), upstream_histogram(50.0)]);
    assert!(derive_costs_with_cpu_reference(&traffic, &contaminated, &base).is_err());

    // Reference from different hardware must not be subtracted.
    let mut foreign = profile(vec![clean_render, upstream_histogram(0.0)]);
    foreign.provenance.hardware_profile = "different-machine".to_owned();
    let Err(err) = derive_costs_with_cpu_reference(&traffic, &foreign, &base) else {
        panic!("hardware mismatch must fail");
    };
    assert!(err.to_string().contains("different machines"));

    // Same hardware is insufficient when a different renderer revision or
    // concurrency shape produced the service-wall reference.
    let verified = profile(vec![
        CalibrationHistogram {
            metric: "biei_render_duration_seconds".to_owned(),
            unit: "seconds".to_owned(),
            query: "test".to_owned(),
            series: vec![series("warm", "tile", &[(Some(0.05), 100.0), (None, 0.0)])],
        },
        upstream_histogram(0.0),
    ]);
    let mut foreign_revision = verified.clone();
    foreign_revision.provenance.deployment_revision = "other-revision".to_owned();
    let Err(err) = derive_costs_with_cpu_reference(&traffic, &foreign_revision, &base) else {
        panic!("renderer revision mismatch must fail");
    };
    assert!(err.to_string().contains("renderer revisions"));

    let mut foreign_shape = verified;
    foreign_shape.provenance.native_render_permits_per_node = 1;
    let Err(err) = derive_costs_with_cpu_reference(&traffic, &foreign_shape, &base) else {
        panic!("node-shape mismatch must fail");
    };
    assert!(err.to_string().contains("node shape"));
}

#[test]
fn fusion_rejects_cross_shape_cpu_subtraction() {
    let reference_render = CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![shaped_series(
            &[
                ("render_mode", "tile"),
                ("scale", "1x"),
                ("format", "png"),
                ("size", "le_256px"),
                ("state", "warm"),
            ],
            0.02,
            100.0,
        )],
    };
    let traffic_render = CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![shaped_series(
            &[
                ("render_mode", "static"),
                ("scale", "2x"),
                ("format", "webp"),
                ("size", "le_2048px"),
                ("state", "warm"),
            ],
            0.5,
            100.0,
        )],
    };
    let reference = profile(vec![reference_render, upstream_histogram(0.0)]);
    let traffic = profile(vec![traffic_render, upstream_histogram(100.0)]);

    let Err(error) = derive_costs_with_cpu_reference(&traffic, &reference, &base_costs()) else {
        panic!("a CPU profile for another render shape is not calibration evidence");
    };
    assert!(
        error
            .to_string()
            .contains("does not cover traffic render shape")
    );
}

#[test]
fn sparse_warm_evidence_falls_back_without_discarding_the_profile() {
    let sparse = profile(vec![CalibrationHistogram {
        metric: "biei_render_duration_seconds".to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![series("warm", "tile", &[(Some(0.1), 5.0), (None, 0.0)])],
    }]);

    let base = base_costs();
    let derived = derive_with_base_cpu_for_test(&sparse, &base).expect("partial profile derives");
    assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
    assert_eq!(
        derived.costs.render_resource_cost.max,
        base.render_resource_cost.max
    );
    assert!(matches!(
        derived.coverage.render_cpu.source,
        CalibrationValueSource::Default
    ));
    assert!(derived.notes.iter().any(|note| note.contains("5 samples")));
}

#[test]
fn setup_only_profile_calibrates_available_stages() {
    let setup = |metric: &str, count: f64| CalibrationHistogram {
        metric: metric.to_owned(),
        unit: "seconds".to_owned(),
        query: "test".to_owned(),
        series: vec![CalibrationSeries {
            labels: BTreeMap::new(),
            sample_count: count,
            buckets: vec![
                CalibrationBucket {
                    upper_bound_seconds: Some(0.1),
                    count,
                },
                CalibrationBucket {
                    upper_bound_seconds: None,
                    count: 0.0,
                },
            ],
        }],
    };
    let partial = profile(vec![
        setup("biei_style_setup_duration_seconds", 20.0),
        setup("biei_source_setup_duration_seconds", 12.0),
    ]);
    let base = base_costs();

    let derived =
        derive_with_base_cpu_for_test(&partial, &base).expect("setup-only profile derives");

    assert!(matches!(
        derived.coverage.style_setup.source,
        CalibrationValueSource::Measured
    ));
    assert_eq!(derived.coverage.style_setup.samples, 20.0);
    assert!(matches!(
        derived.coverage.source_load.source,
        CalibrationValueSource::Measured
    ));
    assert!(matches!(
        derived.coverage.render_cpu.source,
        CalibrationValueSource::Default
    ));
    assert_eq!(derived.costs.render_cpu_cost.min, base.render_cpu_cost.min);
}

#[test]
fn load_rejects_wrong_kind_and_schema() {
    let dir = std::env::temp_dir();

    let mut wrong_kind = profile(vec![render_histogram()]);
    wrong_kind.kind = "something-else".to_owned();
    let path = dir.join("biei-sim-import-kind-test.json");
    std::fs::write(&path, serde_json::to_vec(&wrong_kind).expect("json")).expect("write");
    assert!(load_calibration_profile(&path).is_err());
    let _ = std::fs::remove_file(&path);

    let mut wrong_schema = profile(vec![render_histogram()]);
    wrong_schema.schema_version = 999;
    let path = dir.join("biei-sim-import-schema-test.json");
    std::fs::write(&path, serde_json::to_vec(&wrong_schema).expect("json")).expect("write");
    assert!(load_calibration_profile(&path).is_err());
    let _ = std::fs::remove_file(&path);
}

#[test]
fn profile_provenance_replaces_unrelated_simulator_node_defaults() {
    let profile = profile(vec![render_histogram()]);
    let mut config = SimConfig::default();
    assert_ne!(config.cpu_cores_per_node, 2);

    apply_profile_provenance(&profile, &mut config).expect("apply provenance");

    assert_eq!(config.cpu_cores_per_node, 2);
    assert_eq!(config.cluster.renderer_slots_per_node, 3);
    assert_eq!(config.cluster.render_permits_per_node, Some(2));
    assert_eq!(config.cluster.native_render_permits_per_node, Some(2));
}
