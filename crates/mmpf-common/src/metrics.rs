//! Constructors for fixed Prometheus metric descriptors.

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
    core::Collector, proto::MetricFamily,
};

/// Encodes metric families in Prometheus text exposition format.
///
/// Encoding failures and invalid UTF-8 produce an empty string, preserving the
/// existing scrape fallback semantics used by the services.
pub fn encode_metric_families(families: &[MetricFamily]) -> String {
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    if encoder.encode(families, &mut buffer).is_err() {
        return String::new();
    }
    String::from_utf8(buffer).unwrap_or_default()
}

/// Registers collectors whose descriptors are startup invariants.
///
/// The caller supplies service-specific panic context so registration failures
/// remain attributable to the owning service.
pub fn register_collectors(
    registry: &Registry,
    collectors: impl IntoIterator<Item = Box<dyn Collector>>,
    panic_context: &str,
) {
    for collector in collectors {
        registry.register(collector).expect(panic_context);
    }
}

/// Creates a counter vector whose static descriptor is a startup invariant.
pub fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect("valid counter vec")
}

/// Creates a gauge vector whose static descriptor is a startup invariant.
pub fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels).expect("valid gauge vec")
}

/// Creates a histogram vector with Prometheus's default buckets.
pub fn histogram_vec(name: &str, help: &str, labels: &[&str]) -> HistogramVec {
    HistogramVec::new(HistogramOpts::new(name, help), labels).expect("valid histogram vec")
}

/// Creates a histogram vector with an explicit bucket layout.
pub fn histogram_vec_buckets(
    name: &str,
    help: &str,
    buckets: &[f64],
    labels: &[&str],
) -> HistogramVec {
    HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid histogram vec")
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::IntCounter;

    #[test]
    fn encodes_metric_families_as_prometheus_text() {
        let registry = Registry::new();
        let counter = IntCounter::new("common_test_total", "A shared test counter.")
            .expect("valid test counter");
        counter.inc();
        registry
            .register(Box::new(counter))
            .expect("register test counter");

        assert_eq!(
            encode_metric_families(&registry.gather()),
            "# HELP common_test_total A shared test counter.\n# TYPE common_test_total counter\ncommon_test_total 1\n"
        );
        assert_eq!(encode_metric_families(&[]), "");
    }

    #[test]
    fn registers_collectors_from_arrays_and_iterators() {
        let registry = Registry::new();
        let first = IntCounter::new("common_first_total", "First.").expect("valid first counter");
        let second =
            IntCounter::new("common_second_total", "Second.").expect("valid second counter");

        register_collectors(
            &registry,
            [Box::new(first) as Box<dyn Collector>],
            "register array metric",
        );
        register_collectors(
            &registry,
            [second]
                .into_iter()
                .map(|collector| Box::new(collector) as Box<dyn Collector>),
            "register iterator metric",
        );

        let names = registry
            .gather()
            .into_iter()
            .map(|family| family.name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["common_first_total", "common_second_total"]);
    }

    #[test]
    #[should_panic(expected = "register service metric")]
    fn registration_preserves_service_panic_context() {
        let registry = Registry::new();
        let counter =
            IntCounter::new("common_duplicate_total", "Duplicate.").expect("valid counter");

        register_collectors(
            &registry,
            [Box::new(counter.clone()) as Box<dyn Collector>],
            "register service metric",
        );
        register_collectors(
            &registry,
            [Box::new(counter) as Box<dyn Collector>],
            "register service metric",
        );
    }
}
