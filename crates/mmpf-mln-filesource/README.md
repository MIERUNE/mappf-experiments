# mmpf-mln-filesource

Rust implementations of MapLibre Native's network and database `FileSource`
leaves for the MMPF experiments.

The crate owns the renderer-resource HTTP boundary: bounded concurrency and
body sizes, retry and single-flight behavior, cache freshness, SSRF filtering,
provider-health evidence, and Prometheus metrics. Biei supplies only runtime
capacity and the narrow private-host allowlist, keeping renderer scheduling and
HTTP application policy out of this crate.

Registration is process-global and must run inside the long-lived Tokio runtime
before constructing the first MapLibre Native renderer. Metrics use the
`mmpf_mln_resource_` prefix owned by this reusable crate.
