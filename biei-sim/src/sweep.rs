//! CSV export helper for parameter sweep results.

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

use biei::types::RouteTier;

use crate::metrics::Report;

/// Write a CSV of sweep results. Each (label, Report) pair becomes one row.
/// `label` typically encodes the sweep axes (e.g. "bl=25,alpha=1.0").
pub fn write_csv<P: AsRef<Path>>(path: P, results: &[(String, Report)]) -> io::Result<()> {
    let mut f = File::create(path)?;
    writeln!(
        f,
        "label,submitted,completed,rejected,sla_violations,throughput_rps,\
         cpu_render_util_pct,cpu_render_avg_inflight,cpu_render_peak_inflight,\
         latency_p50_ms,latency_p90_ms,latency_p95_ms,latency_p99_ms,latency_max_ms,\
         tier1_pct,tier2_pct,tier3_pct,tier4_overflow_pct,\
         cold_starts,style_swaps,tasks_with_sources,source_loads,source_hits,elapsed_ms"
    )?;
    for (label, r) in results {
        let label = if label.contains(',') || label.contains('"') || label.contains('\n') {
            format!("\"{}\"", label.replace('"', "\"\""))
        } else {
            label.to_string()
        };
        let pct = |n: usize| -> f64 {
            if r.total > 0 {
                n as f64 / r.total as f64 * 100.0
            } else {
                0.0
            }
        };
        let tier = |t: RouteTier| r.tier_counts.get(&t).copied().unwrap_or(0);
        writeln!(
            f,
            "{},{},{},{},{},{:.2},{:.2},{:.2},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{},{},{},{},{},{:.2}",
            label,
            r.total,
            r.completed,
            r.rejected,
            r.sla_violations,
            r.throughput,
            r.cpu_render_utilization_pct,
            r.cpu_render_avg_inflight,
            r.cpu_render_peak_inflight,
            ms(r.latency_p50),
            ms(r.latency_p90),
            ms(r.latency_p95),
            ms(r.latency_p99),
            ms(r.latency_max),
            pct(tier(RouteTier::Tier1WarmTracking)),
            pct(tier(RouteTier::Tier2HrwBl)),
            pct(tier(RouteTier::Tier3DrainSwap)),
            pct(tier(RouteTier::Tier4Overflow)),
            r.cold_starts,
            r.style_swaps,
            r.tasks_with_sources,
            r.source_loads,
            r.source_hits,
            ms(r.elapsed),
        )?;
    }
    f.flush()?;
    Ok(())
}

fn ms(d: Duration) -> f64 {
    d.as_micros() as f64 / 1000.0
}
