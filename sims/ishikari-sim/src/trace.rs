use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, Write},
    ops::Range,
};

use anyhow::{Context, Result, bail};

use crate::workload::TraceEntry;

/// Reads and validates a JSONL trace.
pub fn read_trace(reader: impl BufRead) -> Result<Vec<TraceEntry>> {
    let mut entries = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.with_context(|| format!("read trace line {line_number}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let entry = serde_json::from_str(&line)
            .with_context(|| format!("parse trace line {line_number}"))?;
        entries.push(entry);
    }
    viewport_batch_ranges(&entries)?;
    Ok(entries)
}

/// Writes one trace entry in JSONL format.
pub fn write_trace_entry(writer: &mut impl Write, entry: &TraceEntry) -> Result<()> {
    serde_json::to_writer(&mut *writer, entry).context("serialize trace entry")?;
    writer.write_all(b"\n").context("write trace newline")
}

/// Returns the contiguous ranges that form viewport batches.
///
/// A batch is identified by `(step, user)`. Ordinals must start at zero and be
/// contiguous, and a batch may not reappear later in the trace.
pub fn viewport_batch_ranges(entries: &[TraceEntry]) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    let mut seen = HashSet::new();
    let mut last_step_by_user = HashMap::new();
    let mut start = 0;

    while start < entries.len() {
        let key = (entries[start].step, entries[start].user);
        if !seen.insert(key) {
            bail!(
                "trace batch step={} user={} is not contiguous",
                key.0,
                key.1
            );
        }
        if let Some(previous_step) = last_step_by_user.insert(key.1, key.0)
            && key.0 <= previous_step
        {
            bail!(
                "trace user={} step={} does not follow previous step={}",
                key.1,
                key.0,
                previous_step
            );
        }

        let mut batch_tiles = HashSet::new();
        let mut end = start;
        while end < entries.len() && (entries[end].step, entries[end].user) == key {
            let expected_ordinal = end - start;
            if entries[end].ordinal != expected_ordinal {
                bail!(
                    "trace batch step={} user={} has ordinal {}, expected {}",
                    key.0,
                    key.1,
                    entries[end].ordinal,
                    expected_ordinal
                );
            }
            let entry = &entries[end];
            if !batch_tiles.insert((entry.tileset.as_str(), entry.z, entry.x, entry.y)) {
                bail!(
                    "trace batch step={} user={} repeats tile {} z={} x={} y={} at ordinal {}",
                    key.0,
                    key.1,
                    entry.tileset,
                    entry.z,
                    entry.x,
                    entry.y,
                    entry.ordinal
                );
            }
            end += 1;
        }
        ranges.push(start..end);
        start = end;
    }

    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{read_trace, viewport_batch_ranges, write_trace_entry};
    use crate::workload::TraceEntry;

    fn entry(step: u64, user: usize, ordinal: usize) -> TraceEntry {
        TraceEntry {
            step,
            user,
            ordinal,
            tileset: "japan".to_string(),
            z: 10,
            x: 900,
            y: 400,
            entry_node: Some(0),
        }
    }

    #[test]
    fn jsonl_round_trip_preserves_batches() {
        let mut second = entry(0, 0, 1);
        second.x += 1;
        let expected = vec![entry(0, 0, 0), second, entry(0, 1, 0)];
        let mut bytes = Vec::new();
        for entry in &expected {
            write_trace_entry(&mut bytes, entry).expect("write entry");
        }

        let actual = read_trace(Cursor::new(bytes)).expect("read trace");

        assert_eq!(actual, expected);
        assert_eq!(
            viewport_batch_ranges(&actual).expect("ranges"),
            [0..2, 2..3]
        );
    }

    #[test]
    fn rejects_non_contiguous_batch() {
        let entries = vec![entry(0, 0, 0), entry(0, 1, 0), entry(0, 0, 0)];

        let error = viewport_batch_ranges(&entries).expect_err("reopened batch must fail");

        assert!(error.to_string().contains("is not contiguous"));
    }

    #[test]
    fn rejects_missing_ordinal() {
        let entries = vec![entry(0, 0, 0), entry(0, 0, 2)];

        let error = viewport_batch_ranges(&entries).expect_err("ordinal gap must fail");

        assert!(error.to_string().contains("has ordinal 2, expected 1"));
    }

    #[test]
    fn rejects_duplicate_tile_within_viewport_batch() {
        let entries = vec![entry(0, 7, 0), entry(0, 7, 1)];

        let error = viewport_batch_ranges(&entries).expect_err("duplicate tile must fail");
        let message = error.to_string();

        assert!(message.contains("step=0 user=7"));
        assert!(message.contains("repeats tile japan z=10 x=900 y=400 at ordinal 1"));
    }

    #[test]
    fn permits_same_coordinate_for_different_tilesets() {
        let mut second = entry(0, 0, 1);
        second.tileset = "regional".to_string();
        let entries = vec![entry(0, 0, 0), second];

        let ranges = viewport_batch_ranges(&entries).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 0..2);
    }

    #[test]
    fn rejects_steps_that_go_backwards_for_one_user() {
        let entries = vec![entry(2, 0, 0), entry(1, 1, 0), entry(1, 0, 0)];

        let error = viewport_batch_ranges(&entries).expect_err("backward step must fail");

        assert!(
            error
                .to_string()
                .contains("does not follow previous step=2")
        );
    }
}
