//! Data-layout facts about an archive: clustering and blob reuse.
//!
//! These are *cost* properties, not correctness ones. A directory carries an
//! explicit offset and length for every tile, so an archive whose data section
//! is in arbitrary order still serves correct bytes. What it loses is locality,
//! and a range-and-chunk reader is built entirely on locality: neighbouring tile
//! ids are expected to land in the same fetched chunk.
//!
//! The cost therefore scales with archive size rather than being uniform. An
//! archive small enough that every chunk fits in cache pays almost nothing once
//! warm; a planet-scale archive pays a chunk-sized read per scattered tile, which
//! is read amplification of one to two orders of magnitude and a near-zero cache
//! hit rate. Callers should weigh [`LayoutReport`] against the archive's size and
//! their chunk capacity rather than treating "not clustered" as pass/fail.
//!
//! The header's `clustered` flag is a *producer claim*. [`LayoutVerifier`] checks
//! the claim against the directory entries themselves, which is why verification
//! belongs at publication time: the publisher is already writing the directory,
//! whereas re-walking every leaf of a large archive to serve one tile is not
//! affordable.

use crate::format::{DirectoryEntry, Header};

/// Verified facts about how an archive's data section is laid out.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LayoutReport {
    /// Directory entries inspected.
    pub entries: u64,
    /// Entries whose offset is lower than the preceding entry's offset.
    ///
    /// For an archive without blob reuse this is exactly a clustering violation.
    /// When blobs *are* reused, a lower offset may instead be a legitimate
    /// back-reference to an already-written blob, so interpret this together with
    /// [`HeaderLayout::reuses_blobs`].
    pub backward_offsets: u64,
    /// Entries whose tile id does not exceed the preceding entry's tile id.
    ///
    /// Directory entries are required to ascend, so any occurrence indicates a
    /// malformed or misordered directory rather than a layout choice.
    pub unordered_tile_ids: u64,
    /// Tile id of the first backward offset, for diagnostics.
    pub first_backward_tile_id: Option<u64>,
}

impl LayoutReport {
    /// Whether every inspected entry advanced through the data section.
    #[must_use]
    pub fn is_ordered(&self) -> bool {
        self.backward_offsets == 0
    }

    /// Whether the inspected directories were themselves well formed.
    #[must_use]
    pub fn directories_are_ascending(&self) -> bool {
        self.unordered_tile_ids == 0
    }
}

/// Checks the clustered claim incrementally, one directory at a time.
///
/// Entries must be supplied in tile-id order, which is the order a directory
/// stores them and the order a leaf-by-leaf walk yields. State is a few integers
/// regardless of archive size, so a planet-scale archive can be verified without
/// holding its directories in memory.
#[derive(Clone, Copy, Debug, Default)]
pub struct LayoutVerifier {
    report: LayoutReport,
    previous_offset: Option<u64>,
    previous_tile_id: Option<u64>,
}

impl LayoutVerifier {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds one directory's entries, in the order the directory stores them.
    ///
    /// Leaf pointers are skipped: they address directory blocks rather than tile
    /// data, so their offsets belong to a different section and say nothing about
    /// tile clustering.
    pub fn observe(&mut self, entries: &[DirectoryEntry]) {
        for entry in entries {
            if entry.is_leaf() {
                continue;
            }
            self.report.entries += 1;
            if self
                .previous_tile_id
                .is_some_and(|previous| entry.tile_id <= previous)
            {
                self.report.unordered_tile_ids += 1;
            }
            if self
                .previous_offset
                .is_some_and(|previous| entry.offset < previous)
            {
                self.report.backward_offsets += 1;
                self.report
                    .first_backward_tile_id
                    .get_or_insert(entry.tile_id);
            }
            self.previous_tile_id = Some(entry.tile_id);
            self.previous_offset = Some(entry.offset);
        }
    }

    #[must_use]
    pub fn finish(self) -> LayoutReport {
        self.report
    }
}

/// Layout facts the fixed header already states, with no directory read.
///
/// Available from the first 127 bytes, so a publisher or operator can screen an
/// archive before deciding whether a full directory walk is worthwhile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeaderLayout {
    /// The producer's clustering claim, unverified.
    pub claims_clustered: bool,
    pub addressed_tiles: u64,
    pub tile_entries: u64,
    pub tile_contents: u64,
}

impl HeaderLayout {
    #[must_use]
    pub fn of(header: &Header) -> Self {
        Self {
            claims_clustered: header.clustered,
            addressed_tiles: header.n_addressed_tiles,
            tile_entries: header.n_tile_entries,
            tile_contents: header.n_tile_contents,
        }
    }

    /// Whether identical tiles share one stored blob.
    ///
    /// Absent reuse, a `LayoutReport` with any backward offset is unambiguously
    /// unclustered rather than possibly a back-reference.
    #[must_use]
    pub fn reuses_blobs(&self) -> bool {
        self.tile_contents < self.tile_entries
    }

    /// Whether runs of identical adjacent tiles are collapsed into one entry.
    ///
    /// Uniform regions — a mostly-empty raster, ocean, a constant weather field —
    /// compress dramatically under run-length encoding, so its absence on such
    /// data is a strong sign the producer is leaving size on the table.
    #[must_use]
    pub fn uses_run_length_encoding(&self) -> bool {
        self.tile_entries < self.addressed_tiles
    }

    /// Whether the header reports no space optimisation of any kind.
    #[must_use]
    pub fn is_fully_unoptimized(&self) -> bool {
        self.addressed_tiles == self.tile_entries && self.tile_entries == self.tile_contents
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tile(tile_id: u64, offset: u64) -> DirectoryEntry {
        DirectoryEntry {
            tile_id,
            offset,
            length: 16,
            run_length: 1,
        }
    }

    fn leaf(tile_id: u64, offset: u64) -> DirectoryEntry {
        DirectoryEntry {
            tile_id,
            offset,
            length: 16,
            run_length: 0,
        }
    }

    #[test]
    fn ascending_offsets_are_ordered() {
        let mut verifier = LayoutVerifier::new();
        verifier.observe(&[tile(0, 0), tile(1, 16), tile(2, 32)]);
        let report = verifier.finish();
        assert!(report.is_ordered());
        assert!(report.directories_are_ascending());
        assert_eq!(report.entries, 3);
        assert_eq!(report.first_backward_tile_id, None);
    }

    #[test]
    fn a_backward_offset_is_reported_with_its_tile_id() {
        let mut verifier = LayoutVerifier::new();
        verifier.observe(&[tile(0, 100), tile(1, 40), tile(2, 60), tile(3, 10)]);
        let report = verifier.finish();
        assert!(!report.is_ordered());
        assert_eq!(report.backward_offsets, 2);
        assert_eq!(report.first_backward_tile_id, Some(1));
    }

    /// Verification spans directories, so a violation across a leaf boundary must
    /// still be caught — otherwise a large archive could look clustered simply
    /// because each leaf is internally ordered.
    #[test]
    fn ordering_is_checked_across_directory_boundaries() {
        let mut verifier = LayoutVerifier::new();
        verifier.observe(&[tile(0, 0), tile(1, 16)]);
        verifier.observe(&[tile(2, 8), tile(3, 64)]);
        let report = verifier.finish();
        assert_eq!(report.backward_offsets, 1);
        assert_eq!(report.first_backward_tile_id, Some(2));
    }

    /// Leaf pointers address the directory section, not tile data. Counting them
    /// would report spurious violations on every well-formed multi-level archive.
    #[test]
    fn leaf_pointers_are_ignored() {
        let mut verifier = LayoutVerifier::new();
        verifier.observe(&[tile(0, 900), leaf(1, 10), tile(2, 950)]);
        let report = verifier.finish();
        assert!(report.is_ordered());
        assert_eq!(report.entries, 2, "only tile entries are inspected");
    }

    #[test]
    fn non_ascending_tile_ids_are_flagged_separately() {
        let mut verifier = LayoutVerifier::new();
        verifier.observe(&[tile(5, 0), tile(5, 16), tile(4, 32)]);
        let report = verifier.finish();
        assert!(report.is_ordered(), "offsets still advance");
        assert!(!report.directories_are_ascending());
        assert_eq!(report.unordered_tile_ids, 2);
    }

    #[test]
    fn header_layout_separates_reuse_from_run_length_encoding() {
        let reused = HeaderLayout {
            claims_clustered: true,
            addressed_tiles: 10,
            tile_entries: 10,
            tile_contents: 4,
        };
        assert!(reused.reuses_blobs());
        assert!(!reused.uses_run_length_encoding());
        assert!(!reused.is_fully_unoptimized());

        let run_length = HeaderLayout {
            claims_clustered: true,
            addressed_tiles: 10,
            tile_entries: 3,
            tile_contents: 3,
        };
        assert!(!run_length.reuses_blobs());
        assert!(run_length.uses_run_length_encoding());

        let unoptimized = HeaderLayout {
            claims_clustered: false,
            addressed_tiles: 427,
            tile_entries: 427,
            tile_contents: 427,
        };
        assert!(unoptimized.is_fully_unoptimized());
        assert!(!unoptimized.reuses_blobs());
        assert!(!unoptimized.uses_run_length_encoding());
    }
}
