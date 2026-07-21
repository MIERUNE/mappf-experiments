//! Deterministic, allocation-free pseudo-random primitives.
//!
//! These are pure numeric mixers with no domain coupling, shared by the tile
//! fetch jitter (`ishikari-core`) and the simulators' deterministic workloads
//! (`ishikari-sim`). Keeping a single copy guarantees every consumer mixes bits
//! identically, so seeded output stays reproducible across crate boundaries.

/// SplitMix64 finalizer (Steele, Lea & Flood).
///
/// This is only the output permutation: it scrambles the supplied word without
/// first advancing it by the SplitMix64 Weyl increment. Use [`splitmix64`] when
/// the increment is part of the required semantics.
pub fn splitmix64_finalize(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Advances a word by the SplitMix64 Weyl increment, then applies
/// [`splitmix64_finalize`].
///
/// This one-shot form preserves consumers whose previous local helper included
/// the increment. It is deliberately distinct from calling the finalizer alone.
pub fn splitmix64(value: u64) -> u64 {
    splitmix64_finalize(value.wrapping_add(0x9e37_79b9_7f4a_7c15))
}

/// Maps a 64-bit value to a half-open `[0, 1)` `f64` using its top 53 bits.
///
/// Multiplying by the exact reciprocal of `2^53` is equivalent to dividing by
/// `2^53` (both operands are exactly representable) but keeps the hot path a
/// single multiply.
pub fn uniform_unit(value: u64) -> f64 {
    (value >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64))
}

/// Maps a 64-bit value to a strictly-positive `f64` fraction by centering each
/// 53-bit bucket at its midpoint.
///
/// Unlike [`uniform_unit`], this never returns exactly `0.0`, which callers rely
/// on where a strictly-positive fraction is required (e.g. before a logarithm).
pub fn uniform_open(value: u64) -> f64 {
    ((value >> 11) as f64 + 0.5) / (1_u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Golden vectors lock both semantics so adding or removing the Weyl
    // increment cannot silently perturb routing or reproducible simulations.
    #[test]
    fn splitmix64_finalize_matches_reference_vectors() {
        assert_eq!(splitmix64_finalize(0), 0);
        assert_eq!(splitmix64_finalize(1), 0x5692_161d_100b_05e5);
        assert_eq!(splitmix64_finalize(0xdead_beef), 0x4e06_2702_ec92_9eea);
        assert_eq!(splitmix64_finalize(u64::MAX), 0xb4d0_55fc_f2cb_bd7b);
    }

    #[test]
    fn splitmix64_matches_reference_vectors() {
        assert_eq!(splitmix64(0), 0xe220_a839_7b1d_cdaf);
        assert_eq!(splitmix64(1), 0x910a_2dec_8902_5cc1);
        assert_eq!(splitmix64(0xdead_beef), 0x4adf_b90f_68c9_eb9b);
    }

    #[test]
    fn uniform_unit_stays_in_half_open_range() {
        assert_eq!(uniform_unit(0), 0.0);
        assert!(uniform_unit(u64::MAX) < 1.0);
    }

    #[test]
    fn uniform_open_is_strictly_positive() {
        assert!(uniform_open(0) > 0.0);
        assert!(uniform_open(u64::MAX) > 0.0);
    }
}
