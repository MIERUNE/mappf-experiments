//! Pure cache sizing policy shared by production caches and the modeled simulator.

use std::mem::size_of;

use crate::interned::TilesetId;

const CHUNK_CACHE_CAPACITY_CEILING_BYTES: u64 = 1024 * 1024 * 1024;
const CACHE_ENTRY_KEY_BYTES: usize = size_of::<TilesetId>() + size_of::<u64>();

/// Applies the production chunk-cache capacity ceiling.
pub const fn effective_chunk_cache_capacity(requested_bytes: u64) -> u64 {
    if requested_bytes < CHUNK_CACHE_CAPACITY_CEILING_BYTES {
        requested_bytes
    } else {
        CHUNK_CACHE_CAPACITY_CEILING_BYTES
    }
}

/// Returns the weighted size of a tile-cache entry, including its key.
///
/// `None` represents a negative cache entry with no payload bytes.
pub fn tile_cache_entry_weight(payload_bytes: Option<usize>) -> u32 {
    cache_entry_weight(payload_bytes.unwrap_or_default())
}

/// Returns the weighted size of a chunk-cache entry, including its key.
pub fn chunk_cache_entry_weight(payload_bytes: usize) -> u32 {
    cache_entry_weight(payload_bytes)
}

fn cache_entry_weight(payload_bytes: usize) -> u32 {
    let total = CACHE_ENTRY_KEY_BYTES.saturating_add(payload_bytes);
    total.min(u32::MAX as usize) as u32
}

#[cfg(test)]
mod tests {
    use super::{
        CACHE_ENTRY_KEY_BYTES, CHUNK_CACHE_CAPACITY_CEILING_BYTES, chunk_cache_entry_weight,
        effective_chunk_cache_capacity, tile_cache_entry_weight,
    };

    #[test]
    fn chunk_capacity_is_capped_at_one_gibibyte() {
        assert_eq!(
            effective_chunk_cache_capacity(CHUNK_CACHE_CAPACITY_CEILING_BYTES + 1),
            CHUNK_CACHE_CAPACITY_CEILING_BYTES
        );
    }

    #[test]
    fn negative_tile_entry_still_accounts_for_its_key() {
        assert_eq!(tile_cache_entry_weight(None), CACHE_ENTRY_KEY_BYTES as u32);
    }

    #[test]
    fn cache_entry_weights_saturate_at_u32_max() {
        assert_eq!(tile_cache_entry_weight(Some(usize::MAX)), u32::MAX);
        assert_eq!(chunk_cache_entry_weight(usize::MAX), u32::MAX);
    }
}
