//! HRW (rendezvous hashing) weight for `(worker_profile, node)` — Tier 2 ordering.

use crate::types::{NodeId, RenderMode, Scale, WorkerProfile};

/// Highest Random Weight (Rendezvous Hashing) score for `(profile, node)`.
/// Hashes stable style_id + render mode + scale so Static/Tile and @1x/@2x
/// get independent worker placement. `StyleRevision.version` is deliberately
/// **not** part of the input — version bumps must not move routing.
pub fn hrw_weight(profile: &WorkerProfile, node: &NodeId) -> u64 {
    // FNV-1a over the key bytes then mixed with the node id via splitmix64.
    // Deterministic, no dependencies, fine for sim/production HRW use
    // (production may want XxHash64 later; the I/F stays the same).
    let profile_hash = fnv1a_profile(profile);
    let node_hash = fnv1a(node.as_bytes());
    let mut h = profile_hash.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= node_hash.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    h ^ (h >> 31)
}

fn fnv1a_profile(profile: &WorkerProfile) -> u64 {
    let mut h = fnv1a(profile.style.id.as_bytes());
    h = fnv1a_continue(h, &[0]);
    h = fnv1a_continue(
        h,
        &[match profile.render_mode {
            RenderMode::Static => b's',
            RenderMode::Tile => b't',
        }],
    );
    h = fnv1a_continue(h, &[0]);
    fnv1a_continue(
        h,
        &[match profile.scale {
            Scale::X1 => b'1',
            Scale::X2 => b'2',
        }],
    )
}

fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_continue(0xcbf2_9ce4_8422_2325, bytes)
}

fn fnv1a_continue(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{StyleId, StyleRevision};

    fn profile(style: &str, render_mode: RenderMode, scale: Scale) -> WorkerProfile {
        WorkerProfile {
            style: StyleRevision {
                id: StyleId(style.to_string()),
                version: 0,
            },
            render_mode,
            scale,
        }
    }

    #[test]
    fn hrw_is_deterministic_per_profile_node_pair() {
        let k = profile("style-7", RenderMode::Static, Scale::X2);
        let a = hrw_weight(&k, &NodeId::from("node-3"));
        let b = hrw_weight(&k, &NodeId::from("node-3"));
        assert_eq!(a, b);
    }

    #[test]
    fn hrw_differs_for_different_style_ids() {
        let k1 = profile("style-7", RenderMode::Static, Scale::X2);
        let k2 = profile("style-8", RenderMode::Static, Scale::X2);
        // statistically almost certainly different; collisions on 1-byte
        // diff inputs are vanishingly rare for FNV-1a + splitmix mixing.
        assert_ne!(
            hrw_weight(&k1, &NodeId::from("node-3")),
            hrw_weight(&k2, &NodeId::from("node-3"))
        );
    }

    #[test]
    fn hrw_differs_for_different_modes_and_scales() {
        let static_2x = profile("style-7", RenderMode::Static, Scale::X2);
        let tile_2x = profile("style-7", RenderMode::Tile, Scale::X2);
        let static_1x = profile("style-7", RenderMode::Static, Scale::X1);
        assert_ne!(
            hrw_weight(&static_2x, &NodeId::from("node-3")),
            hrw_weight(&tile_2x, &NodeId::from("node-3"))
        );
        assert_ne!(
            hrw_weight(&static_2x, &NodeId::from("node-3")),
            hrw_weight(&static_1x, &NodeId::from("node-3"))
        );
    }

    #[test]
    fn hrw_differs_for_different_nodes() {
        let k = profile("style-7", RenderMode::Static, Scale::X2);
        assert_ne!(
            hrw_weight(&k, &NodeId::from("node-0")),
            hrw_weight(&k, &NodeId::from("node-1"))
        );
    }
}
