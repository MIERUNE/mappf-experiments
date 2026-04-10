//! HRW-based placement of tile groups onto cluster members.

use std::{cmp::Ordering, collections::BinaryHeap, hash::Hasher};

use twox_hash::XxHash64;

use crate::membership::Peer;

/// HRW placement over cluster peers for a given tileset and tile-locality group.
#[derive(Clone)]
pub struct HrwRouter {
    candidate_count: usize,
    tile_group_size: u64,
}

#[derive(Eq, PartialEq)]
pub struct ScoredPeer {
    pub score: u64,
    pub peer: Peer,
}

impl Ord for ScoredPeer {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .cmp(&self.score)
            .then_with(|| self.peer.id.cmp(&other.peer.id))
    }
}

impl PartialOrd for ScoredPeer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl HrwRouter {
    /// Creates a router with the given candidate count and tile-group size.
    pub fn new(candidate_count: usize, tile_group_size: u64) -> Self {
        Self {
            candidate_count: candidate_count.max(1),
            tile_group_size: tile_group_size.max(1),
        }
    }

    /// Returns candidate peers for the tile-locality group.
    pub fn route_tile(&self, peers: Vec<Peer>, tileset_id: &str, tile_id: u64) -> Vec<ScoredPeer> {
        let tile_group_id = tile_id / self.tile_group_size;
        self.route_group(peers, tileset_id, tile_group_id)
    }

    /// Returns candidate peers for an arbitrary provider-resource key.
    pub fn route_key(&self, peers: Vec<Peer>, key: &str) -> Vec<ScoredPeer> {
        self.route_group(peers, key, 0)
    }

    fn route_group(&self, peers: Vec<Peer>, key: &str, group_id: u64) -> Vec<ScoredPeer> {
        let mut top_peers =
            BinaryHeap::with_capacity(self.candidate_count.saturating_add(1).min(peers.len()));

        for peer in peers {
            let candidate = ScoredPeer {
                score: hrw_weight(key, group_id, &peer.id),
                peer,
            };
            top_peers.push(candidate);
            if top_peers.len() > self.candidate_count {
                top_peers.pop();
            }
        }

        let routed = top_peers.into_sorted_vec();
        debug_assert!(routed.len() <= self.candidate_count);
        debug_assert!(routed.windows(2).all(|pair| pair[0].score >= pair[1].score));
        routed
    }

    /// Returns candidate peers for tileset-wide metadata endpoints.
    pub fn route_tileset(&self, peers: Vec<Peer>, tileset_id: &str) -> Vec<ScoredPeer> {
        self.route_tile(peers, tileset_id, 0)
    }
}

/// Computes the rendezvous-hash score for a node and tile-locality group.
fn hrw_weight(tileset_id: &str, tile_group_id: u64, node_id: &str) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(tileset_id.as_bytes());
    hasher.write_u8(0xff);
    hasher.write_u64(tile_group_id);
    hasher.write_u8(0xfe);
    hasher.write(node_id.as_bytes());
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::HrwRouter;
    use crate::membership::Peer;

    fn peers() -> Vec<Peer> {
        ["node-a", "node-b", "node-c"]
            .into_iter()
            .enumerate()
            .map(|(index, id)| Peer {
                id: id.to_string(),
                addr: SocketAddr::from(([127, 0, 0, 1], 8000 + index as u16)),
            })
            .collect()
    }

    #[test]
    fn routes_tiles_in_the_same_hilbert_group_to_the_same_candidates() {
        let router = HrwRouter::new(2, 512);

        let first = router.route_tile(peers(), "mierune/omt", 512);
        let second = router.route_tile(peers(), "mierune/omt", 1023);

        assert_eq!(
            first.iter().map(|peer| &peer.peer.id).collect::<Vec<_>>(),
            second.iter().map(|peer| &peer.peer.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn routes_adjacent_hilbert_groups_independently() {
        let router = HrwRouter::new(1, 512);

        let group_zero = router.route_tile(peers(), "mierune/omt", 511);
        let group_one = router.route_tile(peers(), "mierune/omt", 512);

        assert_eq!(group_zero.len(), 1);
        assert_eq!(group_one.len(), 1);
        // This assertion is intentionally not about different winners: HRW may
        // pick the same node for adjacent groups. The important contract is
        // that the group boundary changes the score input.
        assert_ne!(group_zero[0].score, group_one[0].score);
    }

    #[test]
    fn routes_provider_keys_stably() {
        let router = HrwRouter::new(2, 512);

        let first = router.route_key(peers(), "glyph:https://fonts.example/0-255.pbf");
        let second = router.route_key(peers(), "glyph:https://fonts.example/0-255.pbf");

        assert_eq!(
            first.iter().map(|peer| &peer.peer.id).collect::<Vec<_>>(),
            second.iter().map(|peer| &peer.peer.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn returns_candidates_in_score_order() {
        let router = HrwRouter::new(3, 512);
        let routed = router.route_tile(peers(), "mierune/omt", 42);

        assert_eq!(routed.len(), 3);
        assert!(routed.windows(2).all(|pair| pair[0].score >= pair[1].score));
    }
}
