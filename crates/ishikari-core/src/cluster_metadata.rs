//! Pure Ishikari membership metadata shared by runtime adapters.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use crate::storage::Peer;

/// Membership epoch for Ishikari's internal routing contract.
///
/// Different internal-wire epochs intentionally form separate gossip clusters.
pub const CLUSTER_ID: &str = "ishikari-v2";

/// Membership key containing a node's explicitly advertised internal HTTP address.
pub const HTTP_ADVERTISE_ADDR_KEY: &str = "http-advertise-addr";

/// Ishikari's failure-detector retention policy, shared by production and simulation.
pub const DEAD_NODE_GRACE_PERIOD: Duration = Duration::from_secs(30);

/// Ishikari's tombstone retention policy, shared by production and simulation.
pub const MARKED_FOR_DELETION_GRACE_PERIOD: Duration = Duration::from_secs(60 * 60);

/// Decodes an explicitly published internal HTTP address.
///
/// Nodes without a valid address are intentionally excluded from routing. The
/// gossip endpoint and the internal HTTP endpoint need not share an address or
/// port, so guessing from the gossip address could route to the wrong listener.
pub fn peer_http_addr(http_advertise_addr: Option<&str>) -> Option<SocketAddr> {
    http_advertise_addr.and_then(|value| value.parse().ok())
}

/// Projects service metadata into a stable, sorted routable peer snapshot.
pub fn project_peers<'a>(
    nodes: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Arc<[Peer]> {
    let mut peers: Vec<_> = nodes
        .into_iter()
        .filter_map(|(id, advertised_addr)| {
            Some(Peer {
                id: id.to_string(),
                addr: peer_http_addr(advertised_addr)?,
            })
        })
        .collect();
    peers.sort_by(|left, right| left.id.cmp(&right.id));
    peers.into()
}

#[cfg(test)]
mod tests {
    use super::{peer_http_addr, project_peers};

    #[test]
    fn explicit_internal_http_address_is_routable() {
        assert_eq!(
            peer_http_addr(Some("10.0.0.3:9090")),
            Some("10.0.0.3:9090".parse().unwrap())
        );
    }

    #[test]
    fn malformed_internal_http_address_is_not_routable() {
        assert_eq!(peer_http_addr(Some("ishikari-2.internal:9090")), None);
    }

    #[test]
    fn missing_internal_http_address_is_not_routable() {
        assert_eq!(peer_http_addr(None), None);
    }

    #[test]
    fn peer_projection_filters_invalid_metadata_and_sorts_by_node_id() {
        let peers = project_peers([
            ("node-b", Some("10.0.0.2:9090")),
            ("node-invalid", Some("not-an-address")),
            ("node-a", Some("10.0.0.1:9090")),
            ("node-missing", None),
        ]);

        assert_eq!(
            peers
                .iter()
                .map(|peer| peer.id.as_str())
                .collect::<Vec<_>>(),
            vec!["node-a", "node-b"]
        );
    }
}
