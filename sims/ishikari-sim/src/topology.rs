//! Synthetic node identities and addresses shared by simulator implementations.

use std::net::SocketAddr;

use anyhow::{Result, ensure};
use ishikari_core::storage::Peer;

const SIM_NODE_BASE_PORT: u16 = 10_000;
pub(crate) const MAX_SIMULATED_NODE_COUNT: usize = (u16::MAX - SIM_NODE_BASE_PORT) as usize + 1;

pub(crate) fn simulated_peers(node_count: usize) -> Vec<Peer> {
    (0..node_count)
        .map(|index| Peer {
            id: format!("node-{index}"),
            addr: SocketAddr::from((
                [127, 0, 0, 1],
                SIM_NODE_BASE_PORT + u16::try_from(index).expect("validated node index"),
            )),
        })
        .collect()
}

pub(crate) fn simulated_peer(index: usize) -> Result<Peer> {
    ensure!(
        index <= usize::from(u16::MAX - SIM_NODE_BASE_PORT),
        "node index exceeds the simulator address range"
    );
    Ok(Peer {
        id: format!("node-{index}"),
        addr: SocketAddr::from((
            [127, 0, 0, 1],
            SIM_NODE_BASE_PORT + u16::try_from(index).expect("validated node index"),
        )),
    })
}
