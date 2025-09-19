#![feature(const_index)]
#![feature(const_trait_impl)]
mod client;
mod iface;
mod neighbors;
mod server;

use crate::{
    client::{DebouncedClient, parse_instance_from_fullname},
    neighbors::ScopedAddr,
};
use neighbors::NeighborTable;
use std::sync::Arc;
use tokio::spawn;

const SERVICE_TYPE: &str = &FULLNAME_SUFFIX[1..];
const FULLNAME_SUFFIX: &str = "._falcon_transfer._udp.local.";
const ALPN_ID: &str = "falcon-transfer-v1";

pub struct DiscoveryService {
    tx: server::Server,
    rx: client::DebouncedClient,
    neighbors: Arc<NeighborTable>,
}

impl DiscoveryService {
    pub fn run() -> Self {
        let neighbors = Arc::new(NeighborTable::new());
        let tx = server::Server::new();
        let (rx, mut event_rx) = DebouncedClient::run();
        tx.run().expect("failed to run server");
        let neighbors_clone = neighbors.clone();
        spawn(async move {
            while let Some(resolv) = event_rx.recv().await {
                let new_addrs = resolv.addresses.into_iter().map(Into::into).collect::<Vec<ScopedAddr>>();
                let instance_name = parse_instance_from_fullname(&resolv.fullname).unwrap();
                neighbors_clone
                    .entry(instance_name.into())
                    .and_modify(|old_addrs| {
                        if new_addrs != *old_addrs {
                            *old_addrs = new_addrs.clone();
                        }
                    })
                    .or_insert(new_addrs);
            }
        });
        Self { tx, rx, neighbors }
    }

    pub fn neighbors(&self) -> &NeighborTable { &self.neighbors }
}
