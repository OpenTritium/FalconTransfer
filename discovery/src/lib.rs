#![feature(const_index)]
#![feature(const_trait_impl)]
mod announce;
mod iface;
mod listen;
mod neighbors;

use crate::{
    listen::{listen, parse_instance_from_fullname},
    neighbors::ScopedAddr,
};
use compio::runtime::{JoinHandle, spawn};
use futures_util::StreamExt;
use neighbors::NeighborTable;
use std::{pin::pin, sync::Arc};

const SERVICE_TYPE: &str = &FULLNAME_SUFFIX[1..];
const FULLNAME_SUFFIX: &str = "._falcon_transfer._udp.local.";
const ALPN_ID: &str = "falcon-transfer-v1";

pub struct DiscoveryService {
    handle: JoinHandle<()>,
    daemon: announce::AnnounceDaemon,
    neighbors: Arc<NeighborTable>,
}

impl DiscoveryService {
    pub fn run() -> Self {
        let neighbors = Arc::new(NeighborTable::new());
        let announce = announce::AnnounceDaemon::new();
        announce.register().expect("Failed to announce");

        let neighbors_clone = neighbors.clone();
        let handle = spawn(async move {
            use listen::ServiceOpt::*;
            let mut discovery = pin!(listen());
            while let Some(info) = discovery.next().await {
                match info {
                    Register(resolved) => {
                        let new_addrs = resolved.addresses.into_iter().map(Into::into).collect::<Vec<ScopedAddr>>();
                        let instance_name = parse_instance_from_fullname(&resolved.fullname).unwrap();
                        neighbors_clone
                            .entry(instance_name.into())
                            .and_modify(|old_addrs| {
                                if new_addrs != *old_addrs {
                                    *old_addrs = new_addrs.clone();
                                }
                            })
                            .or_insert(new_addrs);
                    }
                    Unregister(instance_name) => {
                        neighbors_clone.remove(&instance_name);
                    }
                }
            }
        });
        Self { daemon: announce, handle, neighbors }
    }

    #[inline]
    pub fn neighbors(&self) -> &NeighborTable { &self.neighbors }
}
