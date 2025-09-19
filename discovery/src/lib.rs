#![feature(const_index)]
#![feature(const_trait_impl)]
mod client;
mod iface;
mod server;

use crate::client::DebouncedClient;
use mdns_sd::ResolvedService;
use tokio::sync::mpsc;

const SERVICE_TYPE: &str = &FULLNAME_SUFFIX[1..];
const FULLNAME_SUFFIX: &str = "._falcon_transfer._udp.local.";
const ALPN_ID: &str = "falcon-transfer-v1";

pub struct DiscoveryService {
    tx: server::Server,
    rx: client::DebouncedClient,
}

impl DiscoveryService {
    pub fn run() -> (Self, mpsc::Receiver<ResolvedService>) {
        let tx = server::Server::new();
        let (rx, srv_rx) = DebouncedClient::run();
        tx.run().expect("failed to run server");
        (Self { tx, rx }, srv_rx)
    }
}
