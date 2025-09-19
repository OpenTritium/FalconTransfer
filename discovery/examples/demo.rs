use std::mem::forget;

#[tokio::main]
async fn main() {
    let (discovery_srv, mut dns_recv) = discovery::DiscoveryService::run();
    forget(discovery_srv);
    while let Some(x) = dns_recv.recv().await {
        println!("{:?}", x);
    }
}
