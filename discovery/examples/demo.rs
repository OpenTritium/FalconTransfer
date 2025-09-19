use std::{mem::forget, time::Duration};

use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let x = discovery::DiscoveryService::run();
    loop {
        sleep(Duration::from_secs(3)).await;
        println!("{:?}", x.neighbors());
    }
}
