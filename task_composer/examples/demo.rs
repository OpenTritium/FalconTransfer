use std::time::Duration;

use async_broadcast as broadcast;
use compio::{runtime::spawn, time::sleep};
use flume as mpmc;
use see::sync as watch;
use task_composer::{Dispatcher, TaskCommand, TaskStatus, fetch_meta};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use url::Url;

#[compio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
            .with_max_level(Level::DEBUG) // 捕获 DEBUG 及更高级别的日志
            .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    let (cmd_tx, cmd_rx) = mpmc::unbounded();
    let (_qos_tx, qos_rx) = broadcast::broadcast(1);
    let dispatcher = Dispatcher::builder().cmd(cmd_rx).qos(qos_rx).build();
    spawn(async move { dispatcher.spawn().await }).detach();
    let url = Url::parse("https://releases.ubuntu.com/24.04/ubuntu-24.04.3-desktop-amd64.iso").unwrap();
    let meta = Box::new(fetch_meta(&url).await.unwrap());
    let (status_tx, status_rx) = watch::channel(TaskStatus::default());
    let cmd = TaskCommand::Create { meta, watch: status_tx.into() };
    cmd_tx.send_async(cmd).await.unwrap();

    sleep(Duration::from_hours(1)).await;
}
