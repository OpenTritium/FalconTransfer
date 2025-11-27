#![feature(never_type)]
mod command;
mod port;
mod task_info;
mod watchers;

use crate::{
    command::handle_cmd_res,
    port::NativePort,
    task_info::NativePayload,
    watchers::{WatchGroup, handle_watch_status},
};
use async_broadcast as broadcast;
use compio::runtime::spawn;
use falcon_task_composer::TaskDispatcher;
use flume as mpmc;
use futures_util::{FutureExt, select};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[compio::main]
async fn main() {
    let log = tracing_appender::rolling::daily("logs", "claw.log");
    let (writer, _guard) = tracing_appender::non_blocking(log);
    let file_layer = fmt::layer().with_writer(writer).json();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry().with(filter).with(file_layer).init();
    std::panic::set_hook(Box::new(tracing_panic::panic_hook));

    let mut native_port = NativePort::new();
    let (cmd_tx, cmd_rx) = mpmc::unbounded();
    // todo 条件编译 qos
    let (_qos_tx, qos_rx) = broadcast::broadcast(1);
    let dispatcher = TaskDispatcher::builder().cmd(cmd_rx).qos(qos_rx).build();
    spawn(async move { dispatcher.spawn().await }).detach();
    let mut watchers = WatchGroup::new();
    let snapshot = watchers.snapshot_all().await;
    // todo 等待持久化状态恢复
    native_port.send(NativePayload(snapshot)).await.unwrap();
    loop {
        select! {
            cmd_res = native_port.recv().fuse() => {
                handle_cmd_res(cmd_res, cmd_tx.clone(), &mut watchers).await;
            }
            update = watchers.next().fuse() => {
                handle_watch_status(update, &mut watchers, &mut native_port).await;
            }
        }
    }
}
