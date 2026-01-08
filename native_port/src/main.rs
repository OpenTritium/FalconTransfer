#![feature(never_type)]
// 仅在非 test 构建时启用这些 Clippy 警告
#![cfg_attr(not(test), warn(clippy::nursery, clippy::unwrap_used, clippy::todo, clippy::dbg_macro,))]
#![allow(clippy::future_not_send)]
mod command;
mod port;
mod task_info;
mod watchers;

use crate::{
    command::handle_cmd_res,
    port::native_port,
    task_info::NativePayload,
    watchers::{WatchGroup, handle_watch_event},
};
use async_broadcast as broadcast;
use compio::runtime::spawn;
use falcon_task_composer::TaskDispatcher;
use flume as mpmc;
use futures_util::{FutureExt, select};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[compio::main]
async fn main() {
    let log = tracing_appender::rolling::daily("logs", "claw.log");
    let (writer, _guard) = tracing_appender::non_blocking(log);
    let file_layer = fmt::layer().with_writer(writer).json();
    // Enable debug logging for development
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::registry().with(filter).with(file_layer).init();
    std::panic::set_hook(Box::new(tracing_panic::panic_hook));

    info!("Initializing native port");

    // Split native port into reader and writer so they can be used in separate tasks
    let (mut port_reader, mut port_writer) = native_port();

    let (cmd_tx, cmd_rx) = mpmc::unbounded();
    // todo 条件编译 qos
    let (_qos_tx, qos_rx) = broadcast::broadcast(1);
    let dispatcher = TaskDispatcher::builder().cmd(cmd_rx).qos(qos_rx).build();
    info!("Spawning task dispatcher");
    spawn(async move { dispatcher.spawn().await }).detach();
    let mut watchers = WatchGroup::new();
    let snapshot = watchers.snapshot_all().await;
    // todo 等待持久化状态恢复
    info!(task_count = snapshot.len(), "Sending initial task snapshot to native port");
    if let Err(err) = port_writer.send(NativePayload(snapshot)).await {
        error!(error = %err, "Failed to send initial task snapshot");
    }

    // Create a channel for commands read from stdin
    // This avoids the select! drop issue by having recv() run in a dedicated task
    let (stdin_tx, stdin_rx) = mpmc::unbounded();

    // Spawn a dedicated task for reading from stdin
    // This task owns the recv() operation and won't be interrupted by select!
    spawn(async move {
        loop {
            let cmd_res = port_reader.recv().await;
            if stdin_tx.send_async(cmd_res).await.is_err() {
                warn!("stdin channel broken")
                break; // Channel closed, exit
            }
        }
    })
    .detach();

    info!("Starting main event loop");
    loop {
        select! {
            cmd_res = stdin_rx.recv_async().fuse() => {
                match cmd_res {
                    Ok(res) => {
                        if !handle_cmd_res(res, cmd_tx.clone(), &mut watchers).await {
                            error!("Command handling failed, exiting event loop");
                            break;
                        }
                    }
                    Err(_) => {
                        error!("Stdin reader task closed");
                        break;
                    }
                }
            }
            update = watchers.next().fuse() => {
                if !handle_watch_event(update, &mut watchers, &mut port_writer).await {
                    error!("Watch event handling failed, exiting event loop");
                    break;
                }
            }
        }
    }
}
