#![feature(never_type)]
// 仅在非 test 构建时启用这些 Clippy 警告
#![cfg_attr(not(test), warn(clippy::nursery, clippy::unwrap_used, clippy::todo, clippy::dbg_macro,))]
#![allow(clippy::future_not_send)]
mod command;
mod port;
mod task_info;
// mod watchers;

use crate::{command::handle_native_cmd, port::native_port, task_info::NativePayload};
use anyhow::bail;
use async_broadcast as broadcast;
use compio::runtime::spawn;
use falcon_config::global_config;
use falcon_task_composer::TaskDispatcher;
use flume as mpmc;
use futures_util::{FutureExt, StreamExt, select};
use tokio_stream::StreamMap;
use tracing::{debug, error, info, trace};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[compio::main]
async fn main() -> anyhow::Result<()> {
    let log = tracing_appender::rolling::daily("logs", "core.log");
    let (writer, _guard) = tracing_appender::non_blocking(log);
    let file_layer = fmt::layer()
        .with_writer(writer)
        .with_ansi(false)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_thread_ids(true)
        .compact();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));
    tracing_subscriber::registry().with(filter).with(file_layer).init();
    std::panic::set_hook(Box::new(tracing_panic::panic_hook));
    run().await
}

async fn run() -> anyhow::Result<()> {
    info!(config = ?global_config(), "Native port starting with configuration");
    debug!("Initializing native port");
    let (mut port_reader, mut port_writer) = native_port();
    debug!("Native port initialized");

    let (cmd_tx, cmd_rx) = mpmc::unbounded();
    debug!("Command channel created");

    // todo 条件编译 qos
    let (_qos_tx, qos_rx) = broadcast::broadcast(1);
    let dispatcher = TaskDispatcher::builder().cmd(cmd_rx).qos(qos_rx).build();
    info!("Spawning task dispatcher");
    dispatcher.spawn().detach();
    debug!("Task dispatcher spawned");

    let mut watchers = StreamMap::new();
    debug!("Watchers map initialized");
    // let snapshot = watchers.snapshot_all().await;
    let (msg_tx, msg_rx) = mpmc::bounded::<NativePayload>(128);
    debug!("Message channel created (capacity: 128)");

    // ----- Send initial snapshot of all tasks -----
    // info!(task_count = snapshot.len(), "Sending initial task snapshot");
    // msg_tx.send_async(NativePayload(snapshot)).await?;

    // ----- MsgRx -> StdOut  -----
    debug!("Spawning writer task (MsgRx -> StdOut)");
    spawn(async move {
        while let Ok(payload) = msg_rx.recv_async().await {
            debug!(payload = ?payload, "Sending payload to native port, forwarding to msg channel now");
            if let Err(e) = port_writer.send(payload).await {
                error!(error = %e, "Failed to send payload to native port");
                break;
            }
        }
        error!("Writer task exiting - output channel closed or send failed");
    })
    .detach();

    // ----- StdIn -> CmdTx -----
    debug!("Spawning reader task (StdIn -> CmdTx)");
    let (ncmd_tx, ncmd_rx) = mpmc::bounded(128);
    spawn(async move {
        loop {
            let cmd = match port_reader.recv().await {
                Ok(cmd) => cmd,
                Err(e) => {
                    error!(error = %e, "Failed to receive command from native port");
                    break;
                }
            };
            debug!(command = ?cmd, "Received command from native port, forwarding to ncmd channel now");
            if ncmd_tx.send_async(cmd).await.is_err() {
                error!("Stdin channel broken");
                break;
            }
        }
        error!("Reader task exiting");
    })
    .detach();

    info!("Entering main event loop");
    loop {
        // ----- CmdRx && Watchers -----
        if watchers.is_empty() {
            trace!("No active watchers, waiting for commands");
            let ncmd = ncmd_rx.recv_async().await?;
            handle_native_cmd(ncmd, &mut watchers, &cmd_tx).await?;
        } else {
            select! {
                watcher_res = watchers.next().fuse() => {
                    let Some((_,info))= watcher_res else {
                        bail!("Watchers task exited");
                    };
                    trace!("Received watcher update, forwarding to msg_tx");
                    msg_tx.send_async(NativePayload([info].into())).await?;
                }
                ncmd_res = ncmd_rx.recv_async().fuse() => {
                    let ncmd = ncmd_res?;
                    handle_native_cmd(ncmd, &mut watchers, &cmd_tx).await?;
                }
            }
        }
    }
}
