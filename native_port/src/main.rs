#![feature(never_type)]
// 仅在非 test 构建时启用这些 Clippy 警告
#![cfg_attr(not(test), warn(clippy::nursery, clippy::unwrap_used, clippy::todo, clippy::dbg_macro,))]
#![allow(clippy::future_not_send)]
mod command;
mod port;
mod task_info;
// mod watchers;

use crate::{
    command::handle_native_cmd,
    port::native_port,
    task_info::{NativePayload, TaskInfo},
};
use anyhow::bail;
use async_broadcast as broadcast;
use compio::{fs::create_dir_all, runtime::spawn};
use compio_debouncer::Debouncer;
use falcon_config::{get_config_path, global_config};
use falcon_persist::{StatePersistPoller, store::PersistStore};
use falcon_task_composer::TaskDispatcher;
use flume as mpmc;
use futures_util::{FutureExt, StreamExt, select};
use std::time::Duration;
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
        // .with_file(true)
        // .with_line_number(true)
        .with_target(false)
        // .with_thread_ids(true)
        .compact();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
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

    let cfg_path = get_config_path()?;
    let path = cfg_path.parent().ok_or_else(|| anyhow::anyhow!("Config path has no parent"))?.join("states.redb");
    create_dir_all(&path.parent().expect("Cannot walk to config file's parent folder")).await?;
    let store = PersistStore::open_or_create(path).await?;

    let mut dispatcher = match TaskDispatcher::from_store_cmd(&store, &cmd_rx).await {
        Ok(dispatcher) => {
            info!("Successfully restored dispatcher from persistent store");
            dispatcher
        }
        Err(e) => {
            info!(error = %e, "No persisted state found, creating new dispatcher");
            // todo 条件编译 qos
            let (_qos_tx, qos_rx) = broadcast::broadcast(1);
            TaskDispatcher::builder().cmd(cmd_rx).qos(qos_rx).build()
        }
    };
    let restored_watchers = dispatcher.acquire_watchers().collect::<Box<_>>();
    // 我们要借用这个迭代器，所以不得不先把它 collect
    info!(task_count = restored_watchers.len(), "Sending initial task snapshot of restored tasks");
    let restored_info: Box<_> =
        restored_watchers.iter().map(|(_, watcher)| watcher.borrow().clone()).map(|status| status.into()).collect();

    // 给刚刚恢复出来的 watcher 套 debouncer 成流
    let mut watchers = restored_watchers
        .into_iter()
        .map(|(id, w)| {
            let rx = w.into_stream();
            let rx = Debouncer::new(rx, Duration::from_millis(global_config().nativeport_debounce_ms.get()));
            let rx = rx.map(TaskInfo::from).boxed_local();
            (id, rx)
        })
        .collect::<StreamMap<_, _>>();
    debug!("Watchers map initialized");
    let mut poller = StatePersistPoller::new(store, Duration::from_secs(1));
    poller.register(dispatcher.subscribe_pendings());
    poller.register(dispatcher.subscribe_persisted_tasks());
    info!("Spawning task dispatcher");
    dispatcher.spawn().detach();
    debug!("Task dispatcher spawned");
    poller.watch().detach();

    let (msg_tx, msg_rx) = mpmc::bounded::<NativePayload>(128);
    debug!("Message channel created (capacity: 128)");
    // 发送刚刚恢复的持久化状态
    msg_tx.send_async(NativePayload(restored_info)).await?;
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
