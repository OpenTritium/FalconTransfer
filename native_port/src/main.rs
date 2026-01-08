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
    task_info::{NativePayload, TaskInfo},
    watchers::{WatchGroup, WatcherEvent},
};
use async_broadcast as broadcast;
use compio::{runtime::spawn, time::interval};
use falcon_config::global_config;
use falcon_identity::task::TaskId;
use falcon_task_composer::TaskDispatcher;
use flume as mpmc;
use futures_util::{FutureExt, select};
use std::{collections::HashMap, time::Duration};
use tracing::{debug, error, info, warn};
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

    // Load configuration from centralized config
    let config = global_config();
    info!(debounce_interval_ms = config.debounce_interval_ms.get(), "Configuration loaded");

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
                warn!("stdin channel broken");
                break; // Channel closed, exit
            }
        }
    })
    .detach();

    info!("Starting main event loop with batch debouncing");
    let mut update_buffer = HashMap::new();
    let debounce_duration = Duration::from_millis(config.debounce_interval_ms.get());
    let mut tick = interval(debounce_duration);
    info!(debounce_interval_ms = config.debounce_interval_ms.get(), "Timer created, starting event loop");

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
            event = watchers.next().fuse() => {
                handle_watcher_event(event, &mut update_buffer, &mut watchers).await;
            }
            _ = tick.tick().fuse() => {
                handle_timer_tick(&mut update_buffer, &mut port_writer).await;
            }
        }
    }
}

#[inline]
async fn handle_watcher_event(
    event: WatcherEvent, update_buffer: &mut HashMap<TaskId, TaskInfo>, watchers: &mut WatchGroup,
) {
    debug!("Received watcher event");
    match event {
        WatcherEvent::Updated(task) => {
            let id = task.id;
            // Buffer the update for batch sending
            update_buffer.insert(id, task);
            // Re-arm the watcher for this task
            watchers.acknowledge_and_rewatch(id);
            debug!(%id, buffer_size = update_buffer.len(), "Task update buffered");
        }
        WatcherEvent::Removed(task_id) => {
            debug!(%task_id, "Task removed event received");
            // Remove from buffer and from the watcher group to prevent continued watching
            update_buffer.remove(&task_id);
            watchers.remove(task_id);
            info!(%task_id, "Task removed and notification sent");
        }
    }
}

#[inline]
async fn handle_timer_tick(update_buffer: &mut HashMap<TaskId, TaskInfo>, port_writer: &mut port::NativePortWriter) {
    debug!("Timer tick triggered");
    // Batch flush: send all buffered updates at once
    if !update_buffer.is_empty() {
        debug!(batch_size = update_buffer.len(), "Preparing to send batch updates");
        // Merge all payloads into a single batch
        let all_tasks: Vec<_> = update_buffer.values().cloned().collect();
        debug!(total_tasks = all_tasks.len(), "Merged all tasks into batch");

        if let Err(e) = port_writer.send(NativePayload(all_tasks.into_boxed_slice())).await {
            error!(error = %e, "Failed to send batch updates");
            // Note: This doesn't break the main loop as it was in the original code
            return;
        }
        info!(batch_size = update_buffer.len(), "Batch updates sent successfully");
        update_buffer.clear();
    } else {
        debug!("Timer tick but no updates to send");
    }
}
