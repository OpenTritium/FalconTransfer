use crate::task_info::TaskInfo;
use compio_debouncer::Debouncer;
use falcon_config::global_config;
use falcon_identity::task::TaskId;
use falcon_task_composer::{TaskCommand, TaskStatus, fetch_meta};
use flume as mpmc;
use futures_util::{
    Stream, StreamExt,
    stream::{BoxStream, LocalBoxStream},
};
use see::sync as watch;
use serde::Deserialize;
use std::{
    num::{NonZeroU8, NonZeroU32},
    pin::Pin,
    time::Duration,
};
use tokio_stream::StreamMap;
use tracing::{debug, error, info};
use url::Url;

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "opt")]
#[serde(rename_all = "snake_case")]
pub enum NativeCommand {
    ChangeConcurrency { id: TaskId, concurrency: NonZeroU8 },
    ChangeRateLimited { id: TaskId, limit: Option<NonZeroU32> },
    Pause { id: TaskId },
    Resume { id: TaskId },
    Cancel { id: TaskId },
    Create { url: Url },
    Remove { id: TaskId },
}

pub async fn handle_native_cmd(
    ncmd: NativeCommand, watchers: &mut StreamMap<TaskId, LocalBoxStream<'static, TaskInfo>>,
    dest: &mpmc::Sender<TaskCommand>,
) -> anyhow::Result<()> {
    use NativeCommand::*;
    match ncmd {
        ChangeConcurrency { id, concurrency } => {
            dest.send_async(TaskCommand::ChangeConcurrency { id, concurrency }).await?;
            info!("ChangeConcurrency[{id}] -> {concurrency}");
        }
        ChangeRateLimited { id, limit } => {
            dest.send_async(TaskCommand::ChangeRateLimited { id, limit }).await?;
            info!("ChangeRateLimited[{id}] -> {limit:?}");
        }
        Pause { id } => {
            dest.send_async(TaskCommand::Pause(id)).await?;
            info!("Pause[{id}]");
        }
        Resume { id } => {
            dest.send_async(TaskCommand::Resume(id)).await?;
            info!("Resume[{id}]");
        }
        Cancel { id } => {
            dest.send_async(TaskCommand::Cancel(id)).await?;
            info!("Cancel[{id}]");
        }
        Create { url } => {
            // fetch builder 后面可以支持添加自定义头
            let id = TaskId::new();
            info!("Generate TaskId[{id}] for URL: {url}");
            let meta = match fetch_meta(&url).await {
                Ok(meta) => meta,
                Err(e) => {
                    error!("Failed to fetch metadata for {url}: {e:#}");
                    return Err(e.into());
                }
            };
            info!("Create Task[{id}] with meta: {meta}");
            let status = TaskStatus::builder().id(id).name(meta.name()).url(url.clone()).total(meta.size()).build();
            let (tx, rx) = watch::channel(status);
            dest.send_async(TaskCommand::Create { meta: meta.into(), watch: tx.into() }).await?;
            info!("Start watching Task[{id}]");
            let rx = rx.into_stream();
            let rx = Debouncer::new(rx, Duration::from_millis(global_config().nativeport_debounce_ms.get()));
            let rx = rx.map(TaskInfo::from);
            watchers.insert(id, rx.boxed_local());
            info!("Add Task[{id}] to watchers (now tracking {} tasks)", watchers.len());
        }
        Remove { id } => {
            watchers.remove(&id);
            info!("Remove Task[{id}] from watchers (now tracking {} tasks)", watchers.len());
        }
    }
    debug!("Native command handled successfully");
    Ok(())
}
