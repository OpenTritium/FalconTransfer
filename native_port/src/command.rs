use crate::watchers::WatchGroup;
use falcon_identity::task::TaskId;
use falcon_task_composer::{TaskCommand, TaskStatus, fetch_meta};
use flume as mpmc;
use see::sync as watch;
use serde::Deserialize;
use std::{
    io,
    num::{NonZeroU8, NonZeroU32},
};
use tracing::{error, info, instrument};
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

macro_rules! forward_error {
    ($cmd:expr, $err:expr) => {
        error!(error = %$err, cmd = ?std::any::type_name_of_val(&$cmd), "Failed to forward command to dispatcher")
    };
}

#[instrument(skip(inner_cmd_tx, outer_watchers))]
#[must_use]
pub async fn handle_cmd_res(
    cmd_res: io::Result<NativeCommand>, inner_cmd_tx: mpmc::Sender<TaskCommand>, outer_watchers: &mut WatchGroup,
) -> bool {
    use NativeCommand::*;
    match cmd_res {
        Ok(Create { ref url }) => {
            let id = TaskId::new();
            info!(%id, %url, "Creating new download task");
            let meta = match fetch_meta(url).await {
                Ok(meta) => Box::new(meta),
                Err(err) => {
                    error!(error = %err, %url, "Failed to fetch metadata for new download task");
                    return false;
                }
            };
            let default_status = TaskStatus::builder()
                .id(id)
                .name(meta.name())
                .url(meta.url().clone())
                .total(meta.full_content_range().and_then(|rng| rng.last().map(|n| n + 1)))
                .build();
            let (status_tx, status_rx) = watch::channel(default_status);
            let cmd = TaskCommand::Create { meta, watch: status_tx.into() };
            if let Err(err) = inner_cmd_tx.send_async(cmd).await {
                error!(error = %err, %id, "Failed to send create command to dispatcher");
                return false;
            }
            outer_watchers.push(status_rx);
            true
        }
        Ok(cc @ ChangeConcurrency { id, concurrency }) => {
            info!(%id, concurrency = %concurrency.get(), "Changing task concurrency");
            inner_cmd_tx
                .send_async(TaskCommand::ChangeConcurrency { id, concurrency })
                .await
                .map(|_| true)
                .unwrap_or_else(|err| {
                    forward_error!(cc, err);
                    false
                })
        }
        Ok(crl @ ChangeRateLimited { id, limit }) => {
            info!(
                %id,
                rate_limit = limit.as_ref().map(|l| l.get()).unwrap_or(0),
                "Changing task rate limit"
            );
            inner_cmd_tx.send_async(TaskCommand::ChangeRateLimited { id, limit }).await.map(|_| true).unwrap_or_else(
                |err| {
                    forward_error!(crl, err);
                    false
                },
            )
        }
        Ok(p @ Pause { id }) => {
            info!(%id, "Pausing task");
            inner_cmd_tx.send_async(TaskCommand::Pause(id)).await.map(|_| true).unwrap_or_else(|err| {
                forward_error!(p, err);
                false
            })
        }
        Ok(r @ Resume { id }) => {
            info!(%id, "Resuming task");
            inner_cmd_tx.send_async(TaskCommand::Resume(id)).await.map(|_| true).unwrap_or_else(|err| {
                forward_error!(r, err);
                false
            })
        }
        Ok(c @ Cancel { id }) => {
            info!(%id, "Cancelling task");
            inner_cmd_tx.send_async(TaskCommand::Cancel(id)).await.map(|_| true).unwrap_or_else(|err| {
                forward_error!(c, err);
                false
            })
        }
        Ok(r @ Remove { id }) => {
            info!(%id, "Removing task");
            inner_cmd_tx.send_async(TaskCommand::Remove(id)).await.map(|_| true).unwrap_or_else(|err| {
                forward_error!(r, err);
                false
            })
        }
        Err(err) => {
            error!(error = %err, "Failed to receive command from native port");
            false
        }
    }
}
