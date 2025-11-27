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
use tracing::error;
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
        error!("Failed forward command {:?} to dispatcher: {}", $cmd, $err)
    };
}

pub async fn handle_cmd_res(
    cmd_res: io::Result<NativeCommand>, inner_cmd_tx: mpmc::Sender<TaskCommand>, outer_watchers: &mut WatchGroup,
) {
    use NativeCommand::*;
    match cmd_res {
        Ok(ref c @ Create { ref url }) => {
            let id = TaskId::new();
            let meta = match fetch_meta(url).await {
                Ok(meta) => Box::new(meta),
                Err(err) => {
                    error!("Failed to fetch meta via command: {c:?}, err: {err}");
                    return;
                }
            };
            let default_status = TaskStatus::default_with(
                id,
                meta.name(),
                meta.url().clone(),
                None,
                meta.full_content_range().and_then(|rng| rng.last().map(|n| n + 1)),
            );
            let (status_tx, status_rx) = watch::channel(default_status);
            let cmd = TaskCommand::Create { meta, watch: status_tx.into() };
            let _ = inner_cmd_tx.send_async(cmd).await.inspect_err(|err| forward_error!(c, err));
            outer_watchers.push(status_rx);
        }
        Ok(cc @ ChangeConcurrency { id, concurrency }) => {
            let _ = inner_cmd_tx
                .send_async(TaskCommand::ChangeConcurrency { id, concurrency })
                .await
                .inspect_err(|err| forward_error!(cc, err));
        }
        Ok(crl @ ChangeRateLimited { id, limit }) => {
            let _ = inner_cmd_tx
                .send_async(TaskCommand::ChangeRateLimited { id, limit })
                .await
                .inspect_err(|err| forward_error!(crl, err));
        }
        Ok(p @ Pause { id }) => {
            let _ = inner_cmd_tx.send_async(TaskCommand::Pause(id)).await.inspect_err(|err| forward_error!(p, err));
        }
        Ok(r @ Resume { id }) => {
            let _ = inner_cmd_tx.send_async(TaskCommand::Resume(id)).await.inspect_err(|err| forward_error!(r, err));
        }
        Ok(c @ Cancel { id }) => {
            let _ = inner_cmd_tx.send_async(TaskCommand::Cancel(id)).await.inspect_err(|err| forward_error!(c, err));
        }
        Ok(r @ Remove { id }) => {
            let _ = inner_cmd_tx.send_async(TaskCommand::Remove(id)).await.inspect_err(|err| forward_error!(r, err));
        }
        Err(err) => {
            panic!("Failed to recv a cmd :{err:?}");
        }
    }
}
