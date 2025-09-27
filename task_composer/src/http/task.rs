use crate::http::{
    command::TaskCommand, controller::Controller, meta::HttpTaskMeta, qos::QosAdvice, status::TaskStatus,
};
use reqwest::Client;
use std::{mem, sync::Arc};
use thiserror::Error;
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::{JoinError, JoinHandle},
};

trait LazyJoinHandle: FnOnce() -> JoinHandle<()> + Send + 'static {}
impl<F> LazyJoinHandle for F where F: FnOnce() -> JoinHandle<()> + Send + 'static {}

struct Task<F: LazyJoinHandle> {
    name: String,
    cmd: mpsc::Sender<TaskCommand>,
    controller: LazyHandle<F>,
    status: watch::Receiver<TaskStatus>,
    meta: Arc<HttpTaskMeta>,
}

#[derive(Default)]
enum LazyHandle<F: LazyJoinHandle> {
    Ready(F),
    Running(JoinHandle<()>),
    #[default]
    None,
}

#[derive(Debug, Error)]
enum TaskError {
    #[error("task was running already")]
    AlreadyRun,
    #[error("")]
    AlreadyPause,
    #[error("")]
    AlreadyCancelOrFinished,
    #[error("failed to send cmd: {cmd:?} via channel, err: {err:?}")]
    ChannelError { cmd: TaskCommand, err: mpsc::error::SendError<TaskCommand> },
    #[error("")]
    JoinError(#[from] JoinError),
}

// todo TASK 不持有具体路径

impl<F: LazyJoinHandle> Task<F> {
    pub fn new(
        name: String, meta: HttpTaskMeta, qos: broadcast::Receiver<QosAdvice>, client: Client,
    ) -> Task<impl LazyJoinHandle> {
        let meta = Arc::new(meta);
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (status_tx, status_rx) = watch::channel(Default::default());
        let meta_clone = meta.clone();
        let run_controller = move || {
            let controller = Controller::new(meta_clone, client, cmd_rx, qos, status_tx);
            controller.run()
        };

        Task { name, cmd: cmd_tx, controller: LazyHandle::Ready(run_controller), status: status_rx, meta }
    }
}

impl<F: LazyJoinHandle> Task<F> {
    async fn run(&mut self) -> Result<(), TaskError> {
        use LazyHandle::*;
        if let Ready(lazy_handle) = mem::take(&mut self.controller) {
            let join_handle = lazy_handle();
            self.controller = Running(join_handle);
            return Ok(());
        }
        Err(TaskError::AlreadyRun)
    }

    async fn send_cmd(&self, cmd: TaskCommand) -> Result<(), mpsc::error::SendError<TaskCommand>> {
        // todo： 接入日志
        self.cmd.send(cmd).await
    }

    async fn pause(&self) -> Result<(), TaskError> {
        use LazyHandle::*;
        use TaskCommand::*;
        use TaskError::*;
        if self.status.borrow().state.is_pending() && matches!(self.controller, Running(_)) {
            return Err(AlreadyPause);
        }
        self.send_cmd(Pause).await.map_err(|err| ChannelError { cmd: Pause, err })?;
        Ok(())
    }

    async fn resume(&self) -> Result<(), TaskError> {
        use LazyHandle::*;
        use TaskCommand::*;
        use TaskError::*;
        if self.status.borrow().state.is_running() && matches!(self.controller, Running(_)) {
            return Err(AlreadyRun);
        }
        self.send_cmd(Pause).await.map_err(|err| ChannelError { cmd: Pause, err })?;
        Ok(())
    }

    // 约定好调用此方法后不可再调用任何通道
    async fn cancel(&mut self) -> Result<(), TaskError> {
        use LazyHandle::*;
        use TaskCommand::*;
        use TaskError::*;
        match mem::take(&mut self.controller) {
            Ready(_) => Ok(()),
            Running(handle) => {
                self.send_cmd(Cancel).await.map_err(|err| ChannelError { cmd: Cancel, err })?;
                handle.await?;
                Ok(())
            }
            None => Err(AlreadyCancelOrFinished),
        }
    }
}
