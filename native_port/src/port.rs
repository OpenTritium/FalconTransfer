use crate::{command::NativeCommand, task_info::NativePayload};
use compio::{
    fs::{Stdin, Stdout, stdin, stdout},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
};
use std::{io, mem::size_of};
use tracing::{debug, error, instrument};

type MsgLen = u32;

const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024; // 100MB

pub struct NativePort {
    rx: Stdin,
    tx: BufWriter<Stdout>,
}

impl NativePort {
    #[inline]
    pub fn new() -> Self {
        let tx = BufWriter::new(stdout());
        let rx = stdin();
        Self { rx, tx }
    }

    #[instrument(skip(self))]
    pub async fn recv(&mut self) -> io::Result<NativeCommand> {
        let msg_len_info = [0u8; size_of::<MsgLen>()];
        let (_, msg_len_info) = self.rx.read_exact(msg_len_info).await?;
        let msg_len = MsgLen::from_ne_bytes(msg_len_info) as usize;

        debug!(msg_len, "Receiving message from native port");

        if msg_len > MAX_MESSAGE_SIZE {
            error!(msg_len, max_size = MAX_MESSAGE_SIZE, "Message size exceeds maximum allowed limit");
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message size {msg_len} exceeds maximum allowed size {MAX_MESSAGE_SIZE}"),
            ));
        }

        let buf = vec![0u8; msg_len];
        let (_, buf) = self.rx.read_exact(buf).await?;
        serde_json::from_slice(&buf).map_err(|err| {
            error!(error = %err, msg_len, "Failed to deserialize command from native port");
            io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse message from slice: {err}"))
        })
    }

    #[instrument(skip(self, payload))]
    pub async fn send(&mut self, payload: NativePayload) -> io::Result<()> {
        let task_count = payload.0.len();
        let msg_json = serde_json::to_string(&payload).map_err(|err| {
            error!(error = %err, task_count, "Failed to serialize task payload to JSON");
            io::Error::new(io::ErrorKind::InvalidData, format!("Failed to serialize message to JSON: {err}"))
        })?;
        let msg_len = msg_json.len() as MsgLen;
        debug!(msg_len, task_count, "Sending task update to native port");
        let msg_len_info = msg_len.to_ne_bytes();
        self.tx.write_all(msg_len_info).await?;
        self.tx.write_all(msg_json.into_bytes()).await?;
        self.tx.flush().await?;
        Ok(())
    }
}
