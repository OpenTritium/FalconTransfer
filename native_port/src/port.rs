use crate::{command::NativeCommand, task_info::NativePayload};
use compio::{
    fs::{Stdin, Stdout, stdin, stdout},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
};
use std::io;

type MsgLen = u32;

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

    pub async fn recv(&mut self) -> io::Result<NativeCommand> {
        let msg_len_info = [0u8; size_of::<MsgLen>()];
        let (_, msg_len_info) = self.rx.read_exact(msg_len_info).await?;
        let msg_len = MsgLen::from_ne_bytes(msg_len_info) as usize;
        let buf = vec![0u8; msg_len];
        let (_, buf) = self.rx.read_exact(buf).await?;
        serde_json::from_slice(&buf).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse message from slice: {err}"))
        })
    }

    pub async fn send(&mut self, payload: NativePayload) -> io::Result<()> {
        let msg_json = serde_json::to_string(&payload).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Failed to serialize message to JSON: {err}"))
        })?;
        let msg_len = msg_json.len() as MsgLen;
        let msg_len_info = msg_len.to_ne_bytes();
        self.tx.write_all(msg_len_info).await?;
        self.tx.write_all(msg_json.into_bytes()).await?;
        self.tx.flush().await?;
        Ok(())
    }
}
