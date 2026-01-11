use crate::{command::NativeCommand, task_info::NativePayload};
use compio::{
    fs::{Stdin, Stdout, stdin, stdout},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};
use std::{io, mem::size_of};
use tracing::trace;

type MsgLen = u32;

const MAX_COMMAND_SIZE: usize = 1024 * 1024;

pub struct NativePortReader {
    rx: Stdin,
}

pub struct NativePortWriter {
    tx: Stdout,
}

pub fn native_port() -> (NativePortReader, NativePortWriter) {
    let rx = NativePortReader { rx: stdin() };
    let tx = NativePortWriter { tx: stdout() };
    (rx, tx)
}

impl NativePortReader {
    pub async fn recv(&mut self) -> io::Result<NativeCommand> {
        let msg_len_info = [0u8; size_of::<MsgLen>()];
        let (_, msg_len_info) = self.rx.read_exact(msg_len_info).await?;
        let msg_len = MsgLen::from_ne_bytes(msg_len_info) as usize;
        trace!("Received message length: {msg_len} bytes");
        if msg_len > MAX_COMMAND_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message size {msg_len} exceeds maximum {MAX_COMMAND_SIZE}"),
            ));
        }
        let buf = vec![0u8; msg_len];
        let (_, buf) = self.rx.read_exact(buf).await?;
        let cmd: NativeCommand =
            serde_json::from_slice(&buf).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        trace!("Received command: {cmd:?}");
        Ok(cmd)
    }
}

impl NativePortWriter {
    pub async fn send(&mut self, payload: NativePayload) -> io::Result<()> {
        const HEADER_LEN: usize = size_of::<MsgLen>();
        let mut buf = Vec::with_capacity(1024);
        buf.extend_from_slice(&[0u8; HEADER_LEN]);
        serde_json::to_writer(&mut buf, &payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        let msg_len = (buf.len() - HEADER_LEN) as MsgLen;
        let len_bytes = msg_len.to_ne_bytes();
        buf[0..HEADER_LEN].copy_from_slice(&len_bytes);
        trace!("Sending payload: {msg_len} bytes");
        self.tx.write_all(buf).await?;
        trace!("Payload sent successfully");
        Ok(())
    }
}
