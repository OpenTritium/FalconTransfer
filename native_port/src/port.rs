use crate::{command::NativeCommand, task_info::NativePayload};
use compio::{
    fs::{Stdin, Stdout, stdin, stdout},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};
use std::{io, mem::size_of};

type MsgLen = u32;

const MAX_COMMAND_SIZE: usize = 1024 * 1024;

/// Reader half of the native port (stdin)
pub struct NativePortReader {
    rx: BufReader<Stdin>,
}

/// Writer half of the native port (stdout)
pub struct NativePortWriter {
    tx: BufWriter<Stdout>,
}

/// Create a new native port reader/writer pair
pub fn native_port() -> (NativePortReader, NativePortWriter) {
    let reader = NativePortReader { rx: BufReader::new(stdin()) };
    let writer = NativePortWriter { tx: BufWriter::new(stdout()) };
    (reader, writer)
}

impl NativePortReader {
    pub async fn recv(&mut self) -> io::Result<NativeCommand> {
        let msg_len_info = [0u8; size_of::<MsgLen>()];
        let (_, msg_len_info) = self.rx.read_exact(msg_len_info).await?;
        let msg_len = MsgLen::from_ne_bytes(msg_len_info) as usize;
        if msg_len > MAX_COMMAND_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Message size {msg_len} exceeds maximum {MAX_COMMAND_SIZE}"),
            ));
        }

        let buf = vec![0u8; msg_len];
        let (_, buf) = self.rx.read_exact(buf).await?;
        serde_json::from_slice(&buf).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))
    }
}

impl NativePortWriter {
    pub async fn send(&mut self, payload: NativePayload) -> io::Result<()> {
        let msg_json = serde_json::to_string(&payload)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        let msg_len = msg_json.len() as MsgLen;
        self.tx.write_all(msg_len.to_ne_bytes()).await?;
        self.tx.write_all(msg_json.into_bytes()).await?;
        self.tx.flush().await
    }
}
