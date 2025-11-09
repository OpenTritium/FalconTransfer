mod watchers;
use compio::{
    fs::{Stdin, Stdout, stdin, stdout},
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
};
use identity::task::TaskId;
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum NativeCommand {
    Subscribe,   // 获取当前所有状态并接受后续更新
    Unsubscribe, // 不订阅了
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum TaskState {
    Idle,
    Running { downloaded: usize },
    Paused { downloaded: usize },
    Completed,
    Cancelled,
    Failed { last_error: String, downloaded: usize },
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct TaskInfo {
    pub id: TaskId,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,
    #[serde(flatten)]
    pub state: TaskState,
}

#[derive(Debug, Serialize, Deserialize)]
struct NativePayload(Vec<TaskInfo>);

type MsgLen = u32;

struct NativePort {
    rx: Stdin,
    tx: Stdout,
}

impl NativePort {
    fn new() -> Self { Self { rx: stdin(), tx: stdout() } }

    async fn recv(&mut self) -> io::Result<NativeCommand> {
        let msg_len_info = [0u8; size_of::<MsgLen>()];
        let (_, msg_len_info) = self.rx.read_exact(msg_len_info).await?;
        let msg_len = MsgLen::from_ne_bytes(msg_len_info) as usize;
        let buf = vec![0u8; msg_len];
        let (_, buf) = self.rx.read_exact(buf).await?;
        serde_json::from_slice(&buf).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse message from slice: {}", err))
        })
    }

    async fn send(&mut self, payload: NativePayload) -> io::Result<()> {
        let msg_json = serde_json::to_string(&payload).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Failed to serialize message to JSON: {}", err))
        })?;
        let msg_len = msg_json.len() as MsgLen;
        let msg_len_info = msg_len.to_ne_bytes();
        self.tx.write_all(msg_len_info).await?;
        self.tx.write_all(msg_json.into_bytes()).await?;
        self.tx.flush().await?;
        Ok(())
    }
}

#[compio::main]
async fn main() {}
