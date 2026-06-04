use std::io::{Error, ErrorKind};

use data_encoding::HEXLOWER;
use serde::{Deserialize, Serialize};

use crate::protocol::route::RouteUpdateMessage;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HelloMessage {
    pub agent_id: String,
    pub agent_name: String,
    pub capabilities: Vec<String>,
    pub protocol_version: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HelloAckMessage {
    pub accepted: bool,
    pub peer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatMessage {
    pub unix_ts: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentAnnounceMessage {
    pub agent_id: String,
    pub agent_name: String,
    pub capabilities: Vec<String>,
    pub services: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskAction {
    Shell,
    Screenshot,
    FileUpload,
    FileDownload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRequestMessage {
    pub task_id: String,
    pub action: TaskAction,
    pub args: Vec<String>,
    pub data_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskResultMessage {
    pub task_id: String,
    pub ok: bool,
    pub output: String,
    pub data_hex: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamOpenMessage {
    pub service: String,
    pub target_host: Option<String>,
    pub target_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamDataMessage {
    pub data_hex: String,
}

impl StreamDataMessage {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            data_hex: HEXLOWER.encode(bytes),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        HEXLOWER
            .decode(self.data_hex.as_bytes())
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamCloseMessage {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "body")]
pub enum Message {
    Hello(HelloMessage),
    HelloAck(HelloAckMessage),
    Heartbeat(HeartbeatMessage),
    AgentAnnounce(AgentAnnounceMessage),
    RouteUpdate(RouteUpdateMessage),
    TaskRequest(TaskRequestMessage),
    TaskResult(TaskResultMessage),
    StreamOpen(StreamOpenMessage),
    StreamData(StreamDataMessage),
    StreamClose(StreamCloseMessage),
}
