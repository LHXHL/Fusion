use serde::{Deserialize, Serialize};

use crate::protocol::message::Message;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    Hello = 1,
    HelloAck = 2,
    Heartbeat = 3,
    AgentAnnounce = 4,
    RouteUpdate = 5,
    TaskRequest = 6,
    TaskResult = 7,
    StreamOpen = 8,
    StreamData = 9,
    StreamClose = 10,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u16,
    pub msg_type: MessageType,
    pub session_id: Option<String>,
    pub stream_id: Option<u32>,
    pub src_agent: Option<String>,
    pub dst_agent: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub message: Message,
}

impl Frame {
    pub fn new(
        msg_type: MessageType,
        src_agent: Option<String>,
        dst_agent: Option<String>,
        message: Message,
    ) -> Self {
        Self {
            header: FrameHeader {
                version: 1,
                msg_type,
                session_id: None,
                stream_id: None,
                src_agent,
                dst_agent,
            },
            message,
        }
    }

    pub fn with_stream_id(mut self, stream_id: u32) -> Self {
        self.header.stream_id = Some(stream_id);
        self
    }
}
