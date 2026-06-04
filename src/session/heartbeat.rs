use chrono::Utc;

use crate::protocol::{
    frame::{Frame, MessageType},
    message::{HeartbeatMessage, Message},
};

pub fn heartbeat_frame(src_agent: String, dst_agent: Option<String>) -> Frame {
    Frame::new(
        MessageType::Heartbeat,
        Some(src_agent),
        dst_agent,
        Message::Heartbeat(HeartbeatMessage {
            unix_ts: Utc::now().timestamp(),
        }),
    )
}
