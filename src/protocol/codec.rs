use std::io::{Error, ErrorKind};

use crate::protocol::frame::Frame;

pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(frame).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
}

pub fn decode_frame(bytes: &[u8]) -> Result<Frame, Error> {
    serde_json::from_slice(bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::protocol::{
        codec::{decode_frame, encode_frame},
        frame::{Frame, MessageType},
        message::{HelloMessage, Message},
    };

    #[test]
    fn frame_roundtrip() {
        let frame = Frame::new(
            MessageType::Hello,
            Some("agent-a".to_string()),
            None,
            Message::Hello(HelloMessage {
                agent_id: "agent-a".to_string(),
                agent_name: "node-a".to_string(),
                capabilities: vec!["transport:tcp".to_string()],
                protocol_version: 1,
            }),
        );

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();
        assert_eq!(decoded, frame);
    }
}
