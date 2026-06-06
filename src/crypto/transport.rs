use std::io::{Error, ErrorKind};

use sha2::{Digest, Sha256};

use crate::{
    crypto::wrapper::{global_wrapper_config, payload_looks_wrapped, WrapperPipeline},
    protocol::{codec, frame::Frame},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedKey([u8; 32]);

impl SharedKey {
    pub fn from_secret(secret: &str) -> Self {
        let digest = Sha256::digest(secret.as_bytes());
        let mut key = [0_u8; 32];
        key.copy_from_slice(&digest);
        Self(key)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

pub fn encode_transport_frame(
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<Vec<u8>, Error> {
    let plain = codec::encode_frame(frame)?;
    WrapperPipeline::from_config(&global_wrapper_config(), shared_key).wrap(plain)
}

pub fn encode_transport_frame_with_pipeline(
    frame: &Frame,
    pipeline: &WrapperPipeline,
) -> Result<Vec<u8>, Error> {
    let plain = codec::encode_frame(frame)?;
    pipeline.wrap(plain)
}

pub fn decode_transport_frame(
    bytes: &[u8],
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let pipeline = WrapperPipeline::from_config(&global_wrapper_config(), shared_key);
    if !pipeline.requires_wrapped_input() && payload_looks_wrapped(bytes) {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "received wrapped transport frame but no local wrapper is configured",
        ));
    }
    let plain = pipeline.unwrap(bytes)?;
    codec::decode_frame(&plain)
}

pub fn decode_transport_frame_with_pipeline(
    bytes: &[u8],
    pipeline: &WrapperPipeline,
) -> Result<Frame, Error> {
    if !pipeline.requires_wrapped_input() && payload_looks_wrapped(bytes) {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "received wrapped transport frame but no local wrapper is configured",
        ));
    }
    let plain = pipeline.unwrap(bytes)?;
    codec::decode_frame(&plain)
}

#[cfg(test)]
mod tests {
    use crate::crypto::wrapper::{WrapperConfig, WrapperPipeline};
    use crate::protocol::{
        frame::{Frame, MessageType},
        message::{HelloMessage, Message},
    };

    use super::{
        decode_transport_frame, decode_transport_frame_with_pipeline, encode_transport_frame,
        encode_transport_frame_with_pipeline, SharedKey,
    };

    fn sample_frame() -> Frame {
        Frame::new(
            MessageType::Hello,
            Some("agent-a".to_string()),
            None,
            Message::Hello(HelloMessage {
                agent_id: "agent-a".to_string(),
                agent_name: "node-a".to_string(),
                capabilities: vec!["transport:tcp".to_string()],
                protocol_version: 1,
            }),
        )
    }

    #[test]
    fn transport_frame_roundtrip_plain() {
        let frame = sample_frame();
        let encoded = encode_transport_frame(&frame, None).unwrap();
        let decoded = decode_transport_frame(&encoded, None).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn transport_frame_roundtrip_encrypted() {
        let frame = sample_frame();
        let key = SharedKey::from_secret("fusion-secret");
        let encoded = encode_transport_frame(&frame, Some(&key)).unwrap();
        let decoded = decode_transport_frame(&encoded, Some(&key)).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn transport_frame_rejects_mismatched_plain_and_encrypted_modes() {
        let frame = sample_frame();
        let key = SharedKey::from_secret("fusion-secret");
        let encrypted = encode_transport_frame(&frame, Some(&key)).unwrap();
        assert!(decode_transport_frame(&encrypted, None).is_err());

        let plain = encode_transport_frame(&frame, None).unwrap();
        assert!(decode_transport_frame(&plain, Some(&key)).is_err());
    }

    #[test]
    fn transport_frame_roundtrip_with_multi_stage_pipeline() {
        let frame = sample_frame();
        let key = SharedKey::from_secret("fusion-secret");
        let pipeline = WrapperPipeline::from_config(
            &WrapperConfig {
                compress: true,
                padding: Some(16),
            },
            Some(&key),
        );
        let encoded = encode_transport_frame_with_pipeline(&frame, &pipeline).unwrap();
        let decoded = decode_transport_frame_with_pipeline(&encoded, &pipeline).unwrap();
        assert_eq!(decoded, frame);
    }
}
