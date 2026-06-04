use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    crypto::aead::{open, seal, EncMessage},
    protocol::{codec, frame::Frame},
};

const ENCRYPTED_FRAME_MAGIC: &[u8] = b"FXE1";

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SealedTransportFrame {
    nonce: String,
    ciphertext: String,
}

pub fn encode_transport_frame(frame: &Frame, shared_key: Option<&SharedKey>) -> Result<Vec<u8>, Error> {
    let plain = codec::encode_frame(frame)?;
    match shared_key {
        None => Ok(plain),
        Some(shared_key) => {
            let sealed = seal(&plain, shared_key.as_bytes()).map_err(|err| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("failed to encrypt transport frame: {err}"),
                )
            })?;
            let envelope = SealedTransportFrame {
                nonce: sealed.nonce,
                ciphertext: sealed.ciphertext,
            };
            let body = serde_json::to_vec(&envelope)
                .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
            let mut out = Vec::with_capacity(ENCRYPTED_FRAME_MAGIC.len() + body.len());
            out.extend_from_slice(ENCRYPTED_FRAME_MAGIC);
            out.extend_from_slice(&body);
            Ok(out)
        }
    }
}

pub fn decode_transport_frame(bytes: &[u8], shared_key: Option<&SharedKey>) -> Result<Frame, Error> {
    match shared_key {
        None => {
            if bytes.starts_with(ENCRYPTED_FRAME_MAGIC) {
                return Err(Error::new(
                    ErrorKind::PermissionDenied,
                    "received encrypted transport frame but local key is not configured",
                ));
            }
            codec::decode_frame(bytes)
        }
        Some(shared_key) => {
            if !bytes.starts_with(ENCRYPTED_FRAME_MAGIC) {
                return Err(Error::new(
                    ErrorKind::PermissionDenied,
                    "received unencrypted transport frame but local key is configured",
                ));
            }
            let envelope: SealedTransportFrame = serde_json::from_slice(&bytes[ENCRYPTED_FRAME_MAGIC.len()..])
                .map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("failed to decode encrypted transport envelope: {err}"),
                    )
                })?;
            let plain = open(
                &EncMessage {
                    nonce: envelope.nonce,
                    ciphertext: envelope.ciphertext,
                },
                shared_key.as_bytes(),
            )
            .map_err(|err| {
                Error::new(
                    ErrorKind::PermissionDenied,
                    format!("failed to decrypt transport frame: {err}"),
                )
            })?;
            codec::decode_frame(&plain)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{
        frame::{Frame, MessageType},
        message::{HelloMessage, Message},
    };

    use super::{decode_transport_frame, encode_transport_frame, SharedKey};

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
}
