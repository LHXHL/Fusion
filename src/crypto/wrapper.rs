use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};

use crate::crypto::{
    aead::{open, seal, EncMessage},
    transport::SharedKey,
};

const ENCRYPTED_FRAME_MAGIC: &[u8] = b"FXE1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SealedPayload {
    nonce: String,
    ciphertext: String,
}

pub trait TransportWrapper: Send + Sync {
    fn wrap(&self, payload: Vec<u8>) -> Result<Vec<u8>, Error>;
    fn unwrap(&self, payload: &[u8]) -> Result<Vec<u8>, Error>;
}

#[derive(Debug, Clone, Default)]
pub struct WrapperPipeline {
    wrappers: Vec<WrapperStage>,
}

impl WrapperPipeline {
    pub fn passthrough() -> Self {
        Self::default()
    }

    pub fn from_shared_key(shared_key: Option<&SharedKey>) -> Self {
        match shared_key {
            Some(shared_key) => Self {
                wrappers: vec![WrapperStage::SharedKeyAead(shared_key.clone())],
            },
            None => Self::passthrough(),
        }
    }

    pub fn wrap(&self, mut payload: Vec<u8>) -> Result<Vec<u8>, Error> {
        for wrapper in &self.wrappers {
            payload = wrapper.wrap(payload)?;
        }
        Ok(payload)
    }

    pub fn unwrap(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let mut current = payload.to_vec();
        for wrapper in self.wrappers.iter().rev() {
            current = wrapper.unwrap(&current)?;
        }
        Ok(current)
    }

    pub fn requires_wrapped_input(&self) -> bool {
        !self.wrappers.is_empty()
    }
}

#[derive(Debug, Clone)]
enum WrapperStage {
    SharedKeyAead(SharedKey),
}

impl TransportWrapper for WrapperStage {
    fn wrap(&self, payload: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self {
            WrapperStage::SharedKeyAead(shared_key) => {
                let sealed = seal(&payload, shared_key.as_bytes()).map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("failed to encrypt transport frame: {err}"),
                    )
                })?;
                let envelope = SealedPayload {
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

    fn unwrap(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        match self {
            WrapperStage::SharedKeyAead(shared_key) => {
                if !payload.starts_with(ENCRYPTED_FRAME_MAGIC) {
                    return Err(Error::new(
                        ErrorKind::PermissionDenied,
                        "received unencrypted transport frame but local key is configured",
                    ));
                }
                let envelope: SealedPayload = serde_json::from_slice(
                    &payload[ENCRYPTED_FRAME_MAGIC.len()..],
                )
                .map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidData,
                        format!("failed to decode encrypted transport envelope: {err}"),
                    )
                })?;
                open(
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
                })
            }
        }
    }
}

pub fn payload_looks_wrapped(payload: &[u8]) -> bool {
    payload.starts_with(ENCRYPTED_FRAME_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::{payload_looks_wrapped, WrapperPipeline};
    use crate::crypto::transport::SharedKey;

    #[test]
    fn passthrough_pipeline_roundtrip() {
        let plain = b"fusion".to_vec();
        let pipeline = WrapperPipeline::passthrough();
        let wrapped = pipeline.wrap(plain.clone()).unwrap();
        assert_eq!(wrapped, plain);
        let unwrapped = pipeline.unwrap(&wrapped).unwrap();
        assert_eq!(unwrapped, plain);
    }

    #[test]
    fn shared_key_pipeline_roundtrip() {
        let plain = b"fusion".to_vec();
        let key = SharedKey::from_secret("fusion-secret");
        let pipeline = WrapperPipeline::from_shared_key(Some(&key));
        let wrapped = pipeline.wrap(plain.clone()).unwrap();
        assert!(payload_looks_wrapped(&wrapped));
        let unwrapped = pipeline.unwrap(&wrapped).unwrap();
        assert_eq!(unwrapped, plain);
    }

    #[test]
    fn shared_key_pipeline_rejects_plain_payload() {
        let key = SharedKey::from_secret("fusion-secret");
        let pipeline = WrapperPipeline::from_shared_key(Some(&key));
        assert!(pipeline.unwrap(b"plain-frame").is_err());
    }
}
