use std::{
    io::{Cursor, Error, ErrorKind, Read, Write},
    sync::{OnceLock, RwLock},
};

use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
use rand::RngCore;
use serde::{Deserialize, Serialize};

use crate::crypto::{
    aead::{open, seal, EncMessage},
    transport::SharedKey,
};
use crate::error::{coded_io_error, ErrorCode};

const ENCRYPTED_FRAME_MAGIC: &[u8] = b"FXE1";
const COMPRESSED_FRAME_MAGIC: &[u8] = b"FXC1";
const PADDED_FRAME_MAGIC: &[u8] = b"FXP1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct WrapperConfig {
    pub compress: bool,
    pub padding: Option<usize>,
}

static GLOBAL_WRAPPER_CONFIG: OnceLock<RwLock<WrapperConfig>> = OnceLock::new();

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
        Self::from_config(&WrapperConfig::default(), shared_key)
    }

    pub fn from_config(config: &WrapperConfig, shared_key: Option<&SharedKey>) -> Self {
        let mut wrappers = Vec::new();
        if config.compress {
            wrappers.push(WrapperStage::Compression);
        }
        if let Some(padding) = config.padding.filter(|padding| *padding > 0) {
            wrappers.push(WrapperStage::Padding { bytes: padding });
        }
        if let Some(shared_key) = shared_key {
            wrappers.push(WrapperStage::SharedKeyAead(shared_key.clone()));
        }
        Self { wrappers }
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

pub fn set_global_wrapper_config(config: WrapperConfig) {
    let store = GLOBAL_WRAPPER_CONFIG.get_or_init(|| RwLock::new(WrapperConfig::default()));
    if let Ok(mut guard) = store.write() {
        *guard = config;
    }
}

pub fn global_wrapper_config() -> WrapperConfig {
    GLOBAL_WRAPPER_CONFIG
        .get_or_init(|| RwLock::new(WrapperConfig::default()))
        .read()
        .map(|guard| guard.clone())
        .unwrap_or_default()
}

#[derive(Debug, Clone)]
enum WrapperStage {
    Compression,
    Padding { bytes: usize },
    SharedKeyAead(SharedKey),
}

impl TransportWrapper for WrapperStage {
    fn wrap(&self, payload: Vec<u8>) -> Result<Vec<u8>, Error> {
        match self {
            WrapperStage::Compression => wrap_compressed(payload),
            WrapperStage::Padding { bytes } => wrap_padded(payload, *bytes),
            WrapperStage::SharedKeyAead(shared_key) => {
                let sealed = seal(&payload, shared_key.as_bytes()).map_err(|err| {
                    coded_io_error(
                        ErrorKind::InvalidData,
                        ErrorCode::WrapperEncryptFailed,
                        "failed to encrypt transport frame",
                        false,
                        Some(err.to_string()),
                    )
                })?;
                let envelope = SealedPayload {
                    nonce: sealed.nonce,
                    ciphertext: sealed.ciphertext,
                };
                let body = serde_json::to_vec(&envelope).map_err(|err| {
                    coded_io_error(
                        ErrorKind::InvalidData,
                        ErrorCode::WrapperEnvelopeDecodeFailed,
                        "failed to encode encrypted transport envelope",
                        false,
                        Some(err.to_string()),
                    )
                })?;
                let mut out = Vec::with_capacity(ENCRYPTED_FRAME_MAGIC.len() + body.len());
                out.extend_from_slice(ENCRYPTED_FRAME_MAGIC);
                out.extend_from_slice(&body);
                Ok(out)
            }
        }
    }

    fn unwrap(&self, payload: &[u8]) -> Result<Vec<u8>, Error> {
        match self {
            WrapperStage::Compression => unwrap_compressed(payload),
            WrapperStage::Padding { .. } => unwrap_padded(payload),
            WrapperStage::SharedKeyAead(shared_key) => {
                if !payload.starts_with(ENCRYPTED_FRAME_MAGIC) {
                    return Err(coded_io_error(
                        ErrorKind::PermissionDenied,
                        ErrorCode::WrapperMissingEncryption,
                        "received unencrypted transport frame but local key is configured",
                        false,
                        Some("configure the same shared key on both peers".to_string()),
                    ));
                }
                let envelope: SealedPayload = serde_json::from_slice(
                    &payload[ENCRYPTED_FRAME_MAGIC.len()..],
                )
                .map_err(|err| {
                    coded_io_error(
                        ErrorKind::InvalidData,
                        ErrorCode::WrapperEnvelopeDecodeFailed,
                        "failed to decode encrypted transport envelope",
                        false,
                        Some(err.to_string()),
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
                    coded_io_error(
                        ErrorKind::PermissionDenied,
                        ErrorCode::WrapperDecryptFailed,
                        "failed to decrypt transport frame",
                        false,
                        Some(format!(
                            "{err}; verify that both peers use the same shared key and wrapper settings"
                        )),
                    )
                })
            }
        }
    }
}

fn wrap_compressed(payload: Vec<u8>) -> Result<Vec<u8>, Error> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&payload).map_err(|err| {
        coded_io_error(
            ErrorKind::InvalidData,
            ErrorCode::WrapperCompressionFailed,
            "failed to write payload into compression wrapper",
            false,
            Some(err.to_string()),
        )
    })?;
    let body = encoder.finish().map_err(|err| {
        coded_io_error(
            ErrorKind::InvalidData,
            ErrorCode::WrapperCompressionFailed,
            "failed to finish compression wrapper",
            false,
            Some(err.to_string()),
        )
    })?;
    let mut out = Vec::with_capacity(COMPRESSED_FRAME_MAGIC.len() + body.len());
    out.extend_from_slice(COMPRESSED_FRAME_MAGIC);
    out.extend_from_slice(&body);
    Ok(out)
}

fn unwrap_compressed(payload: &[u8]) -> Result<Vec<u8>, Error> {
    if !payload.starts_with(COMPRESSED_FRAME_MAGIC) {
        return Err(coded_io_error(
            ErrorKind::PermissionDenied,
            ErrorCode::WrapperMissingCompression,
            "received uncompressed transport frame but compression wrapper is configured",
            false,
            Some("configure compression consistently on both peers".to_string()),
        ));
    }
    let mut decoder = ZlibDecoder::new(Cursor::new(&payload[COMPRESSED_FRAME_MAGIC.len()..]));
    let mut out = Vec::new();
    decoder.read_to_end(&mut out).map_err(|err| {
        coded_io_error(
            ErrorKind::InvalidData,
            ErrorCode::WrapperCompressionFailed,
            "failed to decode compressed transport frame",
            false,
            Some(err.to_string()),
        )
    })?;
    Ok(out)
}

fn wrap_padded(payload: Vec<u8>, bytes: usize) -> Result<Vec<u8>, Error> {
    let original_len = u32::try_from(payload.len()).map_err(|_| {
        coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::WrapperPaddingTooLarge,
            "payload is too large for padding wrapper header",
            false,
            None,
        )
    })?;
    let mut out = Vec::with_capacity(PADDED_FRAME_MAGIC.len() + 4 + payload.len() + bytes);
    out.extend_from_slice(PADDED_FRAME_MAGIC);
    out.extend_from_slice(&original_len.to_le_bytes());
    out.extend_from_slice(&payload);
    if bytes > 0 {
        let mut padding = vec![0_u8; bytes];
        rand::thread_rng().fill_bytes(&mut padding);
        out.extend_from_slice(&padding);
    }
    Ok(out)
}

fn unwrap_padded(payload: &[u8]) -> Result<Vec<u8>, Error> {
    if !payload.starts_with(PADDED_FRAME_MAGIC) {
        return Err(coded_io_error(
            ErrorKind::PermissionDenied,
            ErrorCode::WrapperMissingPadding,
            "received unpadded transport frame but padding wrapper is configured",
            false,
            Some("configure padding consistently on both peers".to_string()),
        ));
    }
    if payload.len() < PADDED_FRAME_MAGIC.len() + 4 {
        return Err(coded_io_error(
            ErrorKind::InvalidData,
            ErrorCode::WrapperPaddingTruncated,
            "padding wrapper header is truncated",
            false,
            None,
        ));
    }
    let mut len_buf = [0_u8; 4];
    len_buf.copy_from_slice(&payload[PADDED_FRAME_MAGIC.len()..PADDED_FRAME_MAGIC.len() + 4]);
    let original_len = u32::from_le_bytes(len_buf) as usize;
    let body = &payload[PADDED_FRAME_MAGIC.len() + 4..];
    if body.len() < original_len {
        return Err(coded_io_error(
            ErrorKind::InvalidData,
            ErrorCode::WrapperPaddingLengthInvalid,
            "padding wrapper body is shorter than declared original length",
            false,
            Some(format!(
                "declared_original_len={original_len} actual_body_len={}",
                body.len()
            )),
        ));
    }
    Ok(body[..original_len].to_vec())
}

pub fn payload_looks_wrapped(payload: &[u8]) -> bool {
    payload.starts_with(ENCRYPTED_FRAME_MAGIC)
        || payload.starts_with(COMPRESSED_FRAME_MAGIC)
        || payload.starts_with(PADDED_FRAME_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::{payload_looks_wrapped, WrapperConfig, WrapperPipeline};
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
        let err = pipeline.unwrap(b"plain-frame").err().unwrap();
        assert!(err.to_string().contains("code=wrapper.missing_encryption"));
    }

    #[test]
    fn compression_pipeline_roundtrip() {
        let payload = vec![b'A'; 4096];
        let pipeline = WrapperPipeline::from_config(
            &WrapperConfig {
                compress: true,
                padding: None,
            },
            None,
        );
        let wrapped = pipeline.wrap(payload.clone()).unwrap();
        assert!(payload_looks_wrapped(&wrapped));
        assert!(wrapped.len() < payload.len());
        let unwrapped = pipeline.unwrap(&wrapped).unwrap();
        assert_eq!(unwrapped, payload);
    }

    #[test]
    fn padding_pipeline_roundtrip() {
        let payload = b"fusion-padding".to_vec();
        let pipeline = WrapperPipeline::from_config(
            &WrapperConfig {
                compress: false,
                padding: Some(32),
            },
            None,
        );
        let wrapped = pipeline.wrap(payload.clone()).unwrap();
        assert!(wrapped.len() >= payload.len() + 32);
        let unwrapped = pipeline.unwrap(&wrapped).unwrap();
        assert_eq!(unwrapped, payload);
    }

    #[test]
    fn multi_stage_pipeline_roundtrip() {
        let payload = vec![b'Z'; 8192];
        let key = SharedKey::from_secret("fusion-secret");
        let pipeline = WrapperPipeline::from_config(
            &WrapperConfig {
                compress: true,
                padding: Some(24),
            },
            Some(&key),
        );
        let wrapped = pipeline.wrap(payload.clone()).unwrap();
        assert!(payload_looks_wrapped(&wrapped));
        let unwrapped = pipeline.unwrap(&wrapped).unwrap();
        assert_eq!(unwrapped, payload);
    }

    #[test]
    fn compression_pipeline_reports_mismatch_code() {
        let pipeline = WrapperPipeline::from_config(
            &WrapperConfig {
                compress: true,
                padding: None,
            },
            None,
        );
        let err = pipeline.unwrap(b"plain-frame").err().unwrap();
        assert!(err.to_string().contains("code=wrapper.missing_compression"));
    }

    #[test]
    fn padding_pipeline_reports_truncated_header_code() {
        let pipeline = WrapperPipeline::from_config(
            &WrapperConfig {
                compress: false,
                padding: Some(32),
            },
            None,
        );
        let err = pipeline.unwrap(b"FXP1").err().unwrap();
        assert!(err.to_string().contains("code=wrapper.padding_truncated"));
    }
}
