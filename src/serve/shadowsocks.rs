use std::io::{Error, ErrorKind};

use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::crypto::aead::{decrypt, encrypt, AES_GCM_KEY_LENGTH, AES_GCM_NONCE_LENGTH};
use crate::utils::url::ParsedUrl;
use serde::{Deserialize, Serialize};

const METHOD_NONE: &str = "none";
const METHOD_AES_256_GCM_SIV: &str = "aes-256-gcm-siv";
const AEAD_FRAME_MAGIC: &[u8; 4] = b"FSS1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowsocksService {
    pub bind_host: String,
    pub bind_port: u16,
    pub method: String,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksRequest {
    pub target_host: String,
    pub target_port: u16,
    pub initial_payload: Vec<u8>,
}

impl ShadowsocksService {
    pub fn from_url(url: &ParsedUrl) -> Result<Self, Error> {
        if url.scheme != "ss" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected ss scheme, got {}", url.scheme),
            ));
        }

        let bind_host = url.host.clone().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "shadowsocks bind host is missing")
        })?;
        let bind_port = url.port.ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "shadowsocks bind port is missing")
        })?;
        let method = url
            .query
            .get("method")
            .cloned()
            .unwrap_or_else(|| METHOD_NONE.to_string());
        if !is_supported_method(&method) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "unsupported shadowsocks method {method}; supported methods: {METHOD_NONE}, {METHOD_AES_256_GCM_SIV}"
                ),
            ));
        }
        if method == METHOD_AES_256_GCM_SIV && !url.query.contains_key("password") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "shadowsocks aes-256-gcm-siv requires password",
            ));
        }

        Ok(Self {
            bind_host,
            bind_port,
            method,
            password: url.query.get("password").cloned(),
        })
    }

    pub fn bind_label(&self) -> String {
        format!("{}:{}", self.bind_host, self.bind_port)
    }

    pub fn summary_suffix(&self) -> String {
        format!("?method={}", self.method)
    }
}

fn is_supported_method(method: &str) -> bool {
    matches!(method, METHOD_NONE | METHOD_AES_256_GCM_SIV)
}

pub fn parse_shadowsocks_request(bytes: &[u8]) -> Result<Option<ShadowsocksRequest>, Error> {
    if bytes.is_empty() {
        return Ok(None);
    }

    let atyp = bytes[0];
    let mut offset = 1usize;
    let target_host = match atyp {
        0x01 => {
            if bytes.len() < offset + 4 {
                return Ok(None);
            }
            let host = format!(
                "{}.{}.{}.{}",
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3]
            );
            offset += 4;
            host
        }
        0x03 => {
            if bytes.len() < offset + 1 {
                return Ok(None);
            }
            let len = bytes[offset] as usize;
            offset += 1;
            if bytes.len() < offset + len {
                return Ok(None);
            }
            let host = String::from_utf8(bytes[offset..offset + len].to_vec())
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
            offset += len;
            host
        }
        0x04 => {
            if bytes.len() < offset + 16 {
                return Ok(None);
            }
            let mut addr = [0u8; 16];
            addr.copy_from_slice(&bytes[offset..offset + 16]);
            offset += 16;
            std::net::Ipv6Addr::from(addr).to_string()
        }
        other => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("unsupported shadowsocks address type {other}"),
            ))
        }
    };

    if bytes.len() < offset + 2 {
        return Ok(None);
    }
    let target_port = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
    offset += 2;

    Ok(Some(ShadowsocksRequest {
        target_host,
        target_port,
        initial_payload: bytes[offset..].to_vec(),
    }))
}

pub fn parse_shadowsocks_request_for_service(
    bytes: &[u8],
    service: &ShadowsocksService,
) -> Result<Option<ShadowsocksRequest>, Error> {
    match service.method.as_str() {
        METHOD_NONE => parse_shadowsocks_request(bytes),
        METHOD_AES_256_GCM_SIV => {
            let Some(plain) = decrypt_aead_request_frame(bytes, service)? else {
                return Ok(None);
            };
            parse_shadowsocks_request(&plain)?
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidData,
                        "decrypted shadowsocks request is incomplete",
                    )
                })
                .map(Some)
        }
        method => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported shadowsocks method {method}"),
        )),
    }
}

pub fn encode_aead_request_frame(
    plain_request: &[u8],
    service: &ShadowsocksService,
) -> Result<Vec<u8>, Error> {
    if service.method != METHOD_AES_256_GCM_SIV {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "AEAD request frame requires method=aes-256-gcm-siv",
        ));
    }
    let key = derive_aead_key(service)?;
    let mut nonce = [0_u8; AES_GCM_NONCE_LENGTH];
    rand::thread_rng().fill_bytes(&mut nonce);
    let ciphertext = encrypt(plain_request, &key, &nonce).map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "failed to encrypt shadowsocks frame",
        )
    })?;
    if ciphertext.len() > u16::MAX as usize {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "shadowsocks encrypted request frame is too large",
        ));
    }
    let mut frame = Vec::with_capacity(4 + AES_GCM_NONCE_LENGTH + 2 + ciphertext.len());
    frame.extend_from_slice(AEAD_FRAME_MAGIC);
    frame.extend_from_slice(&nonce);
    frame.extend_from_slice(&(ciphertext.len() as u16).to_be_bytes());
    frame.extend_from_slice(&ciphertext);
    Ok(frame)
}

fn decrypt_aead_request_frame(
    bytes: &[u8],
    service: &ShadowsocksService,
) -> Result<Option<Vec<u8>>, Error> {
    let header_len = 4 + AES_GCM_NONCE_LENGTH + 2;
    if bytes.len() < header_len {
        return Ok(None);
    }
    if &bytes[..4] != AEAD_FRAME_MAGIC {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "invalid shadowsocks AEAD frame magic",
        ));
    }
    let nonce_start = 4;
    let nonce_end = nonce_start + AES_GCM_NONCE_LENGTH;
    let len_start = nonce_end;
    let ciphertext_len = u16::from_be_bytes([bytes[len_start], bytes[len_start + 1]]) as usize;
    let ciphertext_start = header_len;
    let ciphertext_end = ciphertext_start + ciphertext_len;
    if bytes.len() < ciphertext_end {
        return Ok(None);
    }
    let key = derive_aead_key(service)?;
    decrypt(
        &bytes[ciphertext_start..ciphertext_end],
        &key,
        &bytes[nonce_start..nonce_end],
    )
    .map(Some)
    .map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "failed to decrypt shadowsocks frame",
        )
    })
}

fn derive_aead_key(service: &ShadowsocksService) -> Result<[u8; AES_GCM_KEY_LENGTH], Error> {
    let password = service.password.as_deref().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "shadowsocks AEAD method requires password",
        )
    })?;
    let digest = Sha256::digest(password.as_bytes());
    let mut key = [0_u8; AES_GCM_KEY_LENGTH];
    key.copy_from_slice(&digest[..AES_GCM_KEY_LENGTH]);
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::{
        encode_aead_request_frame, parse_shadowsocks_request,
        parse_shadowsocks_request_for_service, ShadowsocksService,
    };
    use crate::utils::url::ParsedUrl;

    #[test]
    fn parse_shadowsocks_service() {
        let service = ShadowsocksService::from_url(
            &ParsedUrl::parse("ss://127.0.0.1:8388?method=none").unwrap(),
        )
        .unwrap();
        assert_eq!(service.bind_label(), "127.0.0.1:8388");
        assert_eq!(service.summary_suffix(), "?method=none");
    }

    #[test]
    fn parse_domain_request_with_payload() {
        let request = parse_shadowsocks_request(&[
            0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm', 0x01,
            0xbb, b'h', b'e', b'l', b'l', b'o',
        ])
        .unwrap()
        .unwrap();
        assert_eq!(request.target_host, "example.com");
        assert_eq!(request.target_port, 443);
        assert_eq!(request.initial_payload, b"hello");
    }

    #[test]
    fn parse_ipv4_request_without_payload() {
        let request = parse_shadowsocks_request(&[0x01, 127, 0, 0, 1, 0x00, 0x50])
            .unwrap()
            .unwrap();
        assert_eq!(request.target_host, "127.0.0.1");
        assert_eq!(request.target_port, 80);
        assert!(request.initial_payload.is_empty());
    }

    #[test]
    fn parse_aead_service_requires_password() {
        let err = ShadowsocksService::from_url(
            &ParsedUrl::parse("ss://127.0.0.1:8388?method=aes-256-gcm-siv").unwrap(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("requires password"));
    }

    #[test]
    fn parse_aead_encrypted_request_frame() {
        let service = ShadowsocksService::from_url(
            &ParsedUrl::parse("ss://127.0.0.1:8388?method=aes-256-gcm-siv&password=secret")
                .unwrap(),
        )
        .unwrap();
        let plain = [
            0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c', b'o', b'm', 0x01,
            0xbb, b'h', b'e', b'l', b'l', b'o',
        ];
        let frame = encode_aead_request_frame(&plain, &service).unwrap();
        let request = parse_shadowsocks_request_for_service(&frame, &service)
            .unwrap()
            .unwrap();
        assert_eq!(request.target_host, "example.com");
        assert_eq!(request.target_port, 443);
        assert_eq!(request.initial_payload, b"hello");
    }
}
