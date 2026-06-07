use std::io::{Error, ErrorKind};

use data_encoding::HEXLOWER;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha224};

use crate::utils::url::ParsedUrl;

pub const TROJAN_HASH_LEN: usize = 56;
const CMD_CONNECT: u8 = 0x01;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrojanService {
    pub bind_host: String,
    pub bind_port: u16,
    pub password: String,
    pub password_hash: String,
    pub tls_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrojanRequest {
    pub target_host: String,
    pub target_port: u16,
    pub initial_payload: Vec<u8>,
}

impl TrojanService {
    pub fn from_url(url: &ParsedUrl) -> Result<Self, Error> {
        if url.scheme != "trojan" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected trojan scheme, got {}", url.scheme),
            ));
        }

        let bind_host = url.host.clone().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "trojan bind host is missing")
        })?;
        let bind_port = url.port.ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "trojan bind port is missing")
        })?;
        let password = url.query.get("password").cloned().ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "trojan service requires password")
        })?;

        let tls_enabled = url.query.contains_key("tls-cert") || url.query.contains_key("tls-key");
        if url.query.contains_key("tls-cert") ^ url.query.contains_key("tls-key") {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "trojan tls requires both tls-cert and tls-key",
            ));
        }

        Ok(Self {
            bind_host,
            bind_port,
            password_hash: trojan_password_hash(&password),
            password,
            tls_enabled,
        })
    }

    pub fn bind_label(&self) -> String {
        format!("{}:{}", self.bind_host, self.bind_port)
    }

    pub fn summary_suffix(&self) -> String {
        if self.tls_enabled {
            "?password=<hidden>&tls=1".to_string()
        } else {
            "?password=<hidden>".to_string()
        }
    }
}

pub fn trojan_password_hash(password: &str) -> String {
    HEXLOWER.encode(&Sha224::digest(password.as_bytes()))
}

pub fn parse_trojan_request(bytes: &[u8], expected_hash: &str) -> Result<Option<TrojanRequest>, Error> {
    if bytes.len() < TROJAN_HASH_LEN + 4 {
        return Ok(None);
    }

    let hash = std::str::from_utf8(&bytes[..TROJAN_HASH_LEN]).map_err(|err| {
        Error::new(
            ErrorKind::InvalidData,
            format!("trojan password hash is not valid utf-8: {err}"),
        )
    })?;
    if hash != expected_hash {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "invalid trojan password",
        ));
    }
    if &bytes[TROJAN_HASH_LEN..TROJAN_HASH_LEN + 2] != b"\r\n" {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected CRLF after trojan password hash",
        ));
    }

    let mut offset = TROJAN_HASH_LEN + 2;
    let cmd = bytes[offset];
    offset += 1;
    if cmd != CMD_CONNECT {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported trojan command {cmd}; only TCP connect (1) is supported"),
        ));
    }

    if bytes.len() <= offset {
        return Ok(None);
    }
    let atyp = bytes[offset];
    offset += 1;
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
                .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
            offset += len;
            host
        }
        0x04 => {
            if bytes.len() < offset + 16 {
                return Ok(None);
            }
            let mut addr = [0_u8; 16];
            addr.copy_from_slice(&bytes[offset..offset + 16]);
            offset += 16;
            std::net::Ipv6Addr::from(addr).to_string()
        }
        other => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("unsupported trojan address type {other}"),
            ))
        }
    };

    if bytes.len() < offset + 2 {
        return Ok(None);
    }
    let target_port = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
    offset += 2;

    if bytes.len() < offset + 2 {
        return Ok(None);
    }
    if &bytes[offset..offset + 2] != b"\r\n" {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected CRLF after trojan target address",
        ));
    }
    offset += 2;

    Ok(Some(TrojanRequest {
        target_host,
        target_port,
        initial_payload: bytes[offset..].to_vec(),
    }))
}

pub fn encode_trojan_connect_request(
    password: &str,
    target_host: &str,
    target_port: u16,
    payload: &[u8],
) -> Result<Vec<u8>, Error> {
    let mut out = trojan_password_hash(password).into_bytes();
    out.extend_from_slice(b"\r\n");
    out.push(CMD_CONNECT);

    if let Ok(addr) = target_host.parse::<std::net::Ipv4Addr>() {
        out.push(0x01);
        out.extend_from_slice(&addr.octets());
    } else if let Ok(addr) = target_host.parse::<std::net::Ipv6Addr>() {
        out.push(0x04);
        out.extend_from_slice(&addr.octets());
    } else {
        if target_host.len() > u8::MAX as usize {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "trojan domain target is too long",
            ));
        }
        out.push(0x03);
        out.push(target_host.len() as u8);
        out.extend_from_slice(target_host.as_bytes());
    }

    out.extend_from_slice(&target_port.to_be_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(payload);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::{
        encode_trojan_connect_request, parse_trojan_request, trojan_password_hash, TrojanService,
    };
    use std::io::ErrorKind;
    use crate::utils::url::ParsedUrl;

    #[test]
    fn parse_trojan_service_url() {
        let url = ParsedUrl::parse("trojan://127.0.0.1:443?password=secret").unwrap();
        let service = TrojanService::from_url(&url).unwrap();
        assert_eq!(service.bind_host, "127.0.0.1");
        assert_eq!(service.bind_port, 443);
        assert_eq!(service.password, "secret");
        assert!(!service.tls_enabled);
        assert_eq!(service.password_hash.len(), 56);
    }

    #[test]
    fn trojan_password_hash_is_lowercase_sha224_hex() {
        let hash = trojan_password_hash("password");
        assert_eq!(hash.len(), 56);
        assert!(hash.chars().all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase()));
    }

    #[test]
    fn trojan_connect_request_roundtrip() {
        let password = "secret";
        let encoded = encode_trojan_connect_request(password, "127.0.0.1", 8080, b"ping").unwrap();
        let parsed = parse_trojan_request(&encoded, &trojan_password_hash(password))
            .unwrap()
            .unwrap();
        assert_eq!(parsed.target_host, "127.0.0.1");
        assert_eq!(parsed.target_port, 8080);
        assert_eq!(parsed.initial_payload, b"ping");
    }

    #[test]
    fn trojan_rejects_invalid_password() {
        let encoded = encode_trojan_connect_request("secret", "127.0.0.1", 80, b"").unwrap();
        let err = parse_trojan_request(&encoded, &trojan_password_hash("wrong"))
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
    }
}
