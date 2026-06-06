use std::io::{Error, ErrorKind};

use crate::utils::url::ParsedUrl;
use serde::{Deserialize, Serialize};

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
            .unwrap_or_else(|| "none".to_string());
        if method != "none" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "minimal shadowsocks service currently supports only method=none, got {method}"
                ),
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

#[cfg(test)]
mod tests {
    use super::{parse_shadowsocks_request, ShadowsocksService};
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
}
