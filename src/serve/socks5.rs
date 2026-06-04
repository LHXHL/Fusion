use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{protocol::message::StreamOpenMessage, utils::url::ParsedUrl};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Socks5Service {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Socks5Address {
    IpV4([u8; 4]),
    Domain(String),
    IpV6([u8; 16]),
}

impl Socks5Address {
    pub fn host_string(&self) -> String {
        match self {
            Self::IpV4(addr) => format!("{}.{}.{}.{}", addr[0], addr[1], addr[2], addr[3]),
            Self::Domain(domain) => domain.clone(),
            Self::IpV6(addr) => std::net::Ipv6Addr::from(*addr).to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Socks5ConnectRequest {
    pub address: Socks5Address,
    pub port: u16,
}

impl Socks5ConnectRequest {
    pub fn target_host(&self) -> String {
        self.address.host_string()
    }

    pub fn to_stream_open_message(&self, service: &str) -> StreamOpenMessage {
        StreamOpenMessage {
            service: service.to_string(),
            target_host: Some(self.target_host()),
            target_port: Some(self.port),
        }
    }
}

impl Socks5Service {
    pub fn from_url(url: &ParsedUrl) -> Result<Self, Error> {
        if url.scheme != "socks5" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected socks5 scheme, got {}", url.scheme),
            ));
        }

        let host = url
            .host
            .clone()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "socks5 service missing host"))?;
        let port = url
            .port
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "socks5 service missing port"))?;

        Ok(Self { host, port })
    }

    pub fn bind_label(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub async fn accept_no_auth<S>(stream: &mut S) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let version = stream.read_u8().await?;
    if version != 0x05 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("unsupported socks version {version}"),
        ));
    }

    let nmethods = stream.read_u8().await? as usize;
    let mut methods = vec![0_u8; nmethods];
    stream.read_exact(&mut methods).await?;

    if !methods.contains(&0x00) {
        stream.write_all(&[0x05, 0xFF]).await?;
        stream.flush().await?;
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "client does not support no-auth method",
        ));
    }

    stream.write_all(&[0x05, 0x00]).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_connect_request<S>(stream: &mut S) -> Result<Socks5ConnectRequest, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let version = stream.read_u8().await?;
    if version != 0x05 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("unsupported socks request version {version}"),
        ));
    }

    let command = stream.read_u8().await?;
    if command != 0x01 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported socks command {command}"),
        ));
    }

    let _reserved = stream.read_u8().await?;
    let atyp = stream.read_u8().await?;

    let address = match atyp {
        0x01 => {
            let mut octets = [0_u8; 4];
            stream.read_exact(&mut octets).await?;
            Socks5Address::IpV4(octets)
        }
        0x03 => {
            let len = stream.read_u8().await? as usize;
            let mut domain = vec![0_u8; len];
            stream.read_exact(&mut domain).await?;
            let domain = String::from_utf8(domain)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
            Socks5Address::Domain(domain)
        }
        0x04 => {
            let mut octets = [0_u8; 16];
            stream.read_exact(&mut octets).await?;
            Socks5Address::IpV6(octets)
        }
        other => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("unsupported socks address type {other}"),
            ))
        }
    };

    let port = stream.read_u16().await?;
    Ok(Socks5ConnectRequest { address, port })
}

pub async fn write_success_response<S>(stream: &mut S) -> Result<(), Error>
where
    S: AsyncWrite + Unpin,
{
    let response = [0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0];
    stream.write_all(&response).await?;
    stream.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    use crate::{
        serve::socks5::{
            accept_no_auth, read_connect_request, write_success_response, Socks5Address,
            Socks5Service,
        },
        utils::url::ParsedUrl,
    };

    #[test]
    fn parse_socks5_service() {
        let url = ParsedUrl::parse("socks5://127.0.0.1:1080").unwrap();
        let svc = Socks5Service::from_url(&url).unwrap();
        assert_eq!(svc.bind_label(), "127.0.0.1:1080");
    }

    #[tokio::test]
    async fn socks5_no_auth_handshake_and_connect_request() {
        let (mut client, mut server) = duplex(128);

        let server_task = tokio::spawn(async move {
            accept_no_auth(&mut server).await.unwrap();
            let req = read_connect_request(&mut server).await.unwrap();
            write_success_response(&mut server).await.unwrap();
            req
        });

        client.write_all(&[0x05, 0x01, 0x00]).await.unwrap();
        let mut method_reply = [0_u8; 2];
        client.read_exact(&mut method_reply).await.unwrap();
        assert_eq!(method_reply, [0x05, 0x00]);

        client
            .write_all(&[
                0x05, 0x01, 0x00, 0x03, 0x0b, b'e', b'x', b'a', b'm', b'p', b'l', b'e', b'.', b'c',
                b'o', b'm', 0x00, 0x50,
            ])
            .await
            .unwrap();

        let mut connect_reply = [0_u8; 10];
        client.read_exact(&mut connect_reply).await.unwrap();
        assert_eq!(connect_reply[0..2], [0x05, 0x00]);

        let req = server_task.await.unwrap();
        assert_eq!(
            req.address,
            Socks5Address::Domain("example.com".to_string())
        );
        assert_eq!(req.port, 80);
        let open = req.to_stream_open_message("raw");
        assert_eq!(open.service, "raw");
        assert_eq!(open.target_host.as_deref(), Some("example.com"));
        assert_eq!(open.target_port, Some(80));
    }
}
