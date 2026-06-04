use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
};

use crate::utils::url::ParsedUrl;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortForwardService {
    pub listen_host: String,
    pub listen_port: u16,
    pub target_host: String,
    pub target_port: u16,
}

impl PortForwardService {
    pub fn from_url(url: &ParsedUrl) -> Result<Self, Error> {
        if url.scheme != "port" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected port scheme, got {}", url.scheme),
            ));
        }

        let listen_host = url.host.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "port forward service missing listen host",
            )
        })?;
        let listen_port = url.port.ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "port forward service missing listen port",
            )
        })?;
        let target_host = url
            .query
            .get("target_host")
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "port forward missing target_host"))?;
        let target_port = url
            .query
            .get("target_port")
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "port forward missing target_port"))?
            .parse::<u16>()
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid port forward target_port: {err}"),
                )
            })?;

        Ok(Self {
            listen_host,
            listen_port,
            target_host,
            target_port,
        })
    }

    pub fn bind_label(&self) -> String {
        format!("{}:{}", self.listen_host, self.listen_port)
    }

    pub fn target_label(&self) -> String {
        format!("{}:{}", self.target_host, self.target_port)
    }

    pub fn summary_label(&self) -> String {
        format!("{}->{}", self.bind_label(), self.target_label())
    }

    pub async fn bind_listener(&self) -> Result<TcpListener, Error> {
        TcpListener::bind(self.bind_label()).await
    }

    pub async fn connect_target(&self) -> Result<TcpStream, Error> {
        TcpStream::connect(self.target_label()).await
    }
}

pub async fn proxy_connection(mut inbound: TcpStream, service: PortForwardService) -> Result<(), Error> {
    let mut outbound = service.connect_target().await?;
    let _ = copy_bidirectional(&mut inbound, &mut outbound).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{proxy_connection, PortForwardService};
    use crate::utils::url::ParsedUrl;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    #[test]
    fn parse_port_forward_service() {
        let url = ParsedUrl::parse("port://127.0.0.1:8080->example.com:80").unwrap();
        let service = PortForwardService::from_url(&url).unwrap();
        assert_eq!(service.listen_host, "127.0.0.1");
        assert_eq!(service.listen_port, 8080);
        assert_eq!(service.target_host, "example.com");
        assert_eq!(service.target_port, 80);
    }

    #[tokio::test]
    async fn proxy_connection_roundtrip() {
        let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            let mut buf = [0_u8; 64];
            let n = stream.read(&mut buf).await.unwrap();
            stream.write_all(&buf[..n]).await.unwrap();
        });

        let ingress_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ingress_addr = ingress_listener.local_addr().unwrap();
        let service = PortForwardService {
            listen_host: "127.0.0.1".into(),
            listen_port: ingress_addr.port(),
            target_host: "127.0.0.1".into(),
            target_port: echo_addr.port(),
        };

        let proxy_service = service.clone();
        tokio::spawn(async move {
            let (inbound, _) = ingress_listener.accept().await.unwrap();
            proxy_connection(inbound, proxy_service).await.unwrap();
        });

        let mut client = TcpStream::connect(ingress_addr).await.unwrap();
        client.write_all(b"port-forward-ok").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"port-forward-ok");
    }
}
