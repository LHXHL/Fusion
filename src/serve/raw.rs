use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

use crate::{
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, StreamCloseMessage, StreamDataMessage, StreamOpenMessage},
    },
    tunnel::{
        simplex_dns_mux::MuxSimplexDnsPeer, simplex_http_mux::MuxSimplexHttpPeer,
        simplex_oss_mux::MuxSimplexOssPeer, tcp::ActiveTcpPeer, tcp_mux::MuxTcpPeer,
        ws_mux::MuxWsPeer,
        h2_mux::MuxH2Peer,
    },
    utils::url::ParsedUrl,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawService {
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawTarget {
    pub host: String,
    pub port: u16,
}

impl RawTarget {
    pub fn addr_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub async fn connect(&self) -> Result<TcpStream, Error> {
        TcpStream::connect(self.addr_string()).await
    }
}

impl RawService {
    pub fn from_url(url: &ParsedUrl) -> Result<Self, Error> {
        if url.scheme != "raw" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected raw scheme, got {}", url.scheme),
            ));
        }

        Ok(Self {
            host: url.host.clone(),
            port: url.port,
        })
    }

    pub fn target_label(&self) -> String {
        match (&self.host, self.port) {
            (Some(host), Some(port)) => format!("{}:{}", host, port),
            _ => "dynamic".to_string(),
        }
    }

    pub fn resolve_target(
        &self,
        requested_host: Option<&str>,
        requested_port: Option<u16>,
    ) -> Result<RawTarget, Error> {
        let host = self
            .host
            .clone()
            .or_else(|| requested_host.map(ToString::to_string))
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "raw target host is missing"))?;
        let port = self
            .port
            .or(requested_port)
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "raw target port is missing"))?;

        Ok(RawTarget { host, port })
    }
}

pub fn target_from_stream_open(
    service: &RawService,
    open: &StreamOpenMessage,
) -> Result<RawTarget, Error> {
    if open.service != "raw" {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported stream open service {}", open.service),
        ));
    }

    service.resolve_target(open.target_host.as_deref(), open.target_port)
}

pub async fn proxy_ws_mux_stream_loop(
    peer: MuxWsPeer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    while let Some(frame) = rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for ws mux raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message in ws mux raw stream loop: {:?}", other),
                ))
            }
        }
    }

    Ok(())
}

pub async fn proxy_h2_mux_stream_loop(
    peer: MuxH2Peer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    while let Some(frame) = rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for h2 mux raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message in h2 mux raw stream loop: {:?}", other),
                ))
            }
        }
    }

    Ok(())
}
pub async fn proxy_mux_stream_loop(
    peer: MuxTcpPeer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    while let Some(frame) = rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for mux raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message in mux raw stream loop: {:?}", other),
                ))
            }
        }
    }

    Ok(())
}

pub async fn proxy_simplex_mux_stream_loop(
    peer: MuxSimplexHttpPeer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    while let Some(frame) = rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for simplex mux raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "unexpected message in simplex mux raw stream loop: {:?}",
                        other
                    ),
                ))
            }
        }
    }

    Ok(())
}

pub async fn proxy_simplex_oss_mux_stream_loop(
    peer: MuxSimplexOssPeer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    while let Some(frame) = rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for simplex oss mux raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "unexpected message in simplex oss mux raw stream loop: {:?}",
                        other
                    ),
                ))
            }
        }
    }

    Ok(())
}

pub async fn proxy_simplex_dns_mux_stream_loop(
    peer: MuxSimplexDnsPeer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
    mut rx: mpsc::Receiver<Frame>,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    while let Some(frame) = rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for simplex dns mux raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "unexpected message in simplex dns mux raw stream loop: {:?}",
                        other
                    ),
                ))
            }
        }
    }

    Ok(())
}
pub async fn proxy_stream_loop(
    peer: &mut ActiveTcpPeer,
    service: &RawService,
    open: StreamOpenMessage,
    stream_id: u32,
) -> Result<(), Error> {
    let target = target_from_stream_open(service, &open)?;
    let mut target_stream = target.connect().await?;

    loop {
        let frame = peer.read_frame().await?;
        if frame.header.stream_id != Some(stream_id) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unexpected stream id for raw stream loop",
            ));
        }

        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                target_stream.write_all(&bytes).await?;
                target_stream.flush().await?;

                let mut buf = vec![0_u8; 4096];
                let n = target_stream.read(&mut buf).await?;
                if n > 0 {
                    let response = Frame::new(
                        MessageType::StreamData,
                        Some(peer.session.local.agent_id.clone()),
                        Some(peer.session.remote.agent_id.clone()),
                        Message::StreamData(StreamDataMessage::from_bytes(&buf[..n])),
                    )
                    .with_stream_id(stream_id);
                    peer.send_frame(&response).await?;
                }
            }
            Message::StreamClose(_) => {
                let ack = Frame::new(
                    MessageType::StreamClose,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::StreamClose(StreamCloseMessage {
                        reason: Some("ok".to_string()),
                    }),
                )
                .with_stream_id(stream_id);
                peer.send_frame(&ack).await?;
                break;
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("unexpected message in raw stream loop: {:?}", other),
                ))
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, StreamCloseMessage, StreamDataMessage, StreamOpenMessage},
        },
        serve::{
            raw::{proxy_stream_loop, target_from_stream_open, RawService},
            socks5::{Socks5Address, Socks5ConnectRequest},
        },
        tunnel::tcp::{accept_peer, bind, connect_peer},
        utils::url::ParsedUrl,
    };

    #[test]
    fn parse_raw_service() {
        let url = ParsedUrl::parse("raw://example.com:80").unwrap();
        let svc = RawService::from_url(&url).unwrap();
        assert_eq!(svc.target_label(), "example.com:80");
    }

    #[test]
    fn resolve_dynamic_target_from_stream_open() {
        let url = ParsedUrl::parse("raw://").unwrap();
        let svc = RawService::from_url(&url).unwrap();
        let open = StreamOpenMessage {
            service: "raw".to_string(),
            target_host: Some("example.com".to_string()),
            target_port: Some(443),
        };
        let target = target_from_stream_open(&svc, &open).unwrap();
        assert_eq!(target.addr_string(), "example.com:443");
    }

    #[tokio::test]
    async fn connect_to_local_raw_target() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (_stream, peer) = listener.accept().await.unwrap();
            peer
        });

        let svc = RawService {
            host: Some("127.0.0.1".to_string()),
            port: Some(addr.port()),
        };
        let target = svc.resolve_target(None, None).unwrap();
        let _client = target.connect().await.unwrap();
        let peer = server_task.await.unwrap();
        assert_eq!(peer.ip().to_string(), "127.0.0.1");
    }

    #[tokio::test]
    async fn raw_proxy_stream_loop_over_tunnel_multiple_chunks() {
        let echo_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo_listener.local_addr().unwrap();
        let echo_task = tokio::spawn(async move {
            let (mut stream, _) = echo_listener.accept().await.unwrap();
            for _ in 0..2 {
                let mut buf = [0_u8; 1024];
                let n = stream.read(&mut buf).await.unwrap();
                stream.write_all(&buf[..n]).await.unwrap();
            }
        });

        let tunnel_listener = bind("127.0.0.1:0").await.unwrap();
        let tunnel_addr = tunnel_listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("raw-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("raw-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let mut peer = accept_peer(server_identity, tunnel_listener).await.unwrap();
            let open_frame = peer.read_frame().await.unwrap();
            let open = match open_frame.message {
                Message::StreamOpen(open) => open,
                other => panic!("unexpected message: {:?}", other),
            };
            proxy_stream_loop(
                &mut peer,
                &RawService::from_url(&ParsedUrl::parse("raw://").unwrap()).unwrap(),
                open,
                1,
            )
            .await
            .unwrap();
        });

        let mut client_peer = connect_peer(client_identity, &tunnel_addr.to_string())
            .await
            .unwrap();
        let req = Socks5ConnectRequest {
            address: Socks5Address::IpV4([127, 0, 0, 1]),
            port: echo_addr.port(),
        };
        let open = Frame::new(
            MessageType::StreamOpen,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::StreamOpen(req.to_stream_open_message("raw")),
        )
        .with_stream_id(1);
        client_peer.send_frame(&open).await.unwrap();

        for payload_bytes in [b"ping".as_slice(), b"pong".as_slice()] {
            let payload = Frame::new(
                MessageType::StreamData,
                Some(client_peer.session.local.agent_id.clone()),
                Some(client_peer.session.remote.agent_id.clone()),
                Message::StreamData(StreamDataMessage::from_bytes(payload_bytes)),
            )
            .with_stream_id(1);
            client_peer.send_frame(&payload).await.unwrap();

            let response = client_peer.read_frame().await.unwrap();
            match response.message {
                Message::StreamData(data) => {
                    assert_eq!(data.to_bytes().unwrap(), payload_bytes);
                }
                other => panic!("unexpected response: {:?}", other),
            }
        }

        let close = Frame::new(
            MessageType::StreamClose,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::StreamClose(StreamCloseMessage { reason: None }),
        )
        .with_stream_id(1);
        client_peer.send_frame(&close).await.unwrap();
        let close_ack = client_peer.read_frame().await.unwrap();
        match close_ack.message {
            Message::StreamClose(msg) => assert_eq!(msg.reason.as_deref(), Some("ok")),
            other => panic!("unexpected close ack: {:?}", other),
        }

        server_task.await.unwrap();
        echo_task.await.unwrap();
    }
}
