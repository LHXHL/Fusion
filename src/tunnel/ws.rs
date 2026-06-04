use std::{
    io::{Error, ErrorKind},
    net::SocketAddr,
    pin::Pin,
};

use futures_util::{
    sink::Sink,
    stream::{SplitSink, SplitStream, Stream},
    SinkExt, StreamExt,
};
use log::info;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::{
    accept_async, connect_async, connect_async_tls_with_config,
    tungstenite::{self, Message as WsMessage},
    Connector, MaybeTlsStream, WebSocketStream,
};

use crate::{
    agent::identity::AgentIdentity,
    crypto::transport::{decode_transport_frame, encode_transport_frame, SharedKey},
    protocol::{frame::Frame, message::Message},
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
    tunnel::tls::build_ws_tls_connector,
    utils::url::ParsedUrl,
};

type BoxWsSink = Pin<Box<dyn Sink<WsMessage, Error = tungstenite::Error> + Send>>;
type BoxWsStream = Pin<Box<dyn Stream<Item = Result<WsMessage, tungstenite::Error>> + Send>>;

pub struct ActiveWsPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    writer: BoxWsSink,
    reader: BoxWsStream,
    shared_key: Option<SharedKey>,
}

impl ActiveWsPeer {
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        self.writer
            .send(WsMessage::Binary(payload.into()))
            .await
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
    }

    pub async fn read_frame(&mut self) -> Result<Frame, Error> {
        read_boxed_frame(&mut self.reader, self.shared_key.as_ref()).await
    }
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

async fn write_frame<S>(
    stream: &mut WebSocketStream<S>,
    frame: &Frame,
    shared_key: Option<&SharedKey>,
) -> Result<(), Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = encode_transport_frame(frame, shared_key)?;
    stream
        .send(WsMessage::Binary(payload.into()))
        .await
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
}

async fn read_frame<S>(
    stream: &mut WebSocketStream<S>,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let msg = stream
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "websocket closed before frame"))?
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;
    frame_from_ws_message(msg, shared_key)
}

fn frame_from_ws_message(msg: WsMessage, shared_key: Option<&SharedKey>) -> Result<Frame, Error> {
    let bytes = match msg {
        WsMessage::Binary(bytes) => bytes.to_vec(),
        WsMessage::Text(text) => text.as_str().as_bytes().to_vec(),
        WsMessage::Close(_) => {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "received websocket close during frame read",
            ));
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "unsupported websocket message type",
            ));
        }
    };
    decode_transport_frame(&bytes, shared_key)
}

async fn read_boxed_frame(
    stream: &mut BoxWsStream,
    shared_key: Option<&SharedKey>,
) -> Result<Frame, Error> {
    let msg = stream
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "websocket closed before frame"))?
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;
    frame_from_ws_message(msg, shared_key)
}

fn split_boxed<S>(ws: WebSocketStream<S>) -> (BoxWsSink, BoxWsStream)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let (writer, reader): (SplitSink<_, _>, SplitStream<_>) = ws.split();
    (Box::pin(writer), Box::pin(reader))
}

fn peer_addr_from_client_ws(
    ws: &WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<SocketAddr, Error> {
    match ws.get_ref() {
        MaybeTlsStream::Plain(stream) => stream.peer_addr(),
        MaybeTlsStream::NativeTls(stream) => stream.get_ref().get_ref().get_ref().peer_addr(),
        _ => Err(Error::new(
            ErrorKind::Unsupported,
            "unsupported websocket tls transport for peer_addr lookup",
        )),
    }
}

pub async fn accept_peer(
    identity: AgentIdentity,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<ActiveWsPeer, Error> {
    accept_peer_on(identity, &listener, tls_acceptor).await
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<ActiveWsPeer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (stream, addr) = listener.accept().await?;
    match tls_acceptor {
        Some(acceptor) => {
            let tls_stream = acceptor.accept(stream).await.map_err(|e| {
                Error::new(
                    ErrorKind::ConnectionAborted,
                    format!("tls accept failed: {e}"),
                )
            })?;
            let ws_stream = accept_async(tls_stream)
                .await
                .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;
            finish_accept_peer(identity, shared_key, ws_stream, addr).await
        }
        None => {
            let ws_stream = accept_async(stream)
                .await
                .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;
            finish_accept_peer(identity, shared_key, ws_stream, addr).await
        }
    }
}

pub async fn connect_peer(identity: AgentIdentity, endpoint: &str) -> Result<ActiveWsPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let connector = build_ws_tls_connector(&parsed)?;
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (mut ws_stream, _) = connect_ws(endpoint, connector)
        .await
        .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;
    let peer_addr = peer_addr_from_client_ws(&ws_stream)?;

    let hello = hello_frame(&identity);
    write_frame(&mut ws_stream, &hello, shared_key.as_ref()).await?;

    let ack = read_frame(&mut ws_stream, shared_key.as_ref()).await?;
    match &ack.message {
        Message::HelloAck(msg) if msg.accepted => {}
        _ => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer rejected hello handshake",
            ));
        }
    }

    let session = PeerSession {
        local: PeerInfo {
            agent_id: identity.id.clone(),
            agent_name: identity.name.clone(),
            capabilities: identity.capability_labels(),
        },
        remote: PeerInfo {
            agent_id: ack.header.src_agent.clone().unwrap_or_default(),
            agent_name: ack.header.src_agent.clone().unwrap_or_default(),
            capabilities: vec!["transport:ws".to_string()],
        },
        state: SessionState::Active,
    };

    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    write_frame(&mut ws_stream, &heartbeat, shared_key.as_ref()).await?;
    let heartbeat_ack = read_frame(&mut ws_stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let (writer, reader) = split_boxed(ws_stream);
    info!("ws outbound session active to {endpoint}");
    Ok(ActiveWsPeer {
        session,
        peer_addr,
        writer,
        reader,
        shared_key,
    })
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: TcpListener,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<(PeerSession, Vec<Frame>, SocketAddr), Error> {
    let peer = accept_peer(identity, listener, tls_acceptor).await?;
    Ok((peer.session, Vec::new(), peer.peer_addr))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

async fn connect_ws(
    endpoint: &str,
    connector: Option<Connector>,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        tungstenite::handshake::client::Response,
    ),
    tungstenite::Error,
> {
    if let Some(connector) = connector {
        connect_async_tls_with_config(endpoint, None, false, Some(connector)).await
    } else {
        connect_async(endpoint).await
    }
}

async fn finish_accept_peer<S>(
    identity: AgentIdentity,
    shared_key: Option<SharedKey>,
    mut ws_stream: WebSocketStream<S>,
    addr: SocketAddr,
) -> Result<ActiveWsPeer, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let hello = read_frame(&mut ws_stream, shared_key.as_ref()).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_frame(&mut ws_stream, &ack, shared_key.as_ref()).await?;

    let heartbeat = read_frame(&mut ws_stream, shared_key.as_ref()).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_frame(&mut ws_stream, &heartbeat_ack, shared_key.as_ref()).await?;

    let (writer, reader) = split_boxed(ws_stream);
    info!("ws inbound session active from {addr}");
    Ok(ActiveWsPeer {
        session,
        peer_addr: addr,
        writer,
        reader,
        shared_key,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, TaskAction, TaskRequestMessage},
        },
        tunnel::ws::{
            accept_peer, bind, connect_peer, run_inbound_session_once, run_outbound_session_once,
        },
    };

    #[tokio::test]
    async fn ws_session_hello_heartbeat_roundtrip() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            run_inbound_session_once(server_identity, listener, None)
                .await
                .unwrap()
        });

        let (client_session, client_frames) =
            run_outbound_session_once(client_identity, &format!("ws://{}/tunnel", addr))
                .await
                .unwrap();
        let (server_session, server_frames, _) = server_task.await.unwrap();

        assert_eq!(client_session.local.agent_name, "ws-client");
        assert_eq!(server_session.local.agent_name, "ws-server");
        assert!(client_frames.is_empty());
        assert!(server_frames.is_empty());
    }

    #[tokio::test]
    async fn ws_peer_can_exchange_task_frames_after_handshake() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-task-server".to_string()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-task-client".to_string()),
            key: None,
        });

        let server_task = tokio::spawn(async move {
            let mut peer = accept_peer(server_identity, listener, None).await.unwrap();
            let frame = peer.read_frame().await.unwrap();
            match frame.message {
                Message::TaskRequest(req) => {
                    assert!(matches!(req.action, TaskAction::Shell));
                    assert_eq!(req.args.first().map(String::as_str), Some("whoami"));
                }
                other => panic!("unexpected message: {:?}", other),
            }
        });

        let mut client_peer = connect_peer(client_identity, &format!("ws://{}/tunnel", addr))
            .await
            .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "ws-task-1".into(),
                action: TaskAction::Shell,
                args: vec!["whoami".into()],
                data_hex: None,
            }),
        );
        client_peer.send_frame(&frame).await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn ws_peer_can_exchange_frames_with_shared_key() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-server-keyed".to_string()),
            key: Some("shared-secret".to_string()),
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-client-keyed".to_string()),
            key: Some("shared-secret".to_string()),
        });

        let server_task = tokio::spawn(async move {
            let mut peer = accept_peer(server_identity, listener, None).await.unwrap();
            peer.read_frame().await.unwrap()
        });

        let mut client_peer = connect_peer(client_identity, &format!("ws://{}/tunnel", addr))
            .await
            .unwrap();
        let frame = Frame::new(
            MessageType::TaskRequest,
            Some(client_peer.session.local.agent_id.clone()),
            Some(client_peer.session.remote.agent_id.clone()),
            Message::TaskRequest(TaskRequestMessage {
                task_id: "ws-task-keyed".into(),
                action: TaskAction::Shell,
                args: vec!["id".into()],
                data_hex: None,
            }),
        );
        client_peer.send_frame(&frame).await.unwrap();

        let frame = server_task.await.unwrap();
        match frame.message {
            Message::TaskRequest(req) => {
                assert_eq!(req.task_id, "ws-task-keyed");
                assert_eq!(req.args.first().map(String::as_str), Some("id"));
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[tokio::test]
    async fn ws_handshake_rejects_mismatched_shared_key_modes() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-server-key-mismatch".to_string()),
            key: Some("shared-secret".to_string()),
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-client-key-mismatch".to_string()),
            key: None,
        });

        let server_task =
            tokio::spawn(async move { accept_peer(server_identity, listener, None).await });
        let client_result = connect_peer(client_identity, &format!("ws://{}/tunnel", addr)).await;
        assert!(client_result.is_err());
        assert!(server_task.await.unwrap().is_err());
    }
}
