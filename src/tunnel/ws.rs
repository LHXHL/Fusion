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
use tokio_tungstenite::{
    accept_async, connect_async,
    tungstenite::{self, Message as WsMessage},
    MaybeTlsStream, WebSocketStream,
};

use crate::{
    agent::identity::AgentIdentity,
    protocol::{
        codec::{decode_frame, encode_frame},
        frame::Frame,
        message::Message,
    },
    session::{
        handshake::{complete_session, hello_ack_frame, hello_frame},
        heartbeat::heartbeat_frame,
        peer::{PeerInfo, PeerSession, SessionState},
    },
};

type BoxWsSink = Pin<Box<dyn Sink<WsMessage, Error = tungstenite::Error> + Send>>;
type BoxWsStream = Pin<Box<dyn Stream<Item = Result<WsMessage, tungstenite::Error>> + Send>>;

pub struct ActiveWsPeer {
    pub session: PeerSession,
    pub peer_addr: SocketAddr,
    writer: BoxWsSink,
    reader: BoxWsStream,
}

impl ActiveWsPeer {
    pub async fn send_frame(&mut self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_frame(frame)?;
        self.writer
            .send(WsMessage::Binary(payload.into()))
            .await
            .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
    }

    pub async fn read_frame(&mut self) -> Result<Frame, Error> {
        read_boxed_frame(&mut self.reader).await
    }
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

async fn write_frame<S>(stream: &mut WebSocketStream<S>, frame: &Frame) -> Result<(), Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = encode_frame(frame)?;
    stream
        .send(WsMessage::Binary(payload.into()))
        .await
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))
}

async fn read_frame<S>(stream: &mut WebSocketStream<S>) -> Result<Frame, Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let msg = stream
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "websocket closed before frame"))?
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;

    frame_from_ws_message(msg)
}

fn frame_from_ws_message(msg: WsMessage) -> Result<Frame, Error> {
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

    decode_frame(&bytes)
}

async fn read_boxed_frame(stream: &mut BoxWsStream) -> Result<Frame, Error> {
    let msg = stream
        .next()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "websocket closed before frame"))?
        .map_err(|e| Error::new(ErrorKind::BrokenPipe, e.to_string()))?;
    frame_from_ws_message(msg)
}

fn split_boxed_tcp(ws: WebSocketStream<TcpStream>) -> (BoxWsSink, BoxWsStream) {
    let (writer, reader): (SplitSink<_, _>, SplitStream<_>) = ws.split();
    (Box::pin(writer), Box::pin(reader))
}

fn split_boxed_client(ws: WebSocketStream<MaybeTlsStream<TcpStream>>) -> (BoxWsSink, BoxWsStream) {
    let (writer, reader): (SplitSink<_, _>, SplitStream<_>) = ws.split();
    (Box::pin(writer), Box::pin(reader))
}

fn peer_addr_from_client_ws(
    ws: &WebSocketStream<MaybeTlsStream<TcpStream>>,
) -> Result<SocketAddr, Error> {
    if let MaybeTlsStream::Plain(stream) = ws.get_ref() {
        stream.peer_addr()
    } else {
        Err(Error::new(
            ErrorKind::Unsupported,
            "tls websocket peer_addr lookup is not wired yet",
        ))
    }
}

pub async fn accept_peer(
    identity: AgentIdentity,
    listener: TcpListener,
) -> Result<ActiveWsPeer, Error> {
    accept_peer_on(identity, &listener).await
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: &TcpListener,
) -> Result<ActiveWsPeer, Error> {
    let (stream, addr) = listener.accept().await?;
    let mut ws_stream = accept_async(stream)
        .await
        .map_err(|e| Error::new(ErrorKind::ConnectionAborted, e.to_string()))?;

    let hello = read_frame(&mut ws_stream).await?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    write_frame(&mut ws_stream, &ack).await?;

    let heartbeat = read_frame(&mut ws_stream).await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(identity.id.clone(), Some(session.remote.agent_id.clone()));
    write_frame(&mut ws_stream, &heartbeat_ack).await?;

    let (writer, reader) = split_boxed_tcp(ws_stream);
    info!("ws inbound session active from {addr}");
    Ok(ActiveWsPeer {
        session,
        peer_addr: addr,
        writer,
        reader,
    })
}

pub async fn connect_peer(identity: AgentIdentity, endpoint: &str) -> Result<ActiveWsPeer, Error> {
    let (mut ws_stream, _) = connect_async(endpoint)
        .await
        .map_err(|e| Error::new(ErrorKind::ConnectionRefused, e.to_string()))?;
    let peer_addr = peer_addr_from_client_ws(&ws_stream)?;

    let hello = hello_frame(&identity);
    write_frame(&mut ws_stream, &hello).await?;

    let ack = read_frame(&mut ws_stream).await?;
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
    write_frame(&mut ws_stream, &heartbeat).await?;
    let heartbeat_ack = read_frame(&mut ws_stream).await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }

    let (writer, reader) = split_boxed_client(ws_stream);
    info!("ws outbound session active to {endpoint}");
    Ok(ActiveWsPeer {
        session,
        peer_addr,
        writer,
        reader,
    })
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: TcpListener,
) -> Result<(PeerSession, Vec<Frame>, SocketAddr), Error> {
    let peer = accept_peer(identity, listener).await?;
    Ok((peer.session, Vec::new(), peer.peer_addr))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
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
            run_inbound_session_once(server_identity, listener)
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
            let mut peer = accept_peer(server_identity, listener).await.unwrap();
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
}
