use std::{
    collections::VecDeque,
    io::{Error, ErrorKind},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
    time::{sleep, Duration},
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
    utils::url::ParsedUrl,
};

const SSE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const SSE_IDLE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamHttpEnvelope {
    packets: Vec<Vec<u8>>,
}

#[derive(Clone)]
pub struct ActiveStreamHttpPeer {
    pub session: PeerSession,
    endpoint: String,
    path: String,
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    incoming: Arc<Mutex<mpsc::Receiver<Frame>>>,
    shared_key: Option<SharedKey>,
}

impl ActiveStreamHttpPeer {
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        let payload = encode_transport_frame(frame, self.shared_key.as_ref())?;
        if self.endpoint.is_empty() {
            self.outgoing.lock().await.push_back(payload);
            Ok(())
        } else {
            http_post_envelope(&self.endpoint, &self.path, &[payload]).await
        }
    }

    pub async fn read_frame(&self) -> Result<Frame, Error> {
        loop {
            {
                let mut rx = self.incoming.lock().await;
                match rx.try_recv() {
                    Ok(frame) => return Ok(frame),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "streamhttp inbound channel closed",
                        ))
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                }
            }
            sleep(SSE_POLL_INTERVAL).await;
        }
    }
}

pub async fn bind(endpoint: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(endpoint).await
}

pub async fn accept_peer_on(
    identity: AgentIdentity,
    listener: TcpListener,
    path: &str,
) -> Result<ActiveStreamHttpPeer, Error> {
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let outgoing = Arc::new(Mutex::new(VecDeque::new()));
    let (incoming_tx, incoming_rx) = mpsc::channel(64);
    let path = normalize_path(path);
    spawn_server_loop(
        listener,
        path.clone(),
        shared_key.clone(),
        outgoing.clone(),
        incoming_tx.clone(),
    );

    let mut rx = incoming_rx;
    let hello = rx
        .recv()
        .await
        .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "streamhttp hello channel closed"))?;
    let session = complete_session(&identity, &hello)?;
    let ack = hello_ack_frame(&identity, Some(session.remote.agent_id.clone()));
    let peer = ActiveStreamHttpPeer {
        session,
        endpoint: String::new(),
        path,
        outgoing,
        incoming: Arc::new(Mutex::new(rx)),
        shared_key,
    };
    peer.send_frame(&ack).await?;

    let heartbeat = peer.read_frame().await?;
    if !matches!(heartbeat.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat after hello ack",
        ));
    }
    let heartbeat_ack = heartbeat_frame(
        identity.id.clone(),
        Some(peer.session.remote.agent_id.clone()),
    );
    peer.send_frame(&heartbeat_ack).await?;
    Ok(peer)
}

pub async fn connect_peer(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<ActiveStreamHttpPeer, Error> {
    let parsed = ParsedUrl::parse(endpoint)?;
    let host = parsed.host.clone().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "missing host for streamhttp connect",
        )
    })?;
    let port = parsed.port.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "missing port for streamhttp connect",
        )
    })?;
    let base = format!("{host}:{port}");
    let path = normalize_path(&parsed.path);
    let shared_key = identity.shared_key_secret().map(SharedKey::from_secret);
    let (incoming_tx, incoming_rx) = mpsc::channel(64);
    let peer = ActiveStreamHttpPeer {
        session: PeerSession {
            local: PeerInfo {
                agent_id: identity.id.clone(),
                agent_name: identity.name.clone(),
                capabilities: identity.capability_labels(),
            },
            remote: PeerInfo {
                agent_id: String::new(),
                agent_name: String::new(),
                capabilities: vec!["transport:streamhttp".into()],
            },
            state: SessionState::Active,
        },
        endpoint: base.clone(),
        path: path.clone(),
        outgoing: Arc::new(Mutex::new(VecDeque::new())),
        incoming: Arc::new(Mutex::new(incoming_rx)),
        shared_key: shared_key.clone(),
    };

    spawn_sse_reader(
        base.clone(),
        path.clone(),
        shared_key.clone(),
        incoming_tx.clone(),
    );

    let hello = hello_frame(&identity);
    peer.send_frame(&hello).await?;
    let ack = peer.read_frame().await?;
    match &ack.message {
        Message::HelloAck(msg) if msg.accepted => {}
        _ => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "peer rejected hello handshake",
            ));
        }
    }

    let mut session = peer.session.clone();
    session.remote.agent_id = ack.header.src_agent.clone().unwrap_or_default();
    session.remote.agent_name = ack.header.src_agent.clone().unwrap_or_default();
    let connected_peer = ActiveStreamHttpPeer { session, ..peer };

    let heartbeat = heartbeat_frame(identity.id.clone(), ack.header.src_agent.clone());
    connected_peer.send_frame(&heartbeat).await?;
    let heartbeat_ack = connected_peer.read_frame().await?;
    if !matches!(heartbeat_ack.message, Message::Heartbeat(_)) {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "expected heartbeat ack from peer",
        ));
    }
    Ok(connected_peer)
}

pub async fn run_inbound_session_once(
    identity: AgentIdentity,
    listener: TcpListener,
    path: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = accept_peer_on(identity, listener, path).await?;
    Ok((peer.session, Vec::new()))
}

pub async fn run_outbound_session_once(
    identity: AgentIdentity,
    endpoint: &str,
) -> Result<(PeerSession, Vec<Frame>), Error> {
    let peer = connect_peer(identity, endpoint).await?;
    Ok((peer.session, Vec::new()))
}

fn spawn_server_loop(
    listener: TcpListener,
    path: String,
    shared_key: Option<SharedKey>,
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    incoming_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let path = path.clone();
            let shared_key = shared_key.clone();
            let outgoing = outgoing.clone();
            let incoming_tx = incoming_tx.clone();
            tokio::spawn(async move {
                let _ = handle_http_exchange(&mut stream, &path, shared_key, outgoing, incoming_tx)
                    .await;
            });
        }
    });
}

fn spawn_sse_reader(
    endpoint: String,
    path: String,
    shared_key: Option<SharedKey>,
    incoming_tx: mpsc::Sender<Frame>,
) {
    tokio::spawn(async move {
        loop {
            match read_sse_envelope(&endpoint, &path).await {
                Ok(Some(payloads)) => {
                    for payload in payloads {
                        match decode_transport_frame(&payload, shared_key.as_ref()) {
                            Ok(frame) => {
                                if incoming_tx.send(frame).await.is_err() {
                                    return;
                                }
                            }
                            Err(err) => eprintln!("streamhttp.sse.decode.error={err}"),
                        }
                    }
                }
                Ok(None) => sleep(SSE_POLL_INTERVAL).await,
                Err(err) => {
                    eprintln!("streamhttp.sse.error={err}");
                    sleep(SSE_POLL_INTERVAL).await;
                }
            }
        }
    });
}

async fn handle_http_exchange(
    stream: &mut TcpStream,
    expected_path: &str,
    shared_key: Option<SharedKey>,
    outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    incoming_tx: mpsc::Sender<Frame>,
) -> Result<(), Error> {
    let (method, path, headers, body) = read_http_request(stream).await?;
    if path != expected_path {
        write_http_response(stream, 404, b"not found").await?;
        return Ok(());
    }

    match method.as_str() {
        "POST" => {
            for payload in decode_envelope(&body)? {
                let frame = decode_transport_frame(&payload, shared_key.as_ref())?;
                incoming_tx.send(frame).await.map_err(|_| {
                    Error::new(ErrorKind::BrokenPipe, "streamhttp receiver dropped")
                })?;
            }
            write_http_response(stream, 200, b"ok").await?;
        }
        "GET" if wants_event_stream(&headers) => {
            let started = std::time::Instant::now();
            let event_body = loop {
                let payloads = {
                    let mut queue = outgoing.lock().await;
                    if queue.is_empty() {
                        None
                    } else {
                        let mut batch = Vec::new();
                        while let Some(payload) = queue.pop_front() {
                            batch.push(payload);
                        }
                        Some(batch)
                    }
                };
                if let Some(payloads) = payloads {
                    let body = encode_envelope(&payloads)?;
                    break format!("data: {}\n\n", String::from_utf8_lossy(&body));
                }
                if started.elapsed() >= SSE_IDLE_TIMEOUT {
                    break ": keepalive\n\n".to_string();
                }
                sleep(SSE_POLL_INTERVAL).await;
            };
            write_sse_response(stream, event_body.as_bytes()).await?;
        }
        _ => write_http_response(stream, 405, b"method not allowed").await?,
    }
    Ok(())
}

fn wants_event_stream(headers: &[(String, String)]) -> bool {
    headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("accept") && value.contains("text/event-stream")
    })
}

fn encode_envelope(payloads: &[Vec<u8>]) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(&StreamHttpEnvelope {
        packets: payloads.to_vec(),
    })
    .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))
}

fn decode_envelope(bytes: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let envelope: StreamHttpEnvelope = serde_json::from_slice(bytes)
        .map_err(|err| Error::new(ErrorKind::InvalidData, err.to_string()))?;
    Ok(envelope.packets)
}

async fn http_post_envelope(endpoint: &str, path: &str, payloads: &[Vec<u8>]) -> Result<(), Error> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let envelope = encode_envelope(payloads)?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {endpoint}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        envelope.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(&envelope).await?;
    stream.flush().await?;
    let (status, _) = read_http_response(&mut stream).await?;
    if status != 200 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("streamhttp POST failed with status {status}"),
        ));
    }
    Ok(())
}

async fn read_sse_envelope(endpoint: &str, path: &str) -> Result<Option<Vec<Vec<u8>>>, Error> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {endpoint}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;
    let (status, body) = read_http_response(&mut stream).await?;
    if status != 200 || body.is_empty() {
        return Ok(None);
    }
    let Some(data) = extract_sse_data(&body) else {
        return Ok(None);
    };
    Ok(Some(decode_envelope(&data)?))
}

fn extract_sse_data(buf: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(buf).ok()?;
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            return Some(data.as_bytes().to_vec());
        }
    }
    None
}

async fn write_sse_response(stream: &mut TcpStream, body: &[u8]) -> Result<(), Error> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_http_request(
    stream: &mut TcpStream,
) -> Result<(String, String, Vec<(String, String)>, Vec<u8>), Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http request closed early",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "http request missing header terminator",
            )
        })?
        + 4;
    let header_text = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let request_line = lines
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let raw_path = parts.next().unwrap_or("/");
    let path = raw_path
        .split('?')
        .next()
        .map(normalize_path)
        .unwrap_or_else(|| "/".to_string());
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http body closed early",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Ok((method, path, headers, body))
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
) -> Result<(), Error> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "OK",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_http_response(stream: &mut TcpStream) -> Result<(u16, Vec<u8>), Error> {
    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http response closed early",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "http response missing header terminator",
            )
        })?
        + 4;
    let header_text = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
    let status_line = lines
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing status line"))?;
    let mut parts = status_line.split_whitespace();
    let _http = parts.next();
    let status = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "invalid http status code"))?;
    let headers = lines
        .filter_map(|line| {
            line.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect::<Vec<_>>();
    let content_length = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http response body closed early",
            ));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Ok((status, body))
}

fn normalize_path(path: &str) -> String {
    let clean = path.split('?').next().unwrap_or("/");
    if clean.is_empty() {
        "/".to_string()
    } else {
        clean.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::{Message, TaskResultMessage},
        },
    };

    use super::{accept_peer_on, bind, connect_peer};

    #[tokio::test]
    async fn streamhttp_task_roundtrip() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("streamhttp-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("streamhttp-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let peer = accept_peer_on(server_identity, listener, "/task")
                .await
                .unwrap();
            let frame = peer.read_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                peer.send_frame(&Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "streamhttp-ok".into(),
                        data_hex: None,
                    }),
                ))
                .await
                .unwrap();
            }
        });

        let client = timeout(
            Duration::from_secs(5),
            connect_peer(client_identity, &format!("streamhttp://{addr}/task")),
        )
        .await
        .unwrap()
        .unwrap();

        client
            .send_frame(&Frame::new(
                MessageType::TaskRequest,
                Some(client.session.local.agent_id.clone()),
                Some(client.session.remote.agent_id.clone()),
                Message::TaskRequest(crate::protocol::message::TaskRequestMessage {
                    task_id: "task-1".into(),
                    action: crate::protocol::message::TaskAction::Shell,
                    args: vec!["echo".into()],
                    data_hex: None,
                }),
            ))
            .await
            .unwrap();
        let response = client.read_frame().await.unwrap();
        match response.message {
            Message::TaskResult(result) => assert_eq!(result.output, "streamhttp-ok"),
            other => panic!("unexpected message: {other:?}"),
        }
        server.await.unwrap();
    }
}
