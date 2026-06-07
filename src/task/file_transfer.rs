use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    sync::OnceLock,
};

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Mutex},
};

use crate::{
    app::runtime_bridge::{build_stream_close_frame, build_stream_data_frame},
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, StreamOpenMessage},
    },
    task::interactive_shell::ShellPeer,
    tunnel::{dns_tunnel, h2_tunnel, http_poll},
};

pub const FILE_DOWNLOAD_SERVICE: &str = "file://download";
pub const FILE_UPLOAD_SERVICE: &str = "file://upload";
const CHUNK_SIZE: usize = 64 * 1024;

enum PendingFileAction {
    Download { remote_path: String },
    Upload { remote_path: String },
}

struct PendingFileTransfer {
    action: PendingFileAction,
}

static PENDING_TRANSFERS: OnceLock<Mutex<HashMap<String, PendingFileTransfer>>> = OnceLock::new();

fn pending_transfers() -> &'static Mutex<HashMap<String, PendingFileTransfer>> {
    PENDING_TRANSFERS.get_or_init(|| Mutex::new(HashMap::new()))
}

static CLIENT_STREAM_COUNTER: OnceLock<Mutex<u32>> = OnceLock::new();

async fn next_client_stream_id() -> u32 {
    let counter = CLIENT_STREAM_COUNTER.get_or_init(|| Mutex::new(1));
    let mut guard = counter.lock().await;
    let id = *guard;
    *guard = guard.saturating_add(1);
    id
}

pub async fn arm_download(task_id: &str, remote_path: String) {
    pending_transfers().lock().await.insert(
        task_id.to_string(),
        PendingFileTransfer {
            action: PendingFileAction::Download { remote_path },
        },
    );
}

pub async fn arm_upload(task_id: &str, remote_path: String) {
    pending_transfers().lock().await.insert(
        task_id.to_string(),
        PendingFileTransfer {
            action: PendingFileAction::Upload { remote_path },
        },
    );
}

pub async fn disarm_transfer(task_id: &str) {
    pending_transfers().lock().await.remove(task_id);
}

pub fn mux_stream_transport_supported(scheme: &str) -> bool {
    scheme == "tcp"
        || matches!(scheme, "ws" | "wss")
        || h2_tunnel::is_h2_tunnel_scheme(scheme)
        || dns_tunnel::is_dns_tunnel_scheme(scheme)
        || http_poll::is_http_poll_scheme(scheme)
        || scheme == "simplex+oss"
}

pub async fn try_accept_stream_open(peer: ShellPeer, open_frame: Frame) -> Result<bool, Error> {
    let stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on file transfer StreamOpen",
        )
    })?;
    let Message::StreamOpen(open) = open_frame.message else {
        return Ok(false);
    };
    let service = open.service.as_str();
    if service != FILE_DOWNLOAD_SERVICE && service != FILE_UPLOAD_SERVICE {
        return Ok(false);
    }
    let task_id = open.target_host.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "file transfer StreamOpen missing task_id in target_host",
        )
    })?;
    let pending = pending_transfers().lock().await.remove(&task_id);
    let Some(pending) = pending else {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("no pending file transfer for task_id={task_id}"),
        ));
    };

    match pending.action {
        PendingFileAction::Download { remote_path } => {
            spawn_download_session(peer, stream_id, remote_path).await?;
        }
        PendingFileAction::Upload { remote_path } => {
            spawn_upload_session(peer, stream_id, remote_path).await?;
        }
    }
    Ok(true)
}

async fn spawn_download_session(
    peer: ShellPeer,
    stream_id: u32,
    remote_path: String,
) -> Result<(), Error> {
    let stream_rx = peer.open_stream_receiver(stream_id).await;
    tokio::spawn(async move {
        if let Err(err) = run_download_session(peer, stream_id, stream_rx, &remote_path).await {
            log::error!("file download session ended: {err}");
        }
    });
    Ok(())
}

async fn spawn_upload_session(
    peer: ShellPeer,
    stream_id: u32,
    remote_path: String,
) -> Result<(), Error> {
    let stream_rx = peer.open_stream_receiver(stream_id).await;
    tokio::spawn(async move {
        if let Err(err) = run_upload_session(peer, stream_id, stream_rx, &remote_path).await {
            log::error!("file upload session ended: {err}");
        }
    });
    Ok(())
}

async fn run_download_session(
    peer: ShellPeer,
    stream_id: u32,
    mut stream_rx: mpsc::Receiver<Frame>,
    remote_path: &str,
) -> Result<(), Error> {
    let mut file = File::open(remote_path).await.map_err(|err| {
        Error::new(
            ErrorKind::NotFound,
            format!("failed to open `{remote_path}`: {err}"),
        )
    })?;
    let local_id = peer.local_agent_id();
    let remote_id = peer.remote_agent_id();
    let mut buf = vec![0_u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        let frame = build_stream_data_frame(&local_id, &remote_id, stream_id, &buf[..n]);
        peer.send_frame(&frame).await?;
    }
    let close = build_stream_close_frame(&local_id, &remote_id, stream_id);
    peer.send_frame(&close).await?;
    while let Some(frame) = stream_rx.recv().await {
        if frame.header.stream_id == Some(stream_id)
            && matches!(frame.message, Message::StreamClose(_))
        {
            break;
        }
    }
    Ok(())
}

async fn run_upload_session(
    peer: ShellPeer,
    stream_id: u32,
    mut stream_rx: mpsc::Receiver<Frame>,
    remote_path: &str,
) -> Result<(), Error> {
    if let Some(parent) = Path::new(remote_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(remote_path)
        .await?;
    let local_id = peer.local_agent_id();
    let remote_id = peer.remote_agent_id();
    let mut closed = false;
    while let Some(frame) = stream_rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            continue;
        }
        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                file.write_all(&bytes).await?;
            }
            Message::StreamClose(_) => {
                closed = true;
                break;
            }
            _ => {}
        }
    }
    file.flush().await?;
    if !closed {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "upload stream closed before StreamClose",
        ));
    }
    let close = build_stream_close_frame(&local_id, &remote_id, stream_id);
    peer.send_frame(&close).await?;
    Ok(())
}

pub async fn begin_client_download(
    peer: ShellPeer,
    task_id: &str,
    destination_agent_id: &str,
    save_path: PathBuf,
) -> Result<u64, Error> {
    if let Some(parent) = save_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).await?;
        }
    }
    let stream_id = next_client_stream_id().await;
    let _ = peer.open_stream_receiver(stream_id).await;
    let open = Frame::new(
        MessageType::StreamOpen,
        Some(peer.local_agent_id()),
        Some(destination_agent_id.to_string()),
        Message::StreamOpen(StreamOpenMessage {
            service: FILE_DOWNLOAD_SERVICE.into(),
            target_host: Some(task_id.to_string()),
            target_port: None,
        }),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&open).await?;

    let mut file = File::create(&save_path).await?;
    let mut stream_rx = peer.open_stream_receiver(stream_id).await;
    let mut total = 0_u64;
    let mut closed = false;
    while let Some(frame) = stream_rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            continue;
        }
        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                total += bytes.len() as u64;
                file.write_all(&bytes).await?;
            }
            Message::StreamClose(_) => {
                closed = true;
                break;
            }
            _ => {}
        }
    }
    file.flush().await?;
    if !closed {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "download stream ended before StreamClose",
        ));
    }
    let close = build_stream_close_frame(
        &peer.local_agent_id(),
        &peer.remote_agent_id(),
        stream_id,
    );
    let _ = peer.send_frame(&close).await;
    Ok(total)
}

pub async fn begin_client_upload(
    peer: ShellPeer,
    task_id: &str,
    destination_agent_id: &str,
    local_path: PathBuf,
) -> Result<u64, Error> {
    let mut local = File::open(&local_path).await.map_err(|err| {
        Error::new(
            ErrorKind::NotFound,
            format!("failed to open `{}`: {err}", local_path.display()),
        )
    })?;
    let stream_id = next_client_stream_id().await;
    let _ = peer.open_stream_receiver(stream_id).await;
    let open = Frame::new(
        MessageType::StreamOpen,
        Some(peer.local_agent_id()),
        Some(destination_agent_id.to_string()),
        Message::StreamOpen(StreamOpenMessage {
            service: FILE_UPLOAD_SERVICE.into(),
            target_host: Some(task_id.to_string()),
            target_port: None,
        }),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&open).await?;

    let local_id = peer.local_agent_id();
    let remote_id = peer.remote_agent_id();
    let mut buf = vec![0_u8; CHUNK_SIZE];
    let mut total = 0_u64;
    loop {
        let n = local.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total += n as u64;
        let frame = build_stream_data_frame(&local_id, &remote_id, stream_id, &buf[..n]);
        peer.send_frame(&frame).await?;
    }
    let close = build_stream_close_frame(&local_id, &remote_id, stream_id);
    peer.send_frame(&close).await?;

    let mut stream_rx = peer.open_stream_receiver(stream_id).await;
    let mut acked = false;
    while let Some(frame) = stream_rx.recv().await {
        if frame.header.stream_id == Some(stream_id)
            && matches!(frame.message, Message::StreamClose(_))
        {
            acked = true;
            break;
        }
    }
    if !acked {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "upload did not receive server StreamClose ack",
        ));
    }
    Ok(total)
}

pub fn default_download_path(data_dir: &Path, task_id: &str, remote_path: &str) -> PathBuf {
    let name = Path::new(remote_path)
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| format!("{task_id}.bin"));
    data_dir.join("tasks").join(name)
}

#[cfg(test)]
mod tests {
    use super::{
        arm_download, arm_upload, begin_client_download, begin_client_upload,
        mux_stream_transport_supported, try_accept_stream_open, FILE_DOWNLOAD_SERVICE,
        FILE_UPLOAD_SERVICE,
    };
    use crate::{
        agent::identity::AgentIdentity,
        app::config::AgentIdentityConfig,
        protocol::{
            frame::{Frame, MessageType},
            message::Message,
        },
        task::interactive_shell::ShellPeer,
        tunnel::tcp_mux::{accept_mux_peer_on, connect_mux_peer},
        tunnel::tcp::bind,
    };

    #[test]
    fn mux_stream_transport_supported_for_file_transfer() {
        assert!(mux_stream_transport_supported("tcp"));
        assert!(mux_stream_transport_supported("h2"));
        assert!(mux_stream_transport_supported("simplex+http"));
        assert!(!mux_stream_transport_supported("streamhttp"));
    }

    #[tokio::test]
    async fn file_download_roundtrip_over_mux_stream() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("file-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("file-client".into()),
            key: None,
        });

        let payload = vec![7_u8; 150_000];
        let remote_path = std::env::temp_dir().join(format!(
            "fusion-file-remote-{}",
            std::process::id()
        ));
        let download_path = std::env::temp_dir().join(format!(
            "fusion-file-downloaded-{}",
            std::process::id()
        ));
        tokio::fs::write(&remote_path, &payload).await.unwrap();

        let task_id = "task-file-1".to_string();
        arm_download(&task_id, remote_path.to_string_lossy().to_string()).await;

        let server = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener).await.unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            assert_eq!(open.service, FILE_DOWNLOAD_SERVICE);
            let open_frame = Frame::new(
                MessageType::StreamOpen,
                Some("client".into()),
                Some("server".into()),
                Message::StreamOpen(open),
            )
            .with_stream_id(stream_id);
            try_accept_stream_open(ShellPeer::Tcp(peer.clone()), open_frame)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let client_peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let bytes = begin_client_download(
            ShellPeer::Tcp(client_peer),
            &task_id,
            "remote-agent",
            download_path.clone(),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(bytes, payload.len() as u64);
        assert_eq!(tokio::fs::read(&download_path).await.unwrap(), payload);

        let _ = tokio::fs::remove_file(download_path).await;
        let _ = tokio::fs::remove_file(remote_path).await;
    }

    #[tokio::test]
    async fn file_upload_roundtrip_over_mux_stream() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("file-server".into()),
            key: None,
        });
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("file-client".into()),
            key: None,
        });

        let payload = vec![9_u8; 150_000];
        let remote_path = std::env::temp_dir().join(format!(
            "fusion-file-remote-upload-{}",
            std::process::id()
        ));
        let local_upload = std::env::temp_dir().join(format!(
            "fusion-file-local-upload-{}",
            std::process::id()
        ));
        tokio::fs::write(&local_upload, &payload).await.unwrap();

        let task_id = "task-file-upload".to_string();
        arm_upload(&task_id, remote_path.to_string_lossy().to_string()).await;

        let server = tokio::spawn(async move {
            let peer = accept_mux_peer_on(server_identity, &listener).await.unwrap();
            let (stream_id, open) = peer.read_stream_open().await.unwrap();
            assert_eq!(open.service, FILE_UPLOAD_SERVICE);
            let open_frame = Frame::new(
                MessageType::StreamOpen,
                Some("client".into()),
                Some("server".into()),
                Message::StreamOpen(open),
            )
            .with_stream_id(stream_id);
            try_accept_stream_open(ShellPeer::Tcp(peer.clone()), open_frame)
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        let client_peer = connect_mux_peer(client_identity, &addr.to_string())
            .await
            .unwrap();
        let uploaded = begin_client_upload(
            ShellPeer::Tcp(client_peer),
            &task_id,
            "remote-agent",
            local_upload.clone(),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert_eq!(uploaded, payload.len() as u64);
        assert_eq!(tokio::fs::read(&remote_path).await.unwrap(), payload);

        let _ = tokio::fs::remove_file(local_upload).await;
        let _ = tokio::fs::remove_file(remote_path).await;
    }
}
