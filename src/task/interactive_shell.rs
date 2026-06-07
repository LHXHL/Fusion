use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Read, Write},
    sync::OnceLock,
};

use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use tokio::{
    io::AsyncReadExt,
    sync::{mpsc, Mutex},
};

use crate::{
    app::runtime_bridge::{build_stream_close_frame, build_stream_data_frame},
    protocol::{
        frame::{Frame, MessageType},
        message::{Message, StreamOpenMessage},
    },
    tunnel::{h2_mux, simplex_dns_mux, simplex_http_mux, simplex_oss_mux, tcp_mux, ws_mux},
};

pub const INTERACTIVE_SHELL_SERVICE: &str = "shell://interactive";

struct PendingShell {
    shell: Option<String>,
}

static PENDING_SHELLS: OnceLock<Mutex<HashMap<String, PendingShell>>> = OnceLock::new();

fn pending_shells() -> &'static Mutex<HashMap<String, PendingShell>> {
    PENDING_SHELLS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub async fn arm_interactive_shell(task_id: &str, shell: Option<String>) {
    pending_shells()
        .lock()
        .await
        .insert(task_id.to_string(), PendingShell { shell });
}

pub async fn disarm_interactive_shell(task_id: &str) {
    pending_shells().lock().await.remove(task_id);
}

#[derive(Clone)]
pub enum ShellPeer {
    Tcp(tcp_mux::MuxTcpPeer),
    Ws(ws_mux::MuxWsPeer),
    H2(h2_mux::MuxH2Peer),
    SimplexDns(simplex_dns_mux::MuxSimplexDnsPeer),
    SimplexHttp(simplex_http_mux::MuxSimplexHttpPeer),
    SimplexOss(simplex_oss_mux::MuxSimplexOssPeer),
}

impl ShellPeer {
    pub fn local_agent_id(&self) -> String {
        match self {
            Self::Tcp(peer) => peer.session.local.agent_id.clone(),
            Self::Ws(peer) => peer.session.local.agent_id.clone(),
            Self::H2(peer) => peer.session.local.agent_id.clone(),
            Self::SimplexDns(peer) => peer.session.local.agent_id.clone(),
            Self::SimplexHttp(peer) => peer.session.local.agent_id.clone(),
            Self::SimplexOss(peer) => peer.session.local.agent_id.clone(),
        }
    }

    pub fn remote_agent_id(&self) -> String {
        match self {
            Self::Tcp(peer) => peer.session.remote.agent_id.clone(),
            Self::Ws(peer) => peer.session.remote.agent_id.clone(),
            Self::H2(peer) => peer.session.remote.agent_id.clone(),
            Self::SimplexDns(peer) => peer.session.remote.agent_id.clone(),
            Self::SimplexHttp(peer) => peer.session.remote.agent_id.clone(),
            Self::SimplexOss(peer) => peer.session.remote.agent_id.clone(),
        }
    }

    pub async fn send_frame(&self, frame: &Frame) -> Result<(), Error> {
        match self {
            Self::Tcp(peer) => peer.send_frame(frame).await,
            Self::Ws(peer) => peer.send_frame(frame).await,
            Self::H2(peer) => peer.send_frame(frame).await,
            Self::SimplexDns(peer) => peer.send_frame(frame).await,
            Self::SimplexHttp(peer) => peer.send_frame(frame).await,
            Self::SimplexOss(peer) => peer.send_frame(frame).await,
        }
    }

    pub async fn open_stream_receiver(&self, stream_id: u32) -> mpsc::Receiver<Frame> {
        match self {
            Self::Tcp(peer) => peer.open_stream_receiver(stream_id).await,
            Self::Ws(peer) => peer.open_stream_receiver(stream_id).await,
            Self::H2(peer) => peer.open_stream_receiver(stream_id).await,
            Self::SimplexDns(peer) => peer.open_stream_receiver(stream_id).await,
            Self::SimplexHttp(peer) => peer.open_stream_receiver(stream_id).await,
            Self::SimplexOss(peer) => peer.open_stream_receiver(stream_id).await,
        }
    }
}

pub async fn spawn_shell_session(
    peer: ShellPeer,
    stream_id: u32,
    shell: Option<String>,
) -> Result<(), Error> {
    let stream_rx = peer.open_stream_receiver(stream_id).await;
    tokio::spawn(async move {
        if let Err(err) = run_shell_session(peer, stream_id, stream_rx, shell).await {
            log::error!("interactive shell session ended: {err}");
        }
    });
    Ok(())
}

pub async fn try_accept_stream_open(
    peer: ShellPeer,
    open_frame: Frame,
) -> Result<bool, Error> {
    let stream_id = open_frame.header.stream_id.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "missing stream_id on interactive shell StreamOpen",
        )
    })?;
    let Message::StreamOpen(open) = open_frame.message else {
        return Ok(false);
    };
    if open.service != INTERACTIVE_SHELL_SERVICE {
        return Ok(false);
    }
    let task_id = open.target_host.ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidData,
            "interactive shell StreamOpen missing task_id in target_host",
        )
    })?;
    let pending = pending_shells().lock().await.remove(&task_id);
    let Some(pending) = pending else {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("no pending interactive shell for task_id={task_id}"),
        ));
    };
    spawn_shell_session(peer, stream_id, pending.shell).await?;
    Ok(true)
}

static CLIENT_STREAM_COUNTER: OnceLock<Mutex<u32>> = OnceLock::new();

async fn next_client_stream_id() -> u32 {
    let counter = CLIENT_STREAM_COUNTER.get_or_init(|| Mutex::new(1));
    let mut guard = counter.lock().await;
    let id = *guard;
    *guard = guard.saturating_add(1);
    id
}

pub async fn begin_client_session(
    peer: ShellPeer,
    task_id: &str,
    destination_agent_id: &str,
) -> Result<(), Error> {
    let stream_id = next_client_stream_id().await;
    let _ = peer.open_stream_receiver(stream_id).await;
    let open = Frame::new(
        MessageType::StreamOpen,
        Some(peer.local_agent_id()),
        Some(destination_agent_id.to_string()),
        Message::StreamOpen(StreamOpenMessage {
            service: INTERACTIVE_SHELL_SERVICE.into(),
            target_host: Some(task_id.to_string()),
            target_port: None,
        }),
    )
    .with_stream_id(stream_id);
    peer.send_frame(&open).await?;
    run_client_session(peer, stream_id).await
}

fn default_shell() -> String {
    #[cfg(windows)]
    {
        std::env::var("COMSPEC").unwrap_or_else(|_| "cmd.exe".into())
    }
    #[cfg(not(windows))]
    {
        std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into())
    }
}

fn default_shell_args(shell: &str) -> Vec<String> {
    #[cfg(windows)]
    {
        let _ = shell;
        vec!["/K".into()]
    }
    #[cfg(not(windows))]
    {
        let _ = shell;
        vec![]
    }
}

async fn run_shell_session(
    peer: ShellPeer,
    stream_id: u32,
    mut stream_rx: mpsc::Receiver<Frame>,
    shell: Option<String>,
) -> Result<(), Error> {
    let shell_cmd = shell.unwrap_or_else(default_shell);
    let pty_system = native_pty_system();
    let pair = pty_system
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(|err| Error::other(err.to_string()))?;

    let mut cmd = CommandBuilder::new(&shell_cmd);
    for arg in default_shell_args(&shell_cmd) {
        cmd.arg(arg);
    }
    let mut child = pair
        .slave
        .spawn_command(cmd)
        .map_err(|err| Error::other(err.to_string()))?;
    drop(pair.slave);

    let mut reader = pair
        .master
        .try_clone_reader()
        .map_err(|err| Error::other(err.to_string()))?;
    let writer = pair
        .master
        .take_writer()
        .map_err(|err| Error::other(err.to_string()))?;

    let (pty_out_tx, mut pty_out_rx) = mpsc::channel::<Vec<u8>>(64);
    tokio::task::spawn_blocking(move || {
        let mut buf = [0_u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if pty_out_tx.blocking_send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let (pty_in_tx, pty_in_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    std::thread::spawn(move || {
        let mut writer = writer;
        for bytes in pty_in_rx {
            if writer.write_all(&bytes).is_err() {
                break;
            }
        }
    });

    let local_id = peer.local_agent_id();
    let remote_id = peer.remote_agent_id();
    let peer_for_out = peer.clone();
    let out_task = tokio::spawn(async move {
        while let Some(bytes) = pty_out_rx.recv().await {
            let frame = build_stream_data_frame(&local_id, &remote_id, stream_id, &bytes);
            if peer_for_out.send_frame(&frame).await.is_err() {
                break;
            }
        }
    });

    let mut closed = false;
    while let Some(frame) = stream_rx.recv().await {
        if frame.header.stream_id != Some(stream_id) {
            continue;
        }
        match frame.message {
            Message::StreamData(data) => {
                let bytes = data.to_bytes()?;
                if pty_in_tx.send(bytes).is_err() {
                    break;
                }
            }
            Message::StreamClose(_) => {
                closed = true;
                break;
            }
            _ => {}
        }
    }

    let _ = child.kill();
    let _ = child.wait();
    out_task.abort();

    if !closed {
        let close = build_stream_close_frame(&peer.local_agent_id(), &peer.remote_agent_id(), stream_id);
        let _ = peer.send_frame(&close).await;
    }

    Ok(())
}

struct RawModeGuard;

impl RawModeGuard {
    fn enter() -> Result<Self, Error> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|err| Error::other(format!("failed to enter raw terminal mode: {err}")))?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

pub async fn run_client_session(peer: ShellPeer, stream_id: u32) -> Result<(), Error> {
    let mut stream_rx = peer.open_stream_receiver(stream_id).await;
    let _raw = RawModeGuard::enter()?;

    let local_id = peer.local_agent_id();
    let remote_id = peer.remote_agent_id();
    let mut stdout = tokio::io::stdout();
    let mut stdin = tokio::io::stdin();
    let mut stdin_buf = [0_u8; 4096];
    let mut closed = false;

    loop {
        tokio::select! {
            read = stdin.read(&mut stdin_buf) => {
                let n = read?;
                if n == 0 {
                    closed = true;
                    break;
                }
                let frame = build_stream_data_frame(&local_id, &remote_id, stream_id, &stdin_buf[..n]);
                peer.send_frame(&frame).await?;
            }
            frame = stream_rx.recv() => {
                match frame {
                    Some(frame) if frame.header.stream_id == Some(stream_id) => match frame.message {
                        Message::StreamData(data) => {
                            let bytes = data.to_bytes()?;
                            tokio::io::AsyncWriteExt::write_all(&mut stdout, &bytes).await?;
                            tokio::io::AsyncWriteExt::flush(&mut stdout).await?;
                        }
                        Message::StreamClose(_) => {
                            closed = true;
                            break;
                        }
                        _ => {}
                    },
                    Some(_) => {}
                    None => break,
                }
            }
        }
    }

    if !closed {
        let close = build_stream_close_frame(&local_id, &remote_id, stream_id);
        let _ = peer.send_frame(&close).await;
    }

    Ok(())
}

pub fn mux_transport_supported(scheme: &str) -> bool {
    scheme == "tcp"
        || matches!(scheme, "ws" | "wss")
        || crate::tunnel::h2_tunnel::is_h2_tunnel_scheme(scheme)
}

#[cfg(test)]
mod tests {
    use super::mux_transport_supported;

    #[test]
    fn mux_transport_supported_for_common_schemes() {
        assert!(mux_transport_supported("tcp"));
        assert!(mux_transport_supported("ws"));
        assert!(mux_transport_supported("h2"));
        assert!(!mux_transport_supported("simplex+http"));
    }
}
