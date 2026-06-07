use std::{
    collections::HashMap,
    io::Error,
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{
    app::runtime_relay::{
        handle_h2_relay_stream_open, handle_simplex_dns_relay_stream_open,
        handle_simplex_relay_stream_open, handle_simplex_oss_relay_stream_open,
        handle_tcp_relay_stream_open, handle_ws_relay_stream_open,
    },
    app::runtime_status::RelayLinkMap,
    protocol::frame::Frame,
    task::interactive_shell::{self, ShellPeer},
    tunnel::{h2_mux, simplex_dns_mux, simplex_http_mux, simplex_oss_mux, tcp_mux, ws_mux},
};

pub type TcpTaskPeerMap = Arc<Mutex<HashMap<String, tcp_mux::MuxTcpPeer>>>;
pub type WsTaskPeerMap = Arc<Mutex<HashMap<String, ws_mux::MuxWsPeer>>>;
pub type H2TaskPeerMap = Arc<Mutex<HashMap<String, h2_mux::MuxH2Peer>>>;
pub type SimplexDnsTaskPeerMap = Arc<Mutex<HashMap<String, simplex_dns_mux::MuxSimplexDnsPeer>>>;
pub type SimplexHttpTaskPeerMap = Arc<Mutex<HashMap<String, simplex_http_mux::MuxSimplexHttpPeer>>>;
pub type SimplexOssTaskPeerMap = Arc<Mutex<HashMap<String, simplex_oss_mux::MuxSimplexOssPeer>>>;
pub type TcpRelayStreamAllocator = Arc<Mutex<u32>>;
pub type WsRelayStreamAllocator = Arc<Mutex<u32>>;
pub type H2RelayStreamAllocator = Arc<Mutex<u32>>;
pub type SimplexDnsRelayStreamAllocator = Arc<Mutex<u32>>;
pub type SimplexHttpRelayStreamAllocator = Arc<Mutex<u32>>;
pub type SimplexOssRelayStreamAllocator = Arc<Mutex<u32>>;

fn is_local_stream_destination(local_agent_id: &str, open_frame: &Frame) -> bool {
    open_frame
        .header
        .dst_agent
        .as_deref()
        .is_some_and(|dst| dst == local_agent_id)
}

pub async fn handle_mux_stream_open_tcp(
    local_agent_id: &str,
    peer: tcp_mux::MuxTcpPeer,
    open_frame: Frame,
    peer_map: TcpTaskPeerMap,
    allocator: TcpRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    if is_local_stream_destination(local_agent_id, &open_frame)
        && interactive_shell::try_accept_stream_open(ShellPeer::Tcp(peer.clone()), open_frame.clone())
            .await?
    {
        return Ok(());
    }
    if is_local_stream_destination(local_agent_id, &open_frame)
        && crate::task::file_transfer::try_accept_stream_open(
            ShellPeer::Tcp(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    handle_tcp_relay_stream_open(peer_map, allocator, relay_links, peer, open_frame).await
}

pub async fn handle_mux_stream_open_ws(
    local_agent_id: &str,
    peer: ws_mux::MuxWsPeer,
    open_frame: Frame,
    peer_map: WsTaskPeerMap,
    allocator: WsRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    if is_local_stream_destination(local_agent_id, &open_frame)
        && interactive_shell::try_accept_stream_open(ShellPeer::Ws(peer.clone()), open_frame.clone())
            .await?
    {
        return Ok(());
    }
    if is_local_stream_destination(local_agent_id, &open_frame)
        && crate::task::file_transfer::try_accept_stream_open(
            ShellPeer::Ws(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    handle_ws_relay_stream_open(peer_map, allocator, relay_links, peer, open_frame).await
}

pub async fn handle_mux_stream_open_h2(
    local_agent_id: &str,
    peer: h2_mux::MuxH2Peer,
    open_frame: Frame,
    peer_map: H2TaskPeerMap,
    allocator: H2RelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    if is_local_stream_destination(local_agent_id, &open_frame)
        && interactive_shell::try_accept_stream_open(ShellPeer::H2(peer.clone()), open_frame.clone())
            .await?
    {
        return Ok(());
    }
    if is_local_stream_destination(local_agent_id, &open_frame)
        && crate::task::file_transfer::try_accept_stream_open(
            ShellPeer::H2(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    handle_h2_relay_stream_open(peer_map, allocator, relay_links, peer, open_frame).await
}

pub async fn handle_mux_stream_open_simplex_dns(
    local_agent_id: &str,
    peer: simplex_dns_mux::MuxSimplexDnsPeer,
    open_frame: Frame,
    peer_map: SimplexDnsTaskPeerMap,
    allocator: SimplexDnsRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    if is_local_stream_destination(local_agent_id, &open_frame)
        && interactive_shell::try_accept_stream_open(
            ShellPeer::SimplexDns(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    if is_local_stream_destination(local_agent_id, &open_frame)
        && crate::task::file_transfer::try_accept_stream_open(
            ShellPeer::SimplexDns(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    handle_simplex_dns_relay_stream_open(peer_map, allocator, relay_links, peer, open_frame).await
}

pub async fn handle_mux_stream_open_simplex_http(
    local_agent_id: &str,
    peer: simplex_http_mux::MuxSimplexHttpPeer,
    open_frame: Frame,
    peer_map: SimplexHttpTaskPeerMap,
    allocator: SimplexHttpRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    if is_local_stream_destination(local_agent_id, &open_frame)
        && interactive_shell::try_accept_stream_open(
            ShellPeer::SimplexHttp(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    if is_local_stream_destination(local_agent_id, &open_frame)
        && crate::task::file_transfer::try_accept_stream_open(
            ShellPeer::SimplexHttp(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    handle_simplex_relay_stream_open(peer_map, allocator, relay_links, peer, open_frame).await
}

pub async fn handle_mux_stream_open_simplex_oss(
    local_agent_id: &str,
    peer: simplex_oss_mux::MuxSimplexOssPeer,
    open_frame: Frame,
    peer_map: SimplexOssTaskPeerMap,
    allocator: SimplexOssRelayStreamAllocator,
    relay_links: RelayLinkMap,
) -> Result<(), Error> {
    if is_local_stream_destination(local_agent_id, &open_frame)
        && interactive_shell::try_accept_stream_open(
            ShellPeer::SimplexOss(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    if is_local_stream_destination(local_agent_id, &open_frame)
        && crate::task::file_transfer::try_accept_stream_open(
            ShellPeer::SimplexOss(peer.clone()),
            open_frame.clone(),
        )
        .await?
    {
        return Ok(());
    }
    handle_simplex_oss_relay_stream_open(peer_map, allocator, relay_links, peer, open_frame).await
}
