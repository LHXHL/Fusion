use crate::{
    agent::identity::AgentIdentity,
    app::{
        config::{
            AgentIdentityConfig, AppConfig, RetryPolicy, ServeEndpoint, StatusScope,
            TaskRequestConfig, TunnelEndpoint,
        },
        runtime_http::{handle_outbound_http_client, handle_outbound_http_ws_client},
        runtime_orchestrator::{spawn_inbound_tasks, spawn_outbound_tasks, RuntimeShared},
        runtime_relay::{
            handle_h2_relay_stream_open, handle_simplex_dns_relay_stream_open,
            handle_simplex_oss_relay_stream_open, handle_simplex_relay_stream_open,
            handle_tcp_relay_stream_open, handle_ws_relay_stream_open,
        },
        runtime_portfwd::handle_port_forward_simplex_http_client,
        runtime_trojan::{handle_outbound_trojan_client, trojan_plain_client},
        runtime_socks5::{
            handle_outbound_socks5_client, handle_outbound_socks5_simplex_http_client,
            handle_outbound_socks5_ws_client,
        },
        runtime_status::{
            print_status_snapshot, render_status_lines, write_status_snapshot, RelayStreamLink,
            RuntimeConfigSummary, RuntimeStatusSnapshot,
        },
        runtime_task::{maybe_store_task_artifact, run_outbound_task_once},
    },
    protocol::{
        frame::{Frame, MessageType},
        message::{
            Message, StreamCloseMessage, StreamDataMessage, StreamOpenMessage, TaskAction,
            TaskResultMessage,
        },
    },
    serve::{
        http::HttpProxyService,
        portfwd::PortForwardService,
        trojan::{encode_trojan_connect_request, TrojanService},
        raw::{
            proxy_h2_mux_stream_loop, proxy_mux_stream_loop, proxy_simplex_dns_mux_stream_loop,
            proxy_simplex_mux_stream_loop, proxy_simplex_oss_mux_stream_loop,
            proxy_ws_mux_stream_loop, RawService,
        },
        service::{build_remote_services, ServiceDefinition},
        socks5::Socks5Service,
    },
    tunnel::{
        simplex_dns, simplex_dns_mux, simplex_http, simplex_http_mux, simplex_oss, simplex_oss_mux,
        streamhttp,
        h2_mux,
        tcp::bind,
        tcp_mux::{accept_mux_peer, accept_mux_peer_on, connect_mux_peer},
        ws_mux::{
            accept_mux_peer as accept_ws_mux_peer, bind as bind_ws,
            connect_mux_peer as connect_ws_mux_peer,
        },
    },
    utils::url::ParsedUrl,
};
use data_encoding::HEXLOWER;
use std::sync::Arc;
use tokio::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

fn dynamic_raw_service_definition() -> ServiceDefinition {
    build_remote_services(&[ServeEndpoint {
        url: ParsedUrl::parse("raw://").unwrap(),
    }])
    .unwrap()
    .into_iter()
    .next()
    .unwrap()
}

async fn tcp_socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server, _) = listener.accept().await.unwrap();
    (client.await.unwrap(), server)
}

async fn tcp_multi_hop_relay_roundtrip(relay_count: usize, stream_id: u32, payload: &[u8]) {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("multi-hop-target-{relay_count}")),
        key: None,
    });
    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer = accept_mux_peer(target_identity, target_listener)
            .await
            .unwrap();
        let (accepted_stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(accepted_stream_id).await;
        proxy_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            accepted_stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let mut relay_listeners = Vec::with_capacity(relay_count);
    let mut relay_addrs = Vec::with_capacity(relay_count);
    for idx in 0..relay_count {
        let listener = bind("127.0.0.1:0").await.unwrap();
        relay_addrs.push(listener.local_addr().unwrap());
        relay_listeners.push((idx, listener));
    }

    let mut relay_tasks = Vec::with_capacity(relay_count);
    for (idx, listener) in relay_listeners.into_iter().rev() {
        let downstream_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some(format!("multi-hop-relay-{relay_count}-{idx}")),
            key: None,
        });
        let upstream_addr = if idx + 1 == relay_count {
            target_addr.to_string()
        } else {
            relay_addrs[idx + 1].to_string()
        };
        let target_id = target_id.clone();
        relay_tasks.push(tokio::spawn(async move {
            let downstream_peer = accept_mux_peer(downstream_identity, listener)
                .await
                .unwrap();
            let upstream_peer = connect_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some(format!("multi-hop-upstream-{relay_count}-{idx}")),
                    key: None,
                }),
                &upstream_addr,
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(target_id, upstream_peer);
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            handle_tcp_relay_stream_open(
                Arc::new(Mutex::new(peer_map)),
                Arc::new(Mutex::new(10_000 + (idx as u32 * 1000))),
                Arc::new(Mutex::new(std::collections::HashMap::new())),
                downstream_peer,
                open_frame,
            )
            .await
            .unwrap();
        }));
    }

    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("multi-hop-leaf-{relay_count}")),
        key: None,
    });
    let leaf_peer = connect_mux_peer(leaf_identity, &relay_addrs[0].to_string())
        .await
        .unwrap();
    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&open_frame).await.unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(stream_id).await;
    let payload_frame = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(payload)),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&payload_frame).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), payload),
        other => panic!("unexpected multi-hop relayed stream response: {:?}", other),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    for relay_task in relay_tasks {
        relay_task.await.unwrap();
    }
    target_task.await.unwrap();
}

async fn simplex_oss_multi_hop_relay_roundtrip(relay_count: usize, stream_id: u32, payload: &[u8]) {
    let root = std::env::temp_dir().join(format!(
        "fusion-simplex-oss-relay-test-{}-{}",
        relay_count,
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_endpoint = format!(
        "simplex+oss://mesh-target-{relay_count}/target?root={}",
        root.display()
    );
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("simplex-oss-target-{relay_count}")),
        key: None,
    });
    let target_id = target_identity.id.clone();
    let target_endpoint_for_task = target_endpoint.clone();
    let target_task = tokio::spawn(async move {
        let peer = simplex_oss_mux::accept_mux_peer_on(target_identity, &target_endpoint_for_task)
            .await
            .unwrap();
        let (accepted_stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(accepted_stream_id).await;
        proxy_simplex_oss_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            accepted_stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_endpoints: Vec<String> = (0..relay_count)
        .map(|idx| {
            format!(
                "simplex+oss://mesh-relay-{relay_count}-{idx}/tunnel?root={}",
                root.display()
            )
        })
        .collect();

    let mut relay_tasks = Vec::with_capacity(relay_count);
    for idx in (0..relay_count).rev() {
        let downstream_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some(format!("simplex-oss-relay-{relay_count}-{idx}")),
            key: None,
        });
        let upstream_endpoint = if idx + 1 == relay_count {
            target_endpoint.clone()
        } else {
            relay_endpoints[idx + 1].clone()
        };
        let downstream_endpoint = relay_endpoints[idx].clone();
        let target_id = target_id.clone();
        relay_tasks.push(tokio::spawn(async move {
            let downstream_peer =
                simplex_oss_mux::accept_mux_peer_on(downstream_identity, &downstream_endpoint)
                    .await
                    .unwrap();
            let upstream_peer = simplex_oss_mux::connect_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some(format!("simplex-oss-upstream-{relay_count}-{idx}")),
                    key: None,
                }),
                &upstream_endpoint,
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(target_id, upstream_peer);
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            handle_simplex_oss_relay_stream_open(
                Arc::new(Mutex::new(peer_map)),
                Arc::new(Mutex::new(40_000 + (idx as u32 * 1000))),
                Arc::new(Mutex::new(std::collections::HashMap::new())),
                downstream_peer,
                open_frame,
            )
            .await
            .unwrap();
        }));
    }

    let leaf_endpoint = relay_endpoints[0].clone();
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("simplex-oss-leaf-{relay_count}")),
        key: None,
    });
    let leaf_peer = simplex_oss_mux::connect_mux_peer(leaf_identity, &leaf_endpoint)
        .await
        .unwrap();
    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&open_frame).await.unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(stream_id).await;
    let payload_frame = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(payload)),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&payload_frame).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), payload),
        other => panic!(
            "unexpected simplex oss relayed stream response: {:?}",
            other
        ),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    for relay_task in relay_tasks {
        relay_task.await.unwrap();
    }
    target_task.await.unwrap();
    let _ = tokio::fs::remove_dir_all(root).await;
}

async fn simplex_dns_multi_hop_relay_roundtrip(
    relay_count: usize,
    stream_id: u32,
    payload: &[u8],
    scheme: &str,
) {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = simplex_dns::bind(&format!("{scheme}://127.0.0.1:0/target.local"))
        .await
        .unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("simplex-dns-target-{relay_count}")),
        key: None,
    });
    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer =
            simplex_dns_mux::accept_mux_peer_on(target_identity, target_listener, "/target.local")
                .await
                .unwrap();
        let (accepted_stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(accepted_stream_id).await;
        proxy_simplex_dns_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            accepted_stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let mut relay_sockets = Vec::with_capacity(relay_count);
    let mut relay_addrs = Vec::with_capacity(relay_count);
    for _ in 0..relay_count {
        let socket = simplex_dns::bind(&format!("{scheme}://127.0.0.1:0/relay.local"))
            .await
            .unwrap();
        relay_addrs.push(socket.local_addr().unwrap());
        relay_sockets.push(socket);
    }

    let mut relay_tasks = Vec::with_capacity(relay_count);
    for (idx, socket) in relay_sockets.into_iter().enumerate().rev() {
        let downstream_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some(format!("simplex-dns-relay-{relay_count}-{idx}")),
            key: None,
        });
        let upstream_endpoint = if idx + 1 == relay_count {
            format!("{scheme}://{target_addr}/target.local")
        } else {
            format!("{scheme}://{}/relay.local", relay_addrs[idx + 1])
        };
        let target_id = target_id.clone();
        relay_tasks.push(tokio::spawn(async move {
            let downstream_peer =
                simplex_dns_mux::accept_mux_peer_on(downstream_identity, socket, "/relay.local")
                    .await
                    .unwrap();
            let upstream_peer = simplex_dns_mux::connect_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some(format!("simplex-dns-upstream-{relay_count}-{idx}")),
                    key: None,
                }),
                &upstream_endpoint,
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(target_id, upstream_peer);
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            handle_simplex_dns_relay_stream_open(
                Arc::new(Mutex::new(peer_map)),
                Arc::new(Mutex::new(50_000 + (idx as u32 * 1000))),
                Arc::new(Mutex::new(std::collections::HashMap::new())),
                downstream_peer,
                open_frame,
            )
            .await
            .unwrap();
        }));
    }

    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("simplex-dns-leaf-{relay_count}")),
        key: None,
    });
    let leaf_peer = simplex_dns_mux::connect_mux_peer(
        leaf_identity,
        &format!("{scheme}://{}/relay.local", relay_addrs[0]),
    )
    .await
    .unwrap();
    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&open_frame).await.unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(stream_id).await;
    let payload_frame = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(payload)),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&payload_frame).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), payload),
        other => panic!(
            "unexpected simplex dns relayed stream response: {:?}",
            other
        ),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    for relay_task in relay_tasks {
        relay_task.await.unwrap();
    }
    target_task.await.unwrap();
}

async fn h2_multi_hop_relay_roundtrip(relay_count: usize, stream_id: u32, payload: &[u8]) {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = h2_mux::bind("h2://127.0.0.1:0/tunnel").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("h2-target-{relay_count}")),
        key: None,
    });
    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer =
            h2_mux::accept_mux_peer_on(target_identity, &target_listener, None)
                .await
                .unwrap();
        let (accepted_stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(accepted_stream_id).await;
        proxy_h2_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            accepted_stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let mut relay_listeners = Vec::with_capacity(relay_count);
    let mut relay_addrs = Vec::with_capacity(relay_count);
    for _ in 0..relay_count {
        let listener = h2_mux::bind("h2://127.0.0.1:0/tunnel").await.unwrap();
        relay_addrs.push(listener.local_addr().unwrap());
        relay_listeners.push(listener);
    }

    let mut relay_tasks = Vec::with_capacity(relay_count);
    for (idx, listener) in relay_listeners.into_iter().enumerate().rev() {
        let downstream_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some(format!("h2-relay-{relay_count}-{idx}")),
            key: None,
        });
        let upstream_endpoint = if idx + 1 == relay_count {
            format!("h2://{}/tunnel", target_addr)
        } else {
            format!("h2://{}/tunnel", relay_addrs[idx + 1])
        };
        let target_id = target_id.clone();
        relay_tasks.push(tokio::spawn(async move {
            let downstream_peer =
                h2_mux::accept_mux_peer_on(downstream_identity, &listener, None)
                    .await
                    .unwrap();
            let upstream_peer = h2_mux::connect_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some(format!("h2-upstream-{relay_count}-{idx}")),
                    key: None,
                }),
                &upstream_endpoint,
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(target_id, upstream_peer);
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            handle_h2_relay_stream_open(
                Arc::new(Mutex::new(peer_map)),
                Arc::new(Mutex::new(60_000 + (idx as u32 * 1000))),
                Arc::new(Mutex::new(std::collections::HashMap::new())),
                downstream_peer,
                open_frame,
            )
            .await
            .unwrap();
        }));
    }

    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("h2-leaf-{relay_count}")),
        key: None,
    });
    let leaf_peer = h2_mux::connect_mux_peer(
        leaf_identity,
        &format!("h2://{}/tunnel", relay_addrs[0]),
    )
    .await
    .unwrap();
    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&open_frame).await.unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(stream_id).await;
    let payload_frame = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(payload)),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&payload_frame).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), payload),
        other => panic!("unexpected h2 relayed stream response: {:?}", other),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    for relay_task in relay_tasks {
        relay_task.await.unwrap();
    }
    target_task.await.unwrap();
}

async fn simplex_http_multi_hop_relay_roundtrip(
    relay_count: usize,
    stream_id: u32,
    payload: &[u8],
) {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = simplex_http::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("simplex-http-target-{relay_count}")),
        key: None,
    });
    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer =
            simplex_http_mux::accept_mux_peer_on(target_identity, target_listener, "/target")
                .await
                .unwrap();
        let (accepted_stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(accepted_stream_id).await;
        proxy_simplex_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            accepted_stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let mut relay_listeners = Vec::with_capacity(relay_count);
    let mut relay_addrs = Vec::with_capacity(relay_count);
    for _ in 0..relay_count {
        let listener = simplex_http::bind("127.0.0.1:0").await.unwrap();
        relay_addrs.push(listener.local_addr().unwrap());
        relay_listeners.push(listener);
    }

    let mut relay_tasks = Vec::with_capacity(relay_count);
    for (idx, listener) in relay_listeners.into_iter().enumerate().rev() {
        let downstream_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some(format!("simplex-http-relay-{relay_count}-{idx}")),
            key: None,
        });
        let upstream_endpoint = if idx + 1 == relay_count {
            format!("simplex+http://{}/target", target_addr)
        } else {
            format!("simplex+http://{}/relay", relay_addrs[idx + 1])
        };
        let target_id = target_id.clone();
        relay_tasks.push(tokio::spawn(async move {
            let downstream_peer =
                simplex_http_mux::accept_mux_peer_on(downstream_identity, listener, "/relay")
                    .await
                    .unwrap();
            let upstream_peer = simplex_http_mux::connect_mux_peer(
                AgentIdentity::from_config(&AgentIdentityConfig {
                    name: Some(format!("simplex-http-upstream-{relay_count}-{idx}")),
                    key: None,
                }),
                &upstream_endpoint,
            )
            .await
            .unwrap();
            let mut peer_map = std::collections::HashMap::new();
            peer_map.insert(target_id, upstream_peer);
            let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
            handle_simplex_relay_stream_open(
                Arc::new(Mutex::new(peer_map)),
                Arc::new(Mutex::new(60_000 + (idx as u32 * 1000))),
                Arc::new(Mutex::new(std::collections::HashMap::new())),
                downstream_peer,
                open_frame,
            )
            .await
            .unwrap();
        }));
    }

    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some(format!("simplex-http-leaf-{relay_count}")),
        key: None,
    });
    let leaf_peer = simplex_http_mux::connect_mux_peer(
        leaf_identity,
        &format!("simplex+http://{}/relay", relay_addrs[0]),
    )
    .await
    .unwrap();
    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&open_frame).await.unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(stream_id).await;
    let payload_frame = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(payload)),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&payload_frame).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => assert_eq!(data.to_bytes().unwrap(), payload),
        other => panic!(
            "unexpected simplex http relayed stream response: {:?}",
            other
        ),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_id),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(stream_id);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    for relay_task in relay_tasks {
        relay_task.await.unwrap();
    }
    target_task.await.unwrap();
}

#[tokio::test]
async fn task_artifact_save_path_override_is_used() {
    let root = std::env::temp_dir().join(format!("fusion-runtime-test-{}", std::process::id()));
    let path = root.join("custom").join("artifact.txt");
    let req = TaskRequestConfig {
        action: TaskAction::Shell,
        args: vec!["echo hi".into()],
        data_hex: None,
        save_path: Some(path.clone()),
        target_agent_id: None,
    };
    let result = TaskResultMessage {
        task_id: "task-custom-save".into(),
        ok: true,
        output: "hi".into(),
        data_hex: Some(HEXLOWER.encode(b"hello-artifact")),
    };
    let saved = maybe_store_task_artifact(&root, &req, &result)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved, path);
    let bytes = tokio::fs::read(&saved).await.unwrap();
    assert_eq!(bytes, b"hello-artifact");
    let _ = tokio::fs::remove_file(&saved).await;
}

#[tokio::test]
async fn status_snapshot_is_written_and_filtered() {
    let root = std::env::temp_dir().join(format!("fusion-status-test-{}", std::process::id()));
    let hub = Arc::new(Mutex::new(crate::session::hub::SessionHub::new()));
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));
    registry
        .lock()
        .await
        .set_local_services(vec!["service.local=socks5://127.0.0.1:1080".into()]);
    registry.lock().await.upsert_announce(
        &crate::protocol::message::AgentAnnounceMessage {
            agent_id: "peer-x".into(),
            agent_name: "peer-x-name".into(),
            capabilities: vec!["task:shell".into()],
            services: vec!["raw://dynamic".into()],
        },
        "peer-x",
    );
    registry
        .lock()
        .await
        .open_stream(7, "peer-x".into(), "raw".into(), "dynamic".into());
    registry.lock().await.mark_stream_active(7);

    let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
    relay_links.lock().await.insert(
        "tcp:peer-x:7".into(),
        RelayStreamLink {
            transport: "tcp".into(),
            source_peer_agent_id: "peer-x".into(),
            source_stream_id: 7,
            next_hop_agent_id: "peer-y".into(),
            relay_stream_id: 100000,
            destination_agent_id: "peer-z".into(),
            opened_at_unix: 123,
        },
    );
    let upstream_pools = Arc::new(Mutex::new(Vec::new()));
    write_status_snapshot(
        &root,
        &hub,
        &registry,
        &relay_links,
        &upstream_pools,
        &RuntimeConfigSummary::default(),
    )
    .await
    .unwrap();

    let status_file = root.join("runtime-status.json");
    assert!(status_file.exists());
    let payload = tokio::fs::read(&status_file).await.unwrap();
    let snapshot: RuntimeStatusSnapshot = serde_json::from_slice(&payload).unwrap();
    assert_eq!(snapshot.relay_links.len(), 1);
    let stream_lines = render_status_lines(&snapshot, StatusScope::Streams);
    assert!(stream_lines.iter().any(|line| line == "relay.link_count=1"));
    assert!(stream_lines
        .iter()
        .any(|line| line.contains("relay.link transport=tcp")));

    print_status_snapshot(&root, StatusScope::Peers, false)
        .await
        .unwrap();
    print_status_snapshot(&root, StatusScope::Routes, false)
        .await
        .unwrap();
    print_status_snapshot(&root, StatusScope::Streams, false)
        .await
        .unwrap();
    print_status_snapshot(&root, StatusScope::Streams, true)
        .await
        .unwrap();

    let _ = tokio::fs::remove_file(status_file).await;
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[tokio::test]
async fn outbound_task_fails_over_to_second_tcp_endpoint() {
    let listener = bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-failover-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-failover-client".into()),
        key: None,
    });
    let bad_endpoint = TunnelEndpoint {
        url: ParsedUrl::parse("tcp://127.0.0.1:1").unwrap(),
    };
    let good_endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("tcp://{}", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-failover-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = accept_mux_peer_on(server_identity, &listener)
            .await
            .unwrap();
        loop {
            let frame = peer.read_control_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                let result = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "failover-ok".into(),
                        data_hex: None,
                    }),
                );
                peer.send_frame(&result).await.unwrap();
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[bad_endpoint, good_endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo failover".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("task failover timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_task_over_simplex_http_endpoint_succeeds() {
    let listener = simplex_http::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-simplex-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-simplex-client".into()),
        key: None,
    });
    let endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("simplex+http://{}/task", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-simplex-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = simplex_http::accept_peer_on(server_identity, listener, "/task")
            .await
            .unwrap();
        loop {
            let frame = peer.read_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                let result = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "simplex-task-ok".into(),
                        data_hex: None,
                    }),
                );
                peer.send_frame(&result).await.unwrap();
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo simplex".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("simplex task timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_task_over_http_endpoint_succeeds() {
    let listener = simplex_http::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-http-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-http-client".into()),
        key: None,
    });
    let endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("http://{}/task", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-http-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = simplex_http::accept_peer_on(server_identity, listener, "/task")
            .await
            .unwrap();
        loop {
            let frame = peer.read_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                peer.send_frame(&Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "http-task-ok".into(),
                        data_hex: None,
                    }),
                ))
                .await
                .unwrap();
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo http".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("http task timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_task_over_streamhttp_endpoint_succeeds() {
    let listener = streamhttp::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-streamhttp-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-streamhttp-client".into()),
        key: None,
    });
    let endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("streamhttp://{}/task", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-streamhttp-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = streamhttp::accept_peer_on(server_identity, listener, "/task")
            .await
            .unwrap();
        loop {
            let frame = peer.read_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                peer.send_frame(&Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "streamhttp-task-ok".into(),
                        data_hex: None,
                    }),
                ))
                .await
                .unwrap();
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo streamhttp".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("streamhttp task timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_task_over_dns_endpoint_succeeds() {
    let listener = simplex_dns::bind("dns://127.0.0.1:0/task.local")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-dns-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-dns-client".into()),
        key: None,
    });
    let endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("dns://{}/task.local", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-dns-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = simplex_dns::accept_peer_on(server_identity, listener, "/task.local")
            .await
            .unwrap();
        loop {
            let frame = peer.read_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                peer.send_frame(&Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "dns-task-ok".into(),
                        data_hex: None,
                    }),
                ))
                .await
                .unwrap();
                let _ = tokio::time::timeout(Duration::from_secs(1), peer.read_frame()).await;
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo dns".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("dns task timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_task_over_h2_endpoint_succeeds() {
    let listener = h2_mux::bind("h2://127.0.0.1:0/tunnel").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-h2-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-h2-client".into()),
        key: None,
    });
    let endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("h2://{}/tunnel", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-h2-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = h2_mux::accept_mux_peer_on(server_identity, &listener, None)
            .await
            .unwrap();
        loop {
            let frame = peer.read_control_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                peer.send_frame(&Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "h2-task-ok".into(),
                        data_hex: None,
                    }),
                ))
                .await
                .unwrap();
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo h2".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("h2 task timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn outbound_task_over_simplex_dns_endpoint_succeeds() {
    let listener = simplex_dns::bind("simplex+dns://127.0.0.1:0/task.local")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-simplex-dns-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("task-simplex-dns-client".into()),
        key: None,
    });
    let endpoint = TunnelEndpoint {
        url: ParsedUrl::parse(&format!("simplex+dns://{}/task.local", addr)).unwrap(),
    };
    let root = std::env::temp_dir().join(format!(
        "fusion-task-simplex-dns-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &root, RuntimeConfigSummary::default()).await;

    let server = tokio::spawn(async move {
        let peer = simplex_dns::accept_peer_on(server_identity, listener, "/task.local")
            .await
            .unwrap();
        loop {
            let frame = peer.read_frame().await.unwrap();
            if let Message::TaskRequest(req) = frame.message {
                let result = Frame::new(
                    MessageType::TaskResult,
                    Some(peer.session.local.agent_id.clone()),
                    Some(peer.session.remote.agent_id.clone()),
                    Message::TaskResult(TaskResultMessage {
                        task_id: req.task_id,
                        ok: true,
                        output: "simplex-dns-task-ok".into(),
                        data_hex: None,
                    }),
                );
                peer.send_frame(&result).await.unwrap();
                let _ = tokio::time::timeout(Duration::from_secs(1), peer.read_frame()).await;
                break;
            }
        }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        run_outbound_task_once(
            client_identity,
            &[endpoint],
            TaskRequestConfig {
                action: TaskAction::Shell,
                args: vec!["echo simplex dns".into()],
                data_hex: None,
                save_path: None,
                target_agent_id: None,
            },
            root.clone(),
            Vec::new(),
            shared.hub.clone(),
            shared.registry.clone(),
            crate::app::config::ConnPolicy::Fallback,
            Vec::new(),
        ),
    )
    .await
    .expect("simplex dns task timed out")
    .unwrap();

    server.await.unwrap();
}

#[tokio::test]
async fn tcp_relay_stream_bridge_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_down_listener = bind("127.0.0.1:0").await.unwrap();
    let relay_down_addr = relay_down_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("relay-stream-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("relay-stream-node".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("relay-stream-leaf".into()),
        key: None,
    });

    let target_task = tokio::spawn(async move {
        let peer = accept_mux_peer(target_identity, target_listener)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_accept = tokio::spawn(async move {
        accept_mux_peer(relay_identity.clone(), relay_down_listener)
            .await
            .unwrap()
    });

    let target_peer = connect_mux_peer(
        AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("relay-upstream".into()),
            key: None,
        }),
        &target_addr.to_string(),
    )
    .await
    .unwrap();
    let leaf_peer = connect_mux_peer(leaf_identity, &relay_down_addr.to_string())
        .await
        .unwrap();
    let downstream_peer = relay_accept.await.unwrap();

    let mut peer_map = std::collections::HashMap::new();
    peer_map.insert(
        target_peer.session.remote.agent_id.clone(),
        target_peer.clone(),
    );
    let peer_map = Arc::new(Mutex::new(peer_map));
    let allocator = Arc::new(Mutex::new(1000_u32));

    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(7);

    let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
    handle_tcp_relay_stream_open(
        peer_map,
        allocator,
        relay_links,
        downstream_peer,
        open_frame,
    )
    .await
    .unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(7).await;
    let payload = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(b"relay-stream-ok")),
    )
    .with_stream_id(7);
    leaf_peer.send_frame(&payload).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => {
            assert_eq!(data.to_bytes().unwrap(), b"relay-stream-ok");
        }
        other => panic!("unexpected relayed stream response: {:?}", other),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(7);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    target_task.await.unwrap();
}

#[tokio::test]
async fn tcp_relay_stream_bridge_roundtrip_through_three_hops() {
    tcp_multi_hop_relay_roundtrip(2, 21, b"relay-3-hop-ok").await;
}

#[tokio::test]
async fn tcp_relay_stream_bridge_roundtrip_through_five_hops() {
    tcp_multi_hop_relay_roundtrip(4, 31, b"relay-5-hop-ok").await;
}

#[tokio::test]
async fn simplex_oss_relay_stream_bridge_roundtrip_through_three_hops() {
    simplex_oss_multi_hop_relay_roundtrip(2, 41, b"simplex-oss-relay-3-hop-ok").await;
}

#[tokio::test]
async fn simplex_oss_relay_stream_bridge_roundtrip_through_five_hops() {
    simplex_oss_multi_hop_relay_roundtrip(4, 42, b"simplex-oss-relay-5-hop-ok").await;
}

#[tokio::test]
async fn simplex_dns_relay_stream_bridge_roundtrip_through_three_hops() {
    simplex_dns_multi_hop_relay_roundtrip(2, 51, b"simplex-dns-relay-3-hop-ok", "simplex+dns").await;
}

#[tokio::test]
async fn simplex_dns_relay_stream_bridge_roundtrip_through_five_hops() {
    simplex_dns_multi_hop_relay_roundtrip(4, 52, b"simplex-dns-relay-5-hop-ok", "simplex+dns").await;
}

#[tokio::test]
async fn dns_relay_stream_bridge_roundtrip_through_three_hops() {
    simplex_dns_multi_hop_relay_roundtrip(2, 91, b"dns-relay-3-hop-ok", "dns").await;
}

#[tokio::test]
async fn h2_relay_stream_bridge_roundtrip_through_three_hops() {
    h2_multi_hop_relay_roundtrip(2, 81, b"h2-relay-3-hop-ok").await;
}

#[tokio::test]
async fn h2_relay_stream_bridge_roundtrip_through_five_hops() {
    h2_multi_hop_relay_roundtrip(4, 82, b"h2-relay-5-hop-ok").await;
}

#[tokio::test]
async fn h2_relay_stream_bridge_roundtrip_through_ten_hops() {
    h2_multi_hop_relay_roundtrip(9, 83, b"h2-relay-10-hop-ok").await;
}

#[tokio::test]
async fn simplex_http_relay_stream_bridge_roundtrip_through_three_hops() {
    simplex_http_multi_hop_relay_roundtrip(2, 61, b"simplex-http-relay-3-hop-ok").await;
}

#[tokio::test]
async fn simplex_http_relay_stream_bridge_roundtrip_through_five_hops() {
    simplex_http_multi_hop_relay_roundtrip(4, 62, b"simplex-http-relay-5-hop-ok").await;
}

#[tokio::test]
async fn tcp_relay_stream_bridge_roundtrip_through_ten_hops() {
    tcp_multi_hop_relay_roundtrip(9, 71, b"relay-10-hop-ok").await;
}

#[tokio::test]
async fn simplex_http_relay_stream_bridge_roundtrip_through_ten_hops() {
    simplex_http_multi_hop_relay_roundtrip(9, 72, b"simplex-http-relay-10-hop-ok").await;
}

#[tokio::test]
async fn ws_relay_stream_bridge_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind_ws("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_down_listener = bind_ws("127.0.0.1:0").await.unwrap();
    let relay_down_addr = relay_down_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-relay-stream-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-relay-stream-node".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-relay-stream-leaf".into()),
        key: None,
    });

    let target_task = tokio::spawn(async move {
        let peer = accept_ws_mux_peer(target_identity, target_listener, None)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_ws_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_accept = tokio::spawn(async move {
        accept_ws_mux_peer(relay_identity.clone(), relay_down_listener, None)
            .await
            .unwrap()
    });

    let target_peer = connect_ws_mux_peer(
        AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("ws-relay-upstream".into()),
            key: None,
        }),
        &format!("ws://{}/tunnel", target_addr),
    )
    .await
    .unwrap();
    let leaf_peer = connect_ws_mux_peer(leaf_identity, &format!("ws://{}/tunnel", relay_down_addr))
        .await
        .unwrap();
    let downstream_peer = relay_accept.await.unwrap();

    let mut peer_map = std::collections::HashMap::new();
    peer_map.insert(
        target_peer.session.remote.agent_id.clone(),
        target_peer.clone(),
    );
    let peer_map = Arc::new(Mutex::new(peer_map));
    let allocator = Arc::new(Mutex::new(2000_u32));

    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(9);

    let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
    handle_ws_relay_stream_open(
        peer_map,
        allocator,
        relay_links,
        downstream_peer,
        open_frame,
    )
    .await
    .unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(9).await;
    let payload = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(b"ws-relay-stream-ok")),
    )
    .with_stream_id(9);
    leaf_peer.send_frame(&payload).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => {
            assert_eq!(data.to_bytes().unwrap(), b"ws-relay-stream-ok");
        }
        other => panic!("unexpected ws relayed stream response: {:?}", other),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(9);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    target_task.await.unwrap();
}

#[tokio::test]
async fn h2_relay_stream_bridge_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = h2_mux::bind("h2://127.0.0.1:0/tunnel").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_down_listener = h2_mux::bind("h2://127.0.0.1:0/tunnel").await.unwrap();
    let relay_down_addr = relay_down_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("h2-relay-stream-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("h2-relay-stream-node".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("h2-relay-stream-leaf".into()),
        key: None,
    });

    let target_task = tokio::spawn(async move {
        let peer = h2_mux::accept_mux_peer_on(target_identity, &target_listener, None)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_h2_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_accept = tokio::spawn(async move {
        h2_mux::accept_mux_peer_on(relay_identity.clone(), &relay_down_listener, None)
            .await
            .unwrap()
    });

    let target_peer = h2_mux::connect_mux_peer(
        AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("h2-relay-upstream".into()),
            key: None,
        }),
        &format!("h2://{}/tunnel", target_addr),
    )
    .await
    .unwrap();
    let leaf_peer = h2_mux::connect_mux_peer(
        leaf_identity,
        &format!("h2://{}/tunnel", relay_down_addr),
    )
    .await
    .unwrap();
    let downstream_peer = relay_accept.await.unwrap();

    let mut peer_map = std::collections::HashMap::new();
    peer_map.insert(
        target_peer.session.remote.agent_id.clone(),
        target_peer.clone(),
    );
    let peer_map = Arc::new(Mutex::new(peer_map));
    let allocator = Arc::new(Mutex::new(3000_u32));

    let open_frame = Frame::new(
        MessageType::StreamOpen,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamOpen(StreamOpenMessage {
            service: "raw".into(),
            target_host: Some("127.0.0.1".into()),
            target_port: Some(echo_addr.port()),
        }),
    )
    .with_stream_id(19);

    let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
    handle_h2_relay_stream_open(
        peer_map,
        allocator,
        relay_links,
        downstream_peer,
        open_frame,
    )
    .await
    .unwrap();

    let mut leaf_rx = leaf_peer.open_stream_receiver(19).await;
    let payload = Frame::new(
        MessageType::StreamData,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamData(StreamDataMessage::from_bytes(b"h2-relay-stream-ok")),
    )
    .with_stream_id(19);
    leaf_peer.send_frame(&payload).await.unwrap();

    let response = leaf_rx.recv().await.unwrap();
    match response.message {
        Message::StreamData(data) => {
            assert_eq!(data.to_bytes().unwrap(), b"h2-relay-stream-ok");
        }
        other => panic!("unexpected h2 relayed stream response: {:?}", other),
    }

    let close = Frame::new(
        MessageType::StreamClose,
        Some(leaf_peer.session.local.agent_id.clone()),
        Some(target_peer.session.remote.agent_id.clone()),
        Message::StreamClose(StreamCloseMessage { reason: None }),
    )
    .with_stream_id(19);
    leaf_peer.send_frame(&close).await.unwrap();
    let close_ack = leaf_rx.recv().await.unwrap();
    assert!(matches!(close_ack.message, Message::StreamClose(_)));

    target_task.await.unwrap();
}

#[tokio::test]
async fn tcp_socks5_over_relay_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_listener = bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("tcp-socks-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("tcp-socks-relay".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("tcp-socks-leaf".into()),
        key: None,
    });

    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer = accept_mux_peer(target_identity, target_listener)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_task = tokio::spawn(async move {
        let downstream_peer = accept_mux_peer(relay_identity, relay_listener)
            .await
            .unwrap();
        let upstream_peer = connect_mux_peer(
            AgentIdentity::from_config(&AgentIdentityConfig {
                name: Some("tcp-socks-upstream".into()),
                key: None,
            }),
            &target_addr.to_string(),
        )
        .await
        .unwrap();
        let mut peer_map = std::collections::HashMap::new();
        peer_map.insert(
            upstream_peer.session.remote.agent_id.clone(),
            upstream_peer.clone(),
        );
        let peer_map = Arc::new(Mutex::new(peer_map));
        let allocator = Arc::new(Mutex::new(3000_u32));
        let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        handle_tcp_relay_stream_open(
            peer_map,
            allocator,
            relay_links,
            downstream_peer,
            open_frame,
        )
        .await
        .unwrap();
    });

    let leaf_peer = connect_mux_peer(leaf_identity, &relay_addr.to_string())
        .await
        .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let remote_def = dynamic_raw_service_definition();
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let client_task = tokio::spawn(async move {
        local_client.write_all(b"\x05\x01\x00").await.unwrap();
        let mut method_resp = [0_u8; 2];
        local_client.read_exact(&mut method_resp).await.unwrap();
        assert_eq!(&method_resp, b"\x05\x00");

        let mut connect_req = vec![0x05, 0x01, 0x00, 0x01];
        connect_req.extend_from_slice(&[127, 0, 0, 1]);
        connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
        local_client.write_all(&connect_req).await.unwrap();

        let mut connect_resp = [0_u8; 10];
        local_client.read_exact(&mut connect_resp).await.unwrap();
        assert_eq!(&connect_resp[..2], b"\x05\x00");

        local_client.write_all(b"tcp-socks-relay-ok").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"tcp-socks-relay-ok");
    });

    handle_outbound_socks5_client(
        leaf_peer,
        Socks5Service::from_url(&ParsedUrl::parse("socks5://127.0.0.1:1080").unwrap()).unwrap(),
        remote_def,
        Some(target_id),
        local_server,
        1,
        registry,
    )
    .await
    .unwrap();

    client_task.await.unwrap();
    relay_task.await.unwrap();
    target_task.await.unwrap();
}

#[tokio::test]
async fn ws_socks5_over_relay_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind_ws("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_listener = bind_ws("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-socks-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-socks-relay".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-socks-leaf".into()),
        key: None,
    });

    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer = accept_ws_mux_peer(target_identity, target_listener, None)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_ws_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_task = tokio::spawn(async move {
        let downstream_peer = accept_ws_mux_peer(relay_identity, relay_listener, None)
            .await
            .unwrap();
        let upstream_peer = connect_ws_mux_peer(
            AgentIdentity::from_config(&AgentIdentityConfig {
                name: Some("ws-socks-upstream".into()),
                key: None,
            }),
            &format!("ws://{}/tunnel", target_addr),
        )
        .await
        .unwrap();
        let mut peer_map = std::collections::HashMap::new();
        peer_map.insert(
            upstream_peer.session.remote.agent_id.clone(),
            upstream_peer.clone(),
        );
        let peer_map = Arc::new(Mutex::new(peer_map));
        let allocator = Arc::new(Mutex::new(4000_u32));
        let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        handle_ws_relay_stream_open(
            peer_map,
            allocator,
            relay_links,
            downstream_peer,
            open_frame,
        )
        .await
        .unwrap();
    });

    let leaf_peer = connect_ws_mux_peer(leaf_identity, &format!("ws://{}/tunnel", relay_addr))
        .await
        .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let remote_def = dynamic_raw_service_definition();
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let client_task = tokio::spawn(async move {
        local_client.write_all(b"\x05\x01\x00").await.unwrap();
        let mut method_resp = [0_u8; 2];
        local_client.read_exact(&mut method_resp).await.unwrap();
        assert_eq!(&method_resp, b"\x05\x00");

        let mut connect_req = vec![0x05, 0x01, 0x00, 0x01];
        connect_req.extend_from_slice(&[127, 0, 0, 1]);
        connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
        local_client.write_all(&connect_req).await.unwrap();

        let mut connect_resp = [0_u8; 10];
        local_client.read_exact(&mut connect_resp).await.unwrap();
        assert_eq!(&connect_resp[..2], b"\x05\x00");

        local_client.write_all(b"ws-socks-relay-ok").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ws-socks-relay-ok");
    });

    handle_outbound_socks5_ws_client(
        leaf_peer,
        Socks5Service::from_url(&ParsedUrl::parse("socks5://127.0.0.1:1080").unwrap()).unwrap(),
        remote_def,
        Some(target_id),
        local_server,
        1,
        registry,
    )
    .await
    .unwrap();

    client_task.await.unwrap();
    relay_task.await.unwrap();
    target_task.await.unwrap();
}

#[tokio::test]
async fn simplex_http_socks5_over_raw_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = simplex_http::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("simplex-socks-target".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("simplex-socks-leaf".into()),
        key: None,
    });
    let target_id = target_identity.id.clone();

    let target_task = tokio::spawn(async move {
        let peer = simplex_http_mux::accept_mux_peer_on(target_identity, target_listener, "/socks")
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_simplex_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let leaf_peer = simplex_http_mux::connect_mux_peer(
        leaf_identity,
        &format!("simplex+http://{}/socks", target_addr),
    )
    .await
    .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let remote_def = dynamic_raw_service_definition();
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let client_task = tokio::spawn(async move {
        local_client.write_all(b"\x05\x01\x00").await.unwrap();
        let mut method_resp = [0_u8; 2];
        local_client.read_exact(&mut method_resp).await.unwrap();
        assert_eq!(&method_resp, b"\x05\x00");

        let mut connect_req = vec![0x05, 0x01, 0x00, 0x01];
        connect_req.extend_from_slice(&[127, 0, 0, 1]);
        connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
        local_client.write_all(&connect_req).await.unwrap();

        let mut connect_resp = [0_u8; 10];
        local_client.read_exact(&mut connect_resp).await.unwrap();
        assert_eq!(&connect_resp[..2], b"\x05\x00");

        local_client.write_all(b"simplex-socks-ok").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"simplex-socks-ok");
    });

    handle_outbound_socks5_simplex_http_client(
        leaf_peer,
        Socks5Service::from_url(&ParsedUrl::parse("socks5://127.0.0.1:1080").unwrap()).unwrap(),
        remote_def,
        Some(target_id),
        local_server,
        55,
        registry,
    )
    .await
    .unwrap();

    client_task.await.unwrap();
    target_task.await.unwrap();
}

#[tokio::test]
async fn simplex_http_port_forward_over_raw_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = simplex_http::bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("simplex-port-target".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("simplex-port-leaf".into()),
        key: None,
    });
    let target_id = target_identity.id.clone();

    let target_task = tokio::spawn(async move {
        let peer = simplex_http_mux::accept_mux_peer_on(target_identity, target_listener, "/port")
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_simplex_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let leaf_peer = simplex_http_mux::connect_mux_peer(
        leaf_identity,
        &format!("simplex+http://{}/port", target_addr),
    )
    .await
    .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let port_forward = PortForwardService {
        listen_host: "127.0.0.1".into(),
        listen_port: 0,
        target_host: "127.0.0.1".into(),
        target_port: echo_addr.port(),
    };
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let client_task = tokio::spawn(async move {
        local_client.write_all(b"simplex-port-ok").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"simplex-port-ok");
    });

    handle_port_forward_simplex_http_client(
        leaf_peer,
        port_forward,
        Some(target_id),
        local_server,
        56,
        registry,
    )
    .await
    .unwrap();

    client_task.await.unwrap();
    target_task.await.unwrap();
}

#[tokio::test]
async fn tcp_trojan_over_raw_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("trojan-target".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("trojan-leaf".into()),
        key: None,
    });
    let target_id = target_identity.id.clone();

    let target_task = tokio::spawn(async move {
        let peer = accept_mux_peer(target_identity, target_listener)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let leaf_peer = connect_mux_peer(leaf_identity, &target_addr.to_string())
        .await
        .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let remote_def = dynamic_raw_service_definition();
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));
    let trojan_service =
        TrojanService::from_url(&ParsedUrl::parse("trojan://127.0.0.1:443?password=secret").unwrap())
            .unwrap();

    let client_task = tokio::spawn(async move {
        let payload = encode_trojan_connect_request(
            "secret",
            "127.0.0.1",
            echo_addr.port(),
            b"trojan-raw-ok",
        )
        .unwrap();
        local_client.write_all(&payload).await.unwrap();
        let mut buf = [0_u8; 64];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"trojan-raw-ok");
    });

    let mut client = trojan_plain_client(local_server);
    handle_outbound_trojan_client(
        leaf_peer,
        trojan_service,
        remote_def,
        Some(target_id),
        &mut client,
        57,
        registry,
    )
    .await
    .unwrap();

    client_task.await.unwrap();
    target_task.await.unwrap();
}

#[tokio::test]
async fn tcp_http_proxy_over_relay_roundtrip() {
    let http_listener = bind("127.0.0.1:0").await.unwrap();
    let http_addr = http_listener.local_addr().unwrap();
    let http_server = tokio::spawn(async move {
        let (mut stream, _) = http_listener.accept().await.unwrap();
        let mut buf = Vec::new();
        loop {
            let mut chunk = [0_u8; 512];
            let n = stream.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
        buf
    });

    let target_listener = bind("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_listener = bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("tcp-http-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("tcp-http-relay".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("tcp-http-leaf".into()),
        key: None,
    });

    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer = accept_mux_peer(target_identity, target_listener)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_task = tokio::spawn(async move {
        let downstream_peer = accept_mux_peer(relay_identity, relay_listener)
            .await
            .unwrap();
        let upstream_peer = connect_mux_peer(
            AgentIdentity::from_config(&AgentIdentityConfig {
                name: Some("tcp-http-upstream".into()),
                key: None,
            }),
            &target_addr.to_string(),
        )
        .await
        .unwrap();
        let mut peer_map = std::collections::HashMap::new();
        peer_map.insert(
            upstream_peer.session.remote.agent_id.clone(),
            upstream_peer.clone(),
        );
        let peer_map = Arc::new(Mutex::new(peer_map));
        let allocator = Arc::new(Mutex::new(5000_u32));
        let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        handle_tcp_relay_stream_open(
            peer_map,
            allocator,
            relay_links,
            downstream_peer,
            open_frame,
        )
        .await
        .unwrap();
    });

    let leaf_peer = connect_mux_peer(leaf_identity, &relay_addr.to_string())
        .await
        .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let remote_def = dynamic_raw_service_definition();
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let client_task = tokio::spawn(async move {
        local_client
            .write_all(
                format!(
                    "GET http://127.0.0.1:{}/hello?x=1 HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
                    http_addr.port(),
                    http_addr.port()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let mut buf = [0_u8; 128];
        let n = local_client.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.ends_with("\r\n\r\nok"));
    });

    let handler_task = tokio::spawn(handle_outbound_http_client(
        leaf_peer,
        HttpProxyService::from_url(&ParsedUrl::parse("http://127.0.0.1:8080").unwrap()).unwrap(),
        remote_def,
        Some(target_id),
        local_server,
        1,
        registry,
    ));

    tokio::time::timeout(Duration::from_secs(5), client_task)
        .await
        .expect("tcp http proxy client timed out")
        .unwrap();
    let req = String::from_utf8(
        tokio::time::timeout(Duration::from_secs(5), http_server)
            .await
            .expect("tcp http upstream server timed out")
            .unwrap(),
    )
    .unwrap();
    assert!(req.starts_with("GET /hello?x=1 HTTP/1.1\r\n"));
    assert!(req.contains("Host: 127.0.0.1:"));

    handler_task.abort();
    relay_task.abort();
    target_task.abort();
}

#[tokio::test]
async fn ws_http_connect_over_relay_roundtrip() {
    let echo_listener = bind("127.0.0.1:0").await.unwrap();
    let echo_addr = echo_listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = echo_listener.accept().await.unwrap();
        let mut buf = [0_u8; 256];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            stream.write_all(&buf[..n]).await.unwrap();
        }
    });

    let target_listener = bind_ws("127.0.0.1:0").await.unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let relay_listener = bind_ws("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_listener.local_addr().unwrap();

    let target_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-http-target".into()),
        key: None,
    });
    let relay_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-http-relay".into()),
        key: None,
    });
    let leaf_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("ws-http-leaf".into()),
        key: None,
    });

    let target_id = target_identity.id.clone();
    let target_task = tokio::spawn(async move {
        let peer = accept_ws_mux_peer(target_identity, target_listener, None)
            .await
            .unwrap();
        let (stream_id, open) = peer.read_stream_open().await.unwrap();
        let rx = peer.open_stream_receiver(stream_id).await;
        proxy_ws_mux_stream_loop(
            peer,
            &RawService {
                host: None,
                port: None,
            },
            open,
            stream_id,
            rx,
        )
        .await
        .unwrap();
    });

    let relay_task = tokio::spawn(async move {
        let downstream_peer = accept_ws_mux_peer(relay_identity, relay_listener, None)
            .await
            .unwrap();
        let upstream_peer = connect_ws_mux_peer(
            AgentIdentity::from_config(&AgentIdentityConfig {
                name: Some("ws-http-upstream".into()),
                key: None,
            }),
            &format!("ws://{}/tunnel", target_addr),
        )
        .await
        .unwrap();
        let mut peer_map = std::collections::HashMap::new();
        peer_map.insert(
            upstream_peer.session.remote.agent_id.clone(),
            upstream_peer.clone(),
        );
        let peer_map = Arc::new(Mutex::new(peer_map));
        let allocator = Arc::new(Mutex::new(6000_u32));
        let open_frame = downstream_peer.read_stream_open_frame().await.unwrap();
        let relay_links = Arc::new(Mutex::new(std::collections::HashMap::new()));
        handle_ws_relay_stream_open(
            peer_map,
            allocator,
            relay_links,
            downstream_peer,
            open_frame,
        )
        .await
        .unwrap();
    });

    let leaf_peer = connect_ws_mux_peer(leaf_identity, &format!("ws://{}/tunnel", relay_addr))
        .await
        .unwrap();
    let (mut local_client, local_server) = tcp_socket_pair().await;
    let remote_def = dynamic_raw_service_definition();
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let client_task = tokio::spawn(async move {
        local_client
            .write_all(
                format!(
                    "CONNECT 127.0.0.1:{} HTTP/1.1\r\nHost: 127.0.0.1:{}\r\n\r\n",
                    echo_addr.port(),
                    echo_addr.port()
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        let mut established = [0_u8; 128];
        let n = local_client.read(&mut established).await.unwrap();
        let response = String::from_utf8_lossy(&established[..n]);
        assert!(response.starts_with("HTTP/1.1 200 Connection Established\r\n\r\n"));

        local_client.write_all(b"ws-http-connect-ok").await.unwrap();
        let mut buf = [0_u8; 64];
        let n = local_client.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ws-http-connect-ok");
    });

    let handler_task = tokio::spawn(handle_outbound_http_ws_client(
        leaf_peer,
        HttpProxyService::from_url(&ParsedUrl::parse("http://127.0.0.1:8080").unwrap()).unwrap(),
        remote_def,
        Some(target_id),
        local_server,
        1,
        registry,
    ));

    tokio::time::timeout(Duration::from_secs(5), client_task)
        .await
        .expect("ws http proxy client timed out")
        .unwrap();
    handler_task.abort();
    relay_task.abort();
    target_task.abort();
}

#[tokio::test]
async fn udp_inbound_runtime_registers_direct_session() {
    let probe = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let data_dir = std::env::temp_dir().join(format!(
        "fusion-udp-runtime-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &data_dir, RuntimeConfigSummary::default()).await;

    let listener_config = AppConfig {
        listens: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&format!("udp://127.0.0.1:{port}")).unwrap(),
        }],
        connects: vec![],
        up_connects: vec![],
        down_connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        proxy_chain: vec![],
        front_proxy: None,
        conn_policy: crate::app::config::ConnPolicy::Fallback,
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("udp-runtime-listener".into()),
            key: None,
        },
        retry: RetryPolicy::default(),
        wrapper: crate::crypto::wrapper::WrapperConfig::default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: None,
        data_dir: data_dir.clone(),
        log_level: "info".into(),
    };
    let listener_identity = AgentIdentity::from_config(&listener_config.identity);
    let inbound_tasks = spawn_inbound_tasks(
        &listener_config,
        &listener_identity,
        &shared,
        None,
        false,
        &[],
    )
    .await;
    assert_eq!(inbound_tasks.len(), 1);

    let outbound_config = AppConfig {
        listens: vec![],
        connects: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&format!("udp://127.0.0.1:{port}")).unwrap(),
        }],
        up_connects: vec![],
        down_connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        proxy_chain: vec![],
        front_proxy: None,
        conn_policy: crate::app::config::ConnPolicy::Fallback,
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("udp-runtime-dialer".into()),
            key: None,
        },
        retry: RetryPolicy {
            max_retries: Some(1),
            interval_secs: 1,
            max_interval_secs: 1,
        },
        wrapper: crate::crypto::wrapper::WrapperConfig::default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: None,
        data_dir: data_dir.clone(),
        log_level: "info".into(),
    };
    let dialer_identity = AgentIdentity::from_config(&outbound_config.identity);
    let outbound_tasks = spawn_outbound_tasks(
        &outbound_config,
        &dialer_identity,
        &shared,
        None,
        None,
        None,
        None,
        None,
        &[],
        &[],
    );
    assert_eq!(outbound_tasks.len(), 1);

    for task in outbound_tasks {
        task.await.unwrap();
    }
    for task in inbound_tasks {
        task.await.unwrap();
    }

    let sessions = shared.hub.lock().await.sessions_snapshot();
    let peers = shared.registry.lock().await.peers_snapshot();
    assert!(!sessions.is_empty());
    assert!(!peers.is_empty());
    assert!(sessions
        .iter()
        .any(|s| s.remote.agent_name == "udp-runtime-dialer"));
    assert!(peers
        .iter()
        .any(|p| p.session.remote.agent_name == "udp-runtime-dialer"));
}

#[tokio::test]
async fn simplex_http_inbound_runtime_registers_direct_session() {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let data_dir = std::env::temp_dir().join(format!(
        "fusion-simplex-http-runtime-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &data_dir, RuntimeConfigSummary::default()).await;

    let listener_config = AppConfig {
        listens: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&format!("simplex+http://127.0.0.1:{port}/runtime")).unwrap(),
        }],
        connects: vec![],
        up_connects: vec![],
        down_connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        proxy_chain: vec![],
        front_proxy: None,
        conn_policy: crate::app::config::ConnPolicy::Fallback,
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("simplex-http-runtime-listener".into()),
            key: None,
        },
        retry: RetryPolicy::default(),
        wrapper: crate::crypto::wrapper::WrapperConfig::default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: None,
        data_dir: data_dir.clone(),
        log_level: "info".into(),
    };
    let listener_identity = AgentIdentity::from_config(&listener_config.identity);
    let inbound_tasks = spawn_inbound_tasks(
        &listener_config,
        &listener_identity,
        &shared,
        None,
        false,
        &[],
    )
    .await;
    assert_eq!(inbound_tasks.len(), 1);

    let outbound_config = AppConfig {
        listens: vec![],
        connects: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&format!("simplex+http://127.0.0.1:{port}/runtime")).unwrap(),
        }],
        up_connects: vec![],
        down_connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        proxy_chain: vec![],
        front_proxy: None,
        conn_policy: crate::app::config::ConnPolicy::Fallback,
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("simplex-http-runtime-dialer".into()),
            key: None,
        },
        retry: RetryPolicy {
            max_retries: Some(1),
            interval_secs: 1,
            max_interval_secs: 1,
        },
        wrapper: crate::crypto::wrapper::WrapperConfig::default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: None,
        data_dir: data_dir.clone(),
        log_level: "info".into(),
    };
    let dialer_identity = AgentIdentity::from_config(&outbound_config.identity);
    let outbound_tasks = spawn_outbound_tasks(
        &outbound_config,
        &dialer_identity,
        &shared,
        None,
        None,
        None,
        None,
        None,
        &[],
        &[],
    );
    assert_eq!(outbound_tasks.len(), 1);

    for task in outbound_tasks {
        task.await.unwrap();
    }
    for task in inbound_tasks {
        task.await.unwrap();
    }

    let sessions = shared.hub.lock().await.sessions_snapshot();
    let peers = shared.registry.lock().await.peers_snapshot();
    assert!(!sessions.is_empty());
    assert!(!peers.is_empty());
    assert!(sessions
        .iter()
        .any(|s| s.remote.agent_name == "simplex-http-runtime-dialer"));
    assert!(peers
        .iter()
        .any(|p| p.session.remote.agent_name == "simplex-http-runtime-dialer"));
}

#[tokio::test]
async fn outbound_task_over_simplex_oss_endpoint_succeeds() {
    let root = std::env::temp_dir().join(format!(
        "fusion-simplex-oss-task-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let endpoint = format!("simplex+oss://mesh-task/channel?root={}", root.display());
    let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("simplex-oss-task-server".into()),
        key: None,
    });
    let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
        name: Some("simplex-oss-task-client".into()),
        key: None,
    });
    let hub = Arc::new(Mutex::new(crate::session::hub::SessionHub::new()));
    let registry = Arc::new(Mutex::new(crate::agent::registry::AgentRegistry::new()));

    let server_endpoint = endpoint.clone();
    let server_task = tokio::spawn(async move {
        let peer = simplex_oss::accept_peer_on(server_identity, &server_endpoint)
            .await
            .unwrap();
        loop {
            let frame = peer.read_frame().await.unwrap();
            if matches!(
                frame.message,
                Message::AgentAnnounce(_) | Message::RouteUpdate(_)
            ) {
                continue;
            }
            match frame.message {
                Message::TaskRequest(request) => {
                    let response = Frame::new(
                        MessageType::TaskResult,
                        Some(peer.session.local.agent_id.clone()),
                        frame.header.src_agent.clone(),
                        Message::TaskResult(TaskResultMessage {
                            task_id: request.task_id,
                            ok: true,
                            output: "simplex oss ok".into(),
                            data_hex: None,
                        }),
                    );
                    peer.send_frame(&response).await.unwrap();
                    break;
                }
                other => panic!("unexpected frame: {:?}", other),
            }
        }
    });

    run_outbound_task_once(
        client_identity,
        &[TunnelEndpoint {
            url: ParsedUrl::parse(&endpoint).unwrap(),
        }],
        TaskRequestConfig {
            action: TaskAction::Shell,
            args: vec!["echo".into(), "hello".into()],
            data_hex: None,
            save_path: None,
            target_agent_id: None,
        },
        std::env::temp_dir(),
        vec!["svc:a".into()],
        hub,
        registry,
        crate::app::config::ConnPolicy::Fallback,
        vec![],
    )
    .await
    .unwrap();

    server_task.await.unwrap();
}

#[tokio::test]
async fn simplex_oss_inbound_runtime_registers_direct_session() {
    let root = std::env::temp_dir().join(format!(
        "fusion-simplex-oss-runtime-test-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let endpoint = format!("simplex+oss://mesh-runtime/channel?root={}", root.display());
    let data_dir = std::env::temp_dir().join(format!(
        "fusion-simplex-oss-runtime-data-{}-{}",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let shared = RuntimeShared::new(Vec::new(), &data_dir, RuntimeConfigSummary::default()).await;

    let listener_config = AppConfig {
        listens: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&endpoint).unwrap(),
        }],
        connects: vec![],
        up_connects: vec![],
        down_connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        proxy_chain: vec![],
        front_proxy: None,
        conn_policy: crate::app::config::ConnPolicy::Fallback,
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("simplex-oss-runtime-listener".into()),
            key: None,
        },
        retry: RetryPolicy::default(),
        wrapper: crate::crypto::wrapper::WrapperConfig::default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: None,
        data_dir: data_dir.clone(),
        log_level: "info".into(),
    };
    let listener_identity = AgentIdentity::from_config(&listener_config.identity);
    let inbound_tasks = spawn_inbound_tasks(
        &listener_config,
        &listener_identity,
        &shared,
        None,
        false,
        &[],
    )
    .await;
    assert_eq!(inbound_tasks.len(), 1);

    let outbound_config = AppConfig {
        listens: vec![],
        connects: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&endpoint).unwrap(),
        }],
        up_connects: vec![],
        down_connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        proxy_chain: vec![],
        front_proxy: None,
        conn_policy: crate::app::config::ConnPolicy::Fallback,
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("simplex-oss-runtime-dialer".into()),
            key: None,
        },
        retry: RetryPolicy {
            max_retries: Some(1),
            interval_secs: 1,
            max_interval_secs: 1,
        },
        wrapper: crate::crypto::wrapper::WrapperConfig::default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: None,
        data_dir: data_dir.clone(),
        log_level: "info".into(),
    };
    let dialer_identity = AgentIdentity::from_config(&outbound_config.identity);
    let outbound_tasks = spawn_outbound_tasks(
        &outbound_config,
        &dialer_identity,
        &shared,
        None,
        None,
        None,
        None,
        None,
        &[],
        &[],
    );
    assert_eq!(outbound_tasks.len(), 1);

    for task in outbound_tasks {
        task.await.unwrap();
    }
    for task in inbound_tasks {
        task.await.unwrap();
    }

    let sessions = shared.hub.lock().await.sessions_snapshot();
    let peers = shared.registry.lock().await.peers_snapshot();
    assert!(sessions
        .iter()
        .any(|s| s.remote.agent_name == "simplex-oss-runtime-dialer"));
    assert!(peers
        .iter()
        .any(|p| p.session.remote.agent_name == "simplex-oss-runtime-dialer"));
}
