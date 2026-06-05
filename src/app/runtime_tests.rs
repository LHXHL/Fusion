use crate::{
    agent::identity::AgentIdentity,
    app::{
        config::{AgentIdentityConfig, AppConfig, RetryPolicy, ServeEndpoint, StatusScope, TaskRequestConfig, TunnelEndpoint},
        runtime_http::{handle_outbound_http_client, handle_outbound_http_ws_client},
        runtime_orchestrator::{spawn_inbound_tasks, spawn_outbound_tasks, RuntimeShared},
        runtime_relay::{handle_tcp_relay_stream_open, handle_ws_relay_stream_open},
        runtime_socks5::{handle_outbound_socks5_client, handle_outbound_socks5_ws_client},
        runtime_status::{
            print_status_snapshot, render_status_lines, write_status_snapshot, RelayStreamLink,
            RuntimeStatusSnapshot,
        },
        runtime_task::maybe_store_task_artifact,
    },
    protocol::{
        frame::{Frame, MessageType},
        message::{
            Message, StreamCloseMessage, StreamDataMessage, StreamOpenMessage, TaskAction,
            TaskResultMessage,
        },
    },
    serve::{
        raw::{proxy_mux_stream_loop, proxy_ws_mux_stream_loop, RawService},
        service::{build_remote_services, ServiceDefinition},
    },
    tunnel::{
        tcp::bind,
        tcp_mux::{accept_mux_peer, connect_mux_peer},
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
    write_status_snapshot(&root, &hub, &registry, &relay_links)
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
    let shared = RuntimeShared::new(Vec::new(), &data_dir).await;

    let listener_config = AppConfig {
        listens: vec![TunnelEndpoint {
            url: ParsedUrl::parse(&format!("udp://127.0.0.1:{port}")).unwrap(),
        }],
        connects: vec![],
        local_serves: vec![],
        remote_serves: vec![],
        remote_peer_id: None,
        identity: AgentIdentityConfig {
            name: Some("udp-runtime-listener".into()),
            key: None,
        },
        retry: RetryPolicy::default(),
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
        local_serves: vec![],
        remote_serves: vec![],
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
    assert!(sessions.iter().any(|s| s.remote.agent_name == "udp-runtime-dialer"));
    assert!(peers.iter().any(|p| p.session.remote.agent_name == "udp-runtime-dialer"));
}
