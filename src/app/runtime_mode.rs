use crate::{
    app::config::{TaskRequestConfig, TunnelEndpoint},
    serve::{
        portfwd::PortForwardService,
        service::{ServiceDefinition, ServiceKind},
    },
    tunnel::listener::ListenerTransport,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundRuntimeMode {
    RawTcp,
    TaskTcp,
    RawWs,
    TaskWs,
    RawH2,
    TaskH2,
    RawSimplexDns,
    TaskSimplexDns,
    RawSimplexHttp,
    TaskSimplexHttp,
    TaskStreamHttp,
    RawSimplexOss,
    TaskSimplexOss,
    DirectUdp,
    DirectSimplexHttp,
    DirectSimplexOss,
    DirectIcmp,
    DirectWg,
    DirectUnix,
    DirectMemory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRuntimeMode {
    Task,
    Socks5Tcp,
    Socks5Ws,
    Socks5SimplexHttp,
    PortForwardSimplexHttp,
    HttpProxyTcp,
    HttpProxyWs,
    ShadowsocksTcp,
    ShadowsocksWs,
    TrojanTcp,
    TrojanWs,
    RelayTcp,
    RelayWs,
    RelayH2,
    RelaySimplexDns,
    RelaySimplexHttp,
    RelaySimplexOss,
    Direct,
}

pub fn collect_remote_port_forward_services(
    remote_services: &[ServiceDefinition],
) -> Vec<PortForwardService> {
    remote_services
        .iter()
        .filter_map(|svc| match &svc.kind {
            ServiceKind::RemotePortForward(service) => Some(service.clone()),
            _ => None,
        })
        .collect()
}

pub fn has_remote_port_forward_service(remote_services: &[ServiceDefinition]) -> bool {
    remote_services
        .iter()
        .any(|svc| matches!(svc.kind, ServiceKind::RemotePortForward(_)))
}

/// Port-forward services that bind locally and connect directly to the target.
/// When a simplex+http connect is configured, the same port URL is tunneled
/// over mux instead (see [`OutboundRuntimeMode::PortForwardSimplexHttp`]).
pub fn direct_port_forward_services(
    connects: &[TunnelEndpoint],
    services: &[PortForwardService],
) -> Vec<PortForwardService> {
    if services.is_empty() {
        return Vec::new();
    }
    let tunnel_via_simplex = connects.iter().any(|endpoint| {
        crate::tunnel::http_poll::is_http_poll_scheme(&endpoint.url.scheme)
    });
    if tunnel_via_simplex {
        Vec::new()
    } else {
        services.to_vec()
    }
}

pub fn decide_inbound_runtime_mode(
    transport: ListenerTransport,
    has_inbound_raw_service: bool,
    has_local_services: bool,
) -> InboundRuntimeMode {
    match (transport, has_inbound_raw_service, has_local_services) {
        (ListenerTransport::Tcp, true, false) => InboundRuntimeMode::RawTcp,
        (ListenerTransport::Tcp, _, _) => InboundRuntimeMode::TaskTcp,
        (ListenerTransport::Ws, true, false) => InboundRuntimeMode::RawWs,
        (ListenerTransport::Ws, _, _) => InboundRuntimeMode::TaskWs,
        (ListenerTransport::H2, true, false) => InboundRuntimeMode::RawH2,
        (ListenerTransport::H2, _, _) => InboundRuntimeMode::TaskH2,
        (ListenerTransport::SimplexDns, true, false) => InboundRuntimeMode::RawSimplexDns,
        (ListenerTransport::SimplexDns, _, _) => InboundRuntimeMode::TaskSimplexDns,
        (ListenerTransport::SimplexHttp, true, false) => InboundRuntimeMode::RawSimplexHttp,
        (ListenerTransport::SimplexHttp, _, _) => InboundRuntimeMode::TaskSimplexHttp,
        (ListenerTransport::StreamHttp, _, _) => InboundRuntimeMode::TaskStreamHttp,
        (ListenerTransport::SimplexOss, true, false) => InboundRuntimeMode::RawSimplexOss,
        (ListenerTransport::SimplexOss, _, true) => InboundRuntimeMode::TaskSimplexOss,
        (ListenerTransport::SimplexOss, _, false) => InboundRuntimeMode::DirectSimplexOss,
        (ListenerTransport::Udp, _, _) => InboundRuntimeMode::DirectUdp,
        (ListenerTransport::Icmp, _, _) => InboundRuntimeMode::DirectIcmp,
        (ListenerTransport::Wg, _, _) => InboundRuntimeMode::DirectWg,
        (ListenerTransport::Unix, _, _) => InboundRuntimeMode::DirectUnix,
        (ListenerTransport::Memory, _, _) => InboundRuntimeMode::DirectMemory,
    }
}

pub fn decide_outbound_runtime_mode(
    endpoint: &TunnelEndpoint,
    task_request: Option<&TaskRequestConfig>,
    has_local_socks: bool,
    has_local_http_proxy: bool,
    has_local_shadowsocks: bool,
    has_local_trojan: bool,
    has_remote_egress: bool,
    has_remote_port_forward: bool,
    has_listener: bool,
) -> OutboundRuntimeMode {
    if task_request.is_some() {
        return OutboundRuntimeMode::Task;
    }

    let is_tcp = endpoint.url.scheme == "tcp";
    let is_ws = matches!(endpoint.url.scheme.as_str(), "ws" | "wss");
    let is_h2 = crate::tunnel::h2_tunnel::is_h2_tunnel_scheme(&endpoint.url.scheme);
    let is_simplex_dns = crate::tunnel::dns_tunnel::is_dns_tunnel_scheme(&endpoint.url.scheme);
    let is_simplex_http = crate::tunnel::http_poll::is_http_poll_scheme(&endpoint.url.scheme);
    let is_simplex_oss = endpoint.url.scheme == "simplex+oss";

    if is_tcp && has_local_socks && has_remote_egress {
        return OutboundRuntimeMode::Socks5Tcp;
    }
    if is_ws && has_local_socks && has_remote_egress {
        return OutboundRuntimeMode::Socks5Ws;
    }
    if is_simplex_http && has_local_socks && has_remote_egress {
        return OutboundRuntimeMode::Socks5SimplexHttp;
    }
    if is_simplex_http
        && has_remote_port_forward
        && !has_local_socks
        && !has_local_http_proxy
        && !has_local_shadowsocks
        && !has_listener
    {
        return OutboundRuntimeMode::PortForwardSimplexHttp;
    }
    if is_tcp && has_local_http_proxy && has_remote_egress {
        return OutboundRuntimeMode::HttpProxyTcp;
    }
    if is_ws && has_local_http_proxy && has_remote_egress {
        return OutboundRuntimeMode::HttpProxyWs;
    }
    if is_tcp && has_local_shadowsocks && has_remote_egress {
        return OutboundRuntimeMode::ShadowsocksTcp;
    }
    if is_ws && has_local_shadowsocks && has_remote_egress {
        return OutboundRuntimeMode::ShadowsocksWs;
    }
    if is_tcp && has_local_trojan && has_remote_egress {
        return OutboundRuntimeMode::TrojanTcp;
    }
    if is_ws && has_local_trojan && has_remote_egress {
        return OutboundRuntimeMode::TrojanWs;
    }
    if is_tcp && has_listener {
        return OutboundRuntimeMode::RelayTcp;
    }
    if is_ws && has_listener {
        return OutboundRuntimeMode::RelayWs;
    }
    if is_h2 && has_listener {
        return OutboundRuntimeMode::RelayH2;
    }
    if is_simplex_dns && has_listener {
        return OutboundRuntimeMode::RelaySimplexDns;
    }
    if is_simplex_http && has_listener {
        return OutboundRuntimeMode::RelaySimplexHttp;
    }
    if is_simplex_oss && has_listener {
        return OutboundRuntimeMode::RelaySimplexOss;
    }

    OutboundRuntimeMode::Direct
}

#[cfg(test)]
mod tests {
    use crate::{
        app::config::{ServeEndpoint, TunnelEndpoint},
        serve::{
            portfwd::PortForwardService,
            service::{build_remote_services, ServiceKind},
        },
        utils::url::ParsedUrl,
    };

    use super::{
        collect_remote_port_forward_services, decide_inbound_runtime_mode,
        decide_outbound_runtime_mode, InboundRuntimeMode, OutboundRuntimeMode,
    };

    #[test]
    fn inbound_mode_prefers_raw_only_when_no_local_services() {
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::Tcp,
                true,
                false
            ),
            InboundRuntimeMode::RawTcp
        );
        assert_eq!(
            decide_inbound_runtime_mode(crate::tunnel::listener::ListenerTransport::Ws, true, true),
            InboundRuntimeMode::TaskWs
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::Udp,
                false,
                false
            ),
            InboundRuntimeMode::DirectUdp
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexDns,
                false,
                false
            ),
            InboundRuntimeMode::TaskSimplexDns
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexDns,
                true,
                false
            ),
            InboundRuntimeMode::RawSimplexDns
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexHttp,
                false,
                false
            ),
            InboundRuntimeMode::TaskSimplexHttp
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexHttp,
                true,
                false
            ),
            InboundRuntimeMode::RawSimplexHttp
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexOss,
                true,
                false
            ),
            InboundRuntimeMode::RawSimplexOss
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexOss,
                false,
                true
            ),
            InboundRuntimeMode::TaskSimplexOss
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::SimplexOss,
                true,
                false
            ),
            InboundRuntimeMode::RawSimplexOss
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::Icmp,
                false,
                false
            ),
            InboundRuntimeMode::DirectIcmp
        );
        assert_eq!(
            decide_inbound_runtime_mode(
                crate::tunnel::listener::ListenerTransport::Wg,
                false,
                false
            ),
            InboundRuntimeMode::DirectWg
        );
    }

    #[test]
    fn outbound_mode_covers_task_socks_http_relay_and_direct() {
        let tcp = TunnelEndpoint {
            url: ParsedUrl::parse("tcp://127.0.0.1:1").unwrap(),
        };
        let ws = TunnelEndpoint {
            url: ParsedUrl::parse("ws://127.0.0.1:2/tunnel").unwrap(),
        };
        let udp = TunnelEndpoint {
            url: ParsedUrl::parse("udp://127.0.0.1:3").unwrap(),
        };
        let simplex = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+http://127.0.0.1:4/tunnel").unwrap(),
        };
        let simplex_dns = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+dns://127.0.0.1:53/tunnel.local").unwrap(),
        };
        let simplex_oss = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+oss://mesh-a/tunnel").unwrap(),
        };
        let memory = TunnelEndpoint {
            url: ParsedUrl::parse("memory://mesh-a").unwrap(),
        };
        assert_eq!(
            decide_outbound_runtime_mode(
                &tcp,
                Some(&crate::app::config::TaskRequestConfig {
                    action: crate::protocol::message::TaskAction::Shell,
                    args: vec![],
                    data_hex: None,
                    save_path: None,
                    target_agent_id: None,
                }),
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ),
            OutboundRuntimeMode::Task
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, true, false, false, false, true, false, false),
            OutboundRuntimeMode::Socks5Tcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&ws, None, true, false, false, false, true, false, false),
            OutboundRuntimeMode::Socks5Ws
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &simplex,
                None,
                true,
                false,
                false,
                false,
                true,
                false,
                false
            ),
            OutboundRuntimeMode::Socks5SimplexHttp
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &simplex,
                None,
                false,
                false,
                false,
                false,
                true,
                true,
                false
            ),
            OutboundRuntimeMode::PortForwardSimplexHttp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, false, true, false, false, true, false, false),
            OutboundRuntimeMode::HttpProxyTcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&ws, None, false, true, false, false, true, false, false),
            OutboundRuntimeMode::HttpProxyWs
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, false, false, true, false, true, false, false),
            OutboundRuntimeMode::ShadowsocksTcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&ws, None, false, false, true, false, true, false, false),
            OutboundRuntimeMode::ShadowsocksWs
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, false, false, false, true, true, false, false),
            OutboundRuntimeMode::TrojanTcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, false, false, false, false, false, false, true),
            OutboundRuntimeMode::RelayTcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &simplex_dns,
                None,
                false,
                false,
                false,
                false,
                false,
                false,
                true
            ),
            OutboundRuntimeMode::RelaySimplexDns
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &simplex,
                None,
                false,
                false,
                false,
                false,
                false,
                false,
                true
            ),
            OutboundRuntimeMode::RelaySimplexHttp
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &simplex_oss,
                None,
                false,
                false,
                false,
                false,
                false,
                false,
                true
            ),
            OutboundRuntimeMode::RelaySimplexOss
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &udp,
                None,
                false,
                false,
                false,
                false,
                false,
                false,
                true
            ),
            OutboundRuntimeMode::Direct
        );
        assert_eq!(
            decide_outbound_runtime_mode(
                &memory,
                None,
                false,
                false,
                false,
                false,
                false,
                false,
                false
            ),
            OutboundRuntimeMode::Direct
        );
    }

    #[test]
    fn direct_port_forward_skipped_when_simplex_connect_configured() {
        use super::direct_port_forward_services;

        let service = PortForwardService {
            listen_host: "127.0.0.1".into(),
            listen_port: 9000,
            target_host: "example.com".into(),
            target_port: 80,
        };
        let simplex = vec![TunnelEndpoint {
            url: ParsedUrl::parse("simplex+http://127.0.0.1:8080/tunnel").unwrap(),
        }];
        assert!(direct_port_forward_services(&simplex, &[service.clone()]).is_empty());
        assert_eq!(
            direct_port_forward_services(&[], &[service]).len(),
            1
        );
    }

    #[test]
    fn collect_port_forward_services_filters_only_port_kind() {
        let remote = vec![
            ServeEndpoint {
                url: ParsedUrl::parse("port://127.0.0.1:9000->example.com:80").unwrap(),
            },
            ServeEndpoint {
                url: ParsedUrl::parse("raw://example.com:443").unwrap(),
            },
        ];
        let defs = build_remote_services(&remote).unwrap();
        let services = collect_remote_port_forward_services(&defs);
        assert_eq!(services.len(), 1);
        assert!(matches!(
            defs[0].kind,
            ServiceKind::RemotePortForward(PortForwardService { .. })
        ));
    }
}
