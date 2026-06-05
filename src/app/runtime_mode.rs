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
    DirectUdp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundRuntimeMode {
    Task,
    Socks5Tcp,
    Socks5Ws,
    HttpProxyTcp,
    HttpProxyWs,
    RelayTcp,
    RelayWs,
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
        (ListenerTransport::Udp, _, _) => InboundRuntimeMode::DirectUdp,
    }
}

pub fn decide_outbound_runtime_mode(
    endpoint: &TunnelEndpoint,
    task_request: Option<&TaskRequestConfig>,
    has_local_socks: bool,
    has_local_http_proxy: bool,
    has_remote_egress: bool,
    has_listener: bool,
) -> OutboundRuntimeMode {
    if task_request.is_some() {
        return OutboundRuntimeMode::Task;
    }

    let is_tcp = endpoint.url.scheme == "tcp";
    let is_ws = matches!(endpoint.url.scheme.as_str(), "ws" | "wss");

    if is_tcp && has_local_socks && has_remote_egress {
        return OutboundRuntimeMode::Socks5Tcp;
    }
    if is_ws && has_local_socks && has_remote_egress {
        return OutboundRuntimeMode::Socks5Ws;
    }
    if is_tcp && has_local_http_proxy && has_remote_egress {
        return OutboundRuntimeMode::HttpProxyTcp;
    }
    if is_ws && has_local_http_proxy && has_remote_egress {
        return OutboundRuntimeMode::HttpProxyWs;
    }
    if is_tcp && has_listener {
        return OutboundRuntimeMode::RelayTcp;
    }
    if is_ws && has_listener {
        return OutboundRuntimeMode::RelayWs;
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
            decide_inbound_runtime_mode(crate::tunnel::listener::ListenerTransport::Udp, false, false),
            InboundRuntimeMode::DirectUdp
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
                false
            ),
            OutboundRuntimeMode::Task
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, true, false, true, false),
            OutboundRuntimeMode::Socks5Tcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&ws, None, true, false, true, false),
            OutboundRuntimeMode::Socks5Ws
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, false, true, true, false),
            OutboundRuntimeMode::HttpProxyTcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&ws, None, false, true, true, false),
            OutboundRuntimeMode::HttpProxyWs
        );
        assert_eq!(
            decide_outbound_runtime_mode(&tcp, None, false, false, false, true),
            OutboundRuntimeMode::RelayTcp
        );
        assert_eq!(
            decide_outbound_runtime_mode(&udp, None, false, false, false, true),
            OutboundRuntimeMode::Direct
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
