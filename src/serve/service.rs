use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};

use crate::{
    app::config::ServeEndpoint,
    protocol::message::StreamOpenMessage,
    serve::{
        http::{HttpProxyRequest, HttpProxyService},
        portfwd::PortForwardService,
        raw::RawService,
        shadowsocks::{ShadowsocksRequest, ShadowsocksService},
        socks5::{Socks5ConnectRequest, Socks5Service},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceKind {
    LocalSocks5(Socks5Service),
    LocalHttpProxy(HttpProxyService),
    LocalShadowsocks(ShadowsocksService),
    RemoteRaw(RawService),
    RemotePortForward(PortForwardService),
    Unsupported { scheme: String, original: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceDefinition {
    pub original: String,
    pub kind: ServiceKind,
}

impl ServiceDefinition {
    pub fn from_local_endpoint(endpoint: &ServeEndpoint) -> Result<Self, Error> {
        let kind = match endpoint.url.scheme.as_str() {
            "socks5" => ServiceKind::LocalSocks5(Socks5Service::from_url(&endpoint.url)?),
            "http" => ServiceKind::LocalHttpProxy(HttpProxyService::from_url(&endpoint.url)?),
            "ss" => ServiceKind::LocalShadowsocks(ShadowsocksService::from_url(&endpoint.url)?),
            other => ServiceKind::Unsupported {
                scheme: other.to_string(),
                original: endpoint.url.original.clone(),
            },
        };

        Ok(Self {
            original: endpoint.url.original.clone(),
            kind,
        })
    }

    pub fn from_remote_endpoint(endpoint: &ServeEndpoint) -> Result<Self, Error> {
        let kind = match endpoint.url.scheme.as_str() {
            "raw" => ServiceKind::RemoteRaw(RawService::from_url(&endpoint.url)?),
            "port" => ServiceKind::RemotePortForward(PortForwardService::from_url(&endpoint.url)?),
            other => ServiceKind::Unsupported {
                scheme: other.to_string(),
                original: endpoint.url.original.clone(),
            },
        };

        Ok(Self {
            original: endpoint.url.original.clone(),
            kind,
        })
    }

    pub fn summary_line(&self) -> String {
        match &self.kind {
            ServiceKind::LocalSocks5(svc) => {
                format!("service.local=socks5://{}{}", svc.bind_label(), svc.summary_suffix())
            }
            ServiceKind::LocalHttpProxy(svc) => {
                format!("service.local=http://{}{}", svc.bind_label(), svc.summary_suffix())
            }
            ServiceKind::LocalShadowsocks(svc) => {
                format!(
                    "service.local=ss://{}{}",
                    svc.bind_label(),
                    svc.summary_suffix()
                )
            }
            ServiceKind::RemoteRaw(svc) => format!("service.remote=raw://{}", svc.target_label()),
            ServiceKind::RemotePortForward(svc) => {
                format!("service.remote=port://{}", svc.summary_label())
            }
            ServiceKind::Unsupported { scheme, original } => {
                format!(
                    "service.unsupported scheme={} original={}",
                    scheme, original
                )
            }
        }
    }
}

pub fn build_local_services(endpoints: &[ServeEndpoint]) -> Result<Vec<ServiceDefinition>, Error> {
    endpoints
        .iter()
        .map(ServiceDefinition::from_local_endpoint)
        .collect()
}

pub fn build_remote_services(endpoints: &[ServeEndpoint]) -> Result<Vec<ServiceDefinition>, Error> {
    endpoints
        .iter()
        .map(ServiceDefinition::from_remote_endpoint)
        .collect()
}

pub fn validate_service_pairing(
    local: &[ServiceDefinition],
    remote: &[ServiceDefinition],
) -> Result<(), Error> {
    let has_local_stream_proxy = local.iter().any(|s| {
        matches!(
            s.kind,
            ServiceKind::LocalSocks5(_)
                | ServiceKind::LocalHttpProxy(_)
                | ServiceKind::LocalShadowsocks(_)
        )
    });
    let has_supported_remote_egress = remote.iter().any(|s| {
        matches!(
            s.kind,
            ServiceKind::RemoteRaw(_) | ServiceKind::RemotePortForward(_)
        )
    });

    if has_local_stream_proxy && !has_supported_remote_egress {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "local socks5/http/ss service currently requires at least one remote raw/port service",
        ));
    }

    Ok(())
}

pub fn build_remote_stream_open(
    definition: &ServiceDefinition,
) -> Result<StreamOpenMessage, Error> {
    match &definition.kind {
        ServiceKind::RemoteRaw(svc) => Ok(StreamOpenMessage {
            service: "raw".to_string(),
            target_host: svc.host.clone(),
            target_port: svc.port,
        }),
        ServiceKind::RemotePortForward(svc) => Ok(StreamOpenMessage {
            service: "raw".to_string(),
            target_host: Some(svc.target_host.clone()),
            target_port: Some(svc.target_port),
        }),
        ServiceKind::Unsupported { scheme, original } => Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "unsupported remote service scheme={} original={}",
                scheme, original
            ),
        )),
        ServiceKind::LocalSocks5(_)
        | ServiceKind::LocalHttpProxy(_)
        | ServiceKind::LocalShadowsocks(_) => Err(Error::new(
            ErrorKind::InvalidInput,
            "cannot build remote stream open from local proxy service",
        )),
    }
}

pub fn build_remote_stream_open_for_request(
    definition: &ServiceDefinition,
    request: &Socks5ConnectRequest,
) -> Result<StreamOpenMessage, Error> {
    build_remote_stream_open_for_target(definition, &request.target_host(), request.port)
}

pub fn build_remote_stream_open_for_http_request(
    definition: &ServiceDefinition,
    request: &HttpProxyRequest,
) -> Result<StreamOpenMessage, Error> {
    build_remote_stream_open_for_target(definition, &request.target_host, request.target_port)
}

pub fn build_remote_stream_open_for_shadowsocks_request(
    definition: &ServiceDefinition,
    request: &ShadowsocksRequest,
) -> Result<StreamOpenMessage, Error> {
    build_remote_stream_open_for_target(definition, &request.target_host, request.target_port)
}

pub fn build_remote_stream_open_for_target(
    definition: &ServiceDefinition,
    target_host: &str,
    target_port: u16,
) -> Result<StreamOpenMessage, Error> {
    match &definition.kind {
        ServiceKind::RemoteRaw(svc) => {
            let target = svc.resolve_target(Some(target_host), Some(target_port))?;
            Ok(StreamOpenMessage {
                service: "raw".to_string(),
                target_host: Some(target.host),
                target_port: Some(target.port),
            })
        }
        ServiceKind::RemotePortForward(svc) => Ok(StreamOpenMessage {
            service: "raw".to_string(),
            target_host: Some(svc.target_host.clone()),
            target_port: Some(svc.target_port),
        }),
        ServiceKind::Unsupported { scheme, original } => Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "unsupported remote service scheme={} original={}",
                scheme, original
            ),
        )),
        ServiceKind::LocalSocks5(_)
        | ServiceKind::LocalHttpProxy(_)
        | ServiceKind::LocalShadowsocks(_) => Err(Error::new(
            ErrorKind::InvalidInput,
            "cannot build remote stream open from local proxy service",
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        app::config::ServeEndpoint,
        serve::{
            http::HttpProxyRequest,
            portfwd::PortForwardService,
            service::{
                build_local_services, build_remote_services, build_remote_stream_open,
                build_remote_stream_open_for_http_request, build_remote_stream_open_for_request,
                build_remote_stream_open_for_shadowsocks_request, validate_service_pairing,
                ServiceKind,
            },
            shadowsocks::ShadowsocksRequest,
            socks5::{Socks5Address, Socks5ConnectRequest},
        },
        utils::url::ParsedUrl,
    };

    #[test]
    fn build_service_definitions() {
        let local = vec![ServeEndpoint {
            url: ParsedUrl::parse("socks5://127.0.0.1:1080").unwrap(),
        }];
        let remote = vec![ServeEndpoint {
            url: ParsedUrl::parse("raw://example.com:80").unwrap(),
        }];

        let local_defs = build_local_services(&local).unwrap();
        let remote_defs = build_remote_services(&remote).unwrap();

        assert!(matches!(local_defs[0].kind, ServiceKind::LocalSocks5(_)));
        assert!(matches!(remote_defs[0].kind, ServiceKind::RemoteRaw(_)));
        validate_service_pairing(&local_defs, &remote_defs).unwrap();
    }

    #[test]
    fn build_http_proxy_service_definition() {
        let local = vec![ServeEndpoint {
            url: ParsedUrl::parse("http://127.0.0.1:8080").unwrap(),
        }];
        let local_defs = build_local_services(&local).unwrap();
        assert!(matches!(local_defs[0].kind, ServiceKind::LocalHttpProxy(_)));
        assert_eq!(
            local_defs[0].summary_line(),
            "service.local=http://127.0.0.1:8080"
        );
    }

    #[test]
    fn build_shadowsocks_service_definition() {
        let local = vec![ServeEndpoint {
            url: ParsedUrl::parse("ss://127.0.0.1:8388?method=none").unwrap(),
        }];
        let local_defs = build_local_services(&local).unwrap();
        assert!(matches!(
            local_defs[0].kind,
            ServiceKind::LocalShadowsocks(_)
        ));
        assert_eq!(
            local_defs[0].summary_line(),
            "service.local=ss://127.0.0.1:8388?method=none"
        );
    }

    #[test]
    fn build_stream_open_from_remote_raw() {
        let remote = vec![ServeEndpoint {
            url: ParsedUrl::parse("raw://example.com:80").unwrap(),
        }];
        let remote_defs = build_remote_services(&remote).unwrap();
        let open = build_remote_stream_open(&remote_defs[0]).unwrap();
        assert_eq!(open.service, "raw");
        assert_eq!(open.target_host.as_deref(), Some("example.com"));
        assert_eq!(open.target_port, Some(80));
    }

    #[test]
    fn build_stream_open_from_request_for_dynamic_raw() {
        let remote = vec![ServeEndpoint {
            url: ParsedUrl::parse("raw://").unwrap(),
        }];
        let remote_defs = build_remote_services(&remote).unwrap();
        let req = Socks5ConnectRequest {
            address: Socks5Address::Domain("dynamic.example".to_string()),
            port: 8080,
        };
        let open = build_remote_stream_open_for_request(&remote_defs[0], &req).unwrap();
        assert_eq!(open.target_host.as_deref(), Some("dynamic.example"));
        assert_eq!(open.target_port, Some(8080));
    }

    #[test]
    fn build_stream_open_from_http_request_for_dynamic_raw() {
        let remote = vec![ServeEndpoint {
            url: ParsedUrl::parse("raw://").unwrap(),
        }];
        let remote_defs = build_remote_services(&remote).unwrap();
        let req = HttpProxyRequest {
            target_host: "dynamic.example".to_string(),
            target_port: 8081,
            initial_payload: b"GET / HTTP/1.1\r\n\r\n".to_vec(),
            connect_tunnel: false,
            proxy_authorization: None,
        };
        let open = build_remote_stream_open_for_http_request(&remote_defs[0], &req).unwrap();
        assert_eq!(open.target_host.as_deref(), Some("dynamic.example"));
        assert_eq!(open.target_port, Some(8081));
    }

    #[test]
    fn build_stream_open_from_shadowsocks_request_for_dynamic_raw() {
        let remote = vec![ServeEndpoint {
            url: ParsedUrl::parse("raw://").unwrap(),
        }];
        let remote_defs = build_remote_services(&remote).unwrap();
        let req = ShadowsocksRequest {
            target_host: "example.com".to_string(),
            target_port: 443,
            initial_payload: b"ping".to_vec(),
        };
        let open = build_remote_stream_open_for_shadowsocks_request(&remote_defs[0], &req).unwrap();
        assert_eq!(open.target_host.as_deref(), Some("example.com"));
        assert_eq!(open.target_port, Some(443));
    }

    #[test]
    fn build_service_definitions_for_port_forward() {
        let remote = vec![ServeEndpoint {
            url: ParsedUrl::parse("port://127.0.0.1:8080->example.com:80").unwrap(),
        }];

        let remote_defs = build_remote_services(&remote).unwrap();
        assert!(matches!(
            remote_defs[0].kind,
            ServiceKind::RemotePortForward(PortForwardService { .. })
        ));

        let open = build_remote_stream_open(&remote_defs[0]).unwrap();
        assert_eq!(open.target_host.as_deref(), Some("example.com"));
        assert_eq!(open.target_port, Some(80));
    }
}
