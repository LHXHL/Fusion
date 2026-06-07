use std::io::{Error, ErrorKind};

use crate::app::config::TunnelEndpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialTarget {
    Tcp { addr: String },
    Ws { url: String },
    Udp { addr: String },
    SimplexDns { url: String },
    H2 { url: String },
    SimplexHttp { url: String },
    StreamHttp { url: String },
    SimplexOss { url: String },
    Icmp { addr: String },
    Wg { addr: String },
    Unix { path: String },
    Memory { name: String },
}

pub fn classify_endpoint(endpoint: &TunnelEndpoint) -> Result<DialTarget, Error> {
    match endpoint.url.scheme.as_str() {
        "tcp" => {
            let host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for tcp connect")
            })?;
            let port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for tcp connect")
            })?;
            Ok(DialTarget::Tcp {
                addr: format!("{host}:{port}"),
            })
        }
        "ws" | "wss" => Ok(DialTarget::Ws {
            url: endpoint.url.original.clone(),
        }),
        "udp" => {
            let host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for udp connect")
            })?;
            let port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for udp connect")
            })?;
            Ok(DialTarget::Udp {
                addr: format!("{host}:{port}"),
            })
        }
        scheme if crate::tunnel::dns_tunnel::is_dns_tunnel_scheme(scheme) => {
            Ok(DialTarget::SimplexDns {
                url: endpoint.url.original.clone(),
            })
        }
        scheme if crate::tunnel::h2_tunnel::is_h2_tunnel_scheme(scheme) => Ok(DialTarget::H2 {
            url: endpoint.url.original.clone(),
        }),
        scheme if crate::tunnel::http_poll::is_http_poll_scheme(scheme) => {
            Ok(DialTarget::SimplexHttp {
                url: endpoint.url.original.clone(),
            })
        }
        "streamhttp" => Ok(DialTarget::StreamHttp {
            url: endpoint.url.original.clone(),
        }),
        "simplex+oss" => Ok(DialTarget::SimplexOss {
            url: endpoint.url.original.clone(),
        }),
        "icmp" => {
            let host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for icmp connect")
            })?;
            let port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for icmp connect")
            })?;
            Ok(DialTarget::Icmp {
                addr: format!("{host}:{port}"),
            })
        }
        "wg" => {
            let host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for wg connect")
            })?;
            let port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for wg connect")
            })?;
            Ok(DialTarget::Wg {
                addr: format!("{host}:{port}"),
            })
        }
        "unix" => {
            if endpoint.url.path.is_empty() || endpoint.url.path == "/" {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "missing path for unix connect",
                ));
            }
            Ok(DialTarget::Unix {
                path: endpoint.url.path.clone(),
            })
        }
        "memory" => {
            let name = endpoint
                .url
                .host
                .clone()
                .or_else(|| endpoint.url.query.get("name").cloned())
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        "missing listener name for memory connect",
                    )
                })?;
            Ok(DialTarget::Memory { name })
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("connect scheme `{other}` not implemented yet"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::{app::config::TunnelEndpoint, utils::url::ParsedUrl};

    use super::{classify_endpoint, DialTarget};

    #[test]
    fn classify_udp_endpoint() {
        let endpoint = TunnelEndpoint {
            url: ParsedUrl::parse("udp://127.0.0.1:9001").unwrap(),
        };
        assert_eq!(
            classify_endpoint(&endpoint).unwrap(),
            DialTarget::Udp {
                addr: "127.0.0.1:9001".to_string()
            }
        );
    }

    #[test]
    fn classify_unix_and_memory_endpoints() {
        let unix = TunnelEndpoint {
            url: ParsedUrl::parse("unix:///tmp/fusion-test.sock").unwrap(),
        };
        assert_eq!(
            classify_endpoint(&unix).unwrap(),
            DialTarget::Unix {
                path: "/tmp/fusion-test.sock".to_string()
            }
        );

        let memory = TunnelEndpoint {
            url: ParsedUrl::parse("memory://mesh-a").unwrap(),
        };
        let simplex_dns = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+dns://127.0.0.1:5353/tunnel.local").unwrap(),
        };
        let simplex_http = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+http://127.0.0.1:7777/tunnel").unwrap(),
        };
        let simplex_oss = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+oss://mesh-a/tunnel").unwrap(),
        };
        let icmp = TunnelEndpoint {
            url: ParsedUrl::parse("icmp://127.0.0.1:4444").unwrap(),
        };
        let wg = TunnelEndpoint {
            url: ParsedUrl::parse("wg://127.0.0.1:5555").unwrap(),
        };
        assert_eq!(
            classify_endpoint(&memory).unwrap(),
            DialTarget::Memory {
                name: "mesh-a".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&simplex_dns).unwrap(),
            DialTarget::SimplexDns {
                url: "simplex+dns://127.0.0.1:5353/tunnel.local".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&simplex_http).unwrap(),
            DialTarget::SimplexHttp {
                url: "simplex+http://127.0.0.1:7777/tunnel".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&simplex_oss).unwrap(),
            DialTarget::SimplexOss {
                url: "simplex+oss://mesh-a/tunnel".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&icmp).unwrap(),
            DialTarget::Icmp {
                addr: "127.0.0.1:4444".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&wg).unwrap(),
            DialTarget::Wg {
                addr: "127.0.0.1:5555".to_string()
            }
        );
    }

    #[test]
    fn classify_dns_and_h2_endpoints() {
        let dns = TunnelEndpoint {
            url: ParsedUrl::parse("dns://127.0.0.1:5353/task.local").unwrap(),
        };
        let h2 = TunnelEndpoint {
            url: ParsedUrl::parse("h2://127.0.0.1:39200/tunnel").unwrap(),
        };
        assert_eq!(
            classify_endpoint(&dns).unwrap(),
            DialTarget::SimplexDns {
                url: "dns://127.0.0.1:5353/task.local".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&h2).unwrap(),
            DialTarget::H2 {
                url: "h2://127.0.0.1:39200/tunnel".to_string()
            }
        );
    }

    #[test]
    fn classify_http_long_poll_and_streamhttp_endpoints() {
        let http = TunnelEndpoint {
            url: ParsedUrl::parse("http://127.0.0.1:39090/task").unwrap(),
        };
        let streamhttp = TunnelEndpoint {
            url: ParsedUrl::parse("streamhttp://127.0.0.1:39100/events").unwrap(),
        };
        assert_eq!(
            classify_endpoint(&http).unwrap(),
            DialTarget::SimplexHttp {
                url: "http://127.0.0.1:39090/task".to_string()
            }
        );
        assert_eq!(
            classify_endpoint(&streamhttp).unwrap(),
            DialTarget::StreamHttp {
                url: "streamhttp://127.0.0.1:39100/events".to_string()
            }
        );
    }
}
