use std::io::{Error, ErrorKind};

use tokio::net::{TcpListener, UdpSocket, UnixListener};
use tokio_rustls::TlsAcceptor;

use crate::app::config::TunnelEndpoint;
use crate::tunnel::{memory::MemoryListener, tls::build_ws_tls_acceptor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerTransport {
    Tcp,
    Ws,
    Udp,
    SimplexDns,
    SimplexHttp,
    SimplexOss,
    Icmp,
    Wg,
    Unix,
    Memory,
}

pub enum BoundListenerHandle {
    Tcp(TcpListener),
    Udp(UdpSocket),
    Unix(UnixListener),
    Memory(MemoryListener),
    SimplexOss(String),
}

pub struct BoundTunnelListener {
    pub transport: ListenerTransport,
    pub handle: BoundListenerHandle,
    pub display_url: String,
    pub ws_tls_acceptor: Option<TlsAcceptor>,
}

pub async fn bind_endpoint(endpoint: &TunnelEndpoint) -> Result<BoundTunnelListener, Error> {
    let host = endpoint.url.host.as_deref().unwrap_or("0.0.0.0");
    let port = endpoint.url.port.unwrap_or(0);
    let bind_addr = format!("{host}:{port}");

    match endpoint.url.scheme.as_str() {
        "tcp" => {
            let listener = TcpListener::bind(&bind_addr).await?;
            let local_addr = listener.local_addr()?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Tcp,
                handle: BoundListenerHandle::Tcp(listener),
                display_url: format!("tcp://{local_addr}"),
                ws_tls_acceptor: None,
            })
        }
        "ws" | "wss" => {
            let listener = TcpListener::bind(&bind_addr).await?;
            let local_addr = listener.local_addr()?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Ws,
                handle: BoundListenerHandle::Tcp(listener),
                display_url: format!(
                    "{}://{}{}",
                    endpoint.url.scheme, local_addr, endpoint.url.path
                ),
                ws_tls_acceptor: build_ws_tls_acceptor(&endpoint.url)?,
            })
        }
        "udp" => {
            let socket = UdpSocket::bind(&bind_addr).await?;
            let local_addr = socket.local_addr()?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Udp,
                handle: BoundListenerHandle::Udp(socket),
                display_url: format!("udp://{local_addr}"),
                ws_tls_acceptor: None,
            })
        }
        "simplex+dns" => {
            let socket = crate::tunnel::simplex_dns::bind(&endpoint.url.original).await?;
            let local_addr = socket.local_addr()?;
            let path = if endpoint.url.path.is_empty() {
                "/fusion.local".to_string()
            } else {
                endpoint.url.path.clone()
            };
            Ok(BoundTunnelListener {
                transport: ListenerTransport::SimplexDns,
                handle: BoundListenerHandle::Udp(socket),
                display_url: format!("simplex+dns://{local_addr}{path}"),
                ws_tls_acceptor: None,
            })
        }
        "simplex+http" => {
            let listener = TcpListener::bind(&bind_addr).await?;
            let local_addr = listener.local_addr()?;
            let path = if endpoint.url.path.is_empty() {
                "/".to_string()
            } else {
                endpoint.url.path.clone()
            };
            Ok(BoundTunnelListener {
                transport: ListenerTransport::SimplexHttp,
                handle: BoundListenerHandle::Tcp(listener),
                display_url: format!("simplex+http://{local_addr}{path}"),
                ws_tls_acceptor: None,
            })
        }
        "simplex+oss" => {
            crate::tunnel::simplex_oss::bind(&endpoint.url.original).await?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::SimplexOss,
                handle: BoundListenerHandle::SimplexOss(endpoint.url.original.clone()),
                display_url: endpoint.url.original.clone(),
                ws_tls_acceptor: None,
            })
        }
        "icmp" => {
            let socket = UdpSocket::bind(&bind_addr).await?;
            let local_addr = socket.local_addr()?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Icmp,
                handle: BoundListenerHandle::Udp(socket),
                display_url: format!("icmp://{local_addr}"),
                ws_tls_acceptor: None,
            })
        }
        "wg" => {
            let socket = UdpSocket::bind(&bind_addr).await?;
            let local_addr = socket.local_addr()?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Wg,
                handle: BoundListenerHandle::Udp(socket),
                display_url: format!("wg://{local_addr}"),
                ws_tls_acceptor: None,
            })
        }
        "unix" => {
            let path = endpoint.url.path.clone();
            if path.is_empty() || path == "/" {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "missing path for unix listen",
                ));
            }
            let listener = crate::tunnel::unix::bind(&path).await?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Unix,
                handle: BoundListenerHandle::Unix(listener),
                display_url: format!("unix://{}", path),
                ws_tls_acceptor: None,
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
                        "missing listener name for memory listen",
                    )
                })?;
            let listener = MemoryListener::bind(&name)?;
            Ok(BoundTunnelListener {
                transport: ListenerTransport::Memory,
                handle: BoundListenerHandle::Memory(listener),
                display_url: format!("memory://{}", name),
                ws_tls_acceptor: None,
            })
        }
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("listen scheme `{other}` not implemented yet"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use crate::{app::config::TunnelEndpoint, utils::url::ParsedUrl};

    use super::{bind_endpoint, BoundListenerHandle, ListenerTransport};

    #[tokio::test]
    async fn bind_udp_endpoint() {
        let endpoint = TunnelEndpoint {
            url: ParsedUrl::parse("udp://127.0.0.1:0").unwrap(),
        };
        let bound = bind_endpoint(&endpoint).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::Udp);
        assert!(bound.display_url.starts_with("udp://127.0.0.1:"));
        assert!(matches!(bound.handle, BoundListenerHandle::Udp(_)));
    }

    #[tokio::test]
    async fn bind_unix_and_memory_endpoints() {
        let unix = TunnelEndpoint {
            url: ParsedUrl::parse("unix:///tmp/fusion-listener-test.sock").unwrap(),
        };
        let bound = bind_endpoint(&unix).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::Unix);
        assert!(matches!(bound.handle, BoundListenerHandle::Unix(_)));
        let _ = std::fs::remove_file("/tmp/fusion-listener-test.sock");

        let memory = TunnelEndpoint {
            url: ParsedUrl::parse("memory://bind-memory-test").unwrap(),
        };
        let bound = bind_endpoint(&memory).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::Memory);
        assert!(matches!(bound.handle, BoundListenerHandle::Memory(_)));

        let simplex_http = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+http://127.0.0.1:0/tunnel").unwrap(),
        };
        let bound = bind_endpoint(&simplex_http).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::SimplexHttp);
        assert!(matches!(bound.handle, BoundListenerHandle::Tcp(_)));
        assert!(bound.display_url.starts_with("simplex+http://127.0.0.1:"));
        assert!(bound.display_url.ends_with("/tunnel"));

        let simplex_dns = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+dns://127.0.0.1:0/tunnel.local").unwrap(),
        };
        let bound = bind_endpoint(&simplex_dns).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::SimplexDns);
        assert!(matches!(bound.handle, BoundListenerHandle::Udp(_)));
        assert!(bound.display_url.starts_with("simplex+dns://127.0.0.1:"));
        assert!(bound.display_url.ends_with("/tunnel.local"));

        let simplex_oss = TunnelEndpoint {
            url: ParsedUrl::parse("simplex+oss://mesh-a/tunnel").unwrap(),
        };
        let bound = bind_endpoint(&simplex_oss).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::SimplexOss);
        assert!(matches!(bound.handle, BoundListenerHandle::SimplexOss(_)));

        let icmp = TunnelEndpoint {
            url: ParsedUrl::parse("icmp://127.0.0.1:0").unwrap(),
        };
        let bound = bind_endpoint(&icmp).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::Icmp);
        assert!(matches!(bound.handle, BoundListenerHandle::Udp(_)));

        let wg = TunnelEndpoint {
            url: ParsedUrl::parse("wg://127.0.0.1:0").unwrap(),
        };
        let bound = bind_endpoint(&wg).await.unwrap();
        assert_eq!(bound.transport, ListenerTransport::Wg);
        assert!(matches!(bound.handle, BoundListenerHandle::Udp(_)));
    }
}
