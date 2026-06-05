use std::io::{Error, ErrorKind};

use tokio::net::{TcpListener, UdpSocket};
use tokio_rustls::TlsAcceptor;

use crate::app::config::TunnelEndpoint;
use crate::tunnel::tls::build_ws_tls_acceptor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerTransport {
    Tcp,
    Ws,
    Udp,
}

pub enum BoundListenerHandle {
    Tcp(TcpListener),
    Udp(UdpSocket),
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
}
