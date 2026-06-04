use std::io::{Error, ErrorKind};

use tokio::net::TcpListener;

use crate::app::config::TunnelEndpoint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerTransport {
    Tcp,
    Ws,
}

pub struct BoundTunnelListener {
    pub transport: ListenerTransport,
    pub listener: TcpListener,
    pub display_url: String,
}

pub async fn bind_endpoint(endpoint: &TunnelEndpoint) -> Result<BoundTunnelListener, Error> {
    let host = endpoint.url.host.as_deref().unwrap_or("0.0.0.0");
    let port = endpoint.url.port.unwrap_or(0);
    let bind_addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&bind_addr).await?;
    let local_addr = listener.local_addr()?;

    match endpoint.url.scheme.as_str() {
        "tcp" => Ok(BoundTunnelListener {
            transport: ListenerTransport::Tcp,
            listener,
            display_url: format!("tcp://{local_addr}"),
        }),
        "ws" | "wss" => Ok(BoundTunnelListener {
            transport: ListenerTransport::Ws,
            listener,
            display_url: format!("{}://{}{}", endpoint.url.scheme, local_addr, endpoint.url.path),
        }),
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("listen scheme `{other}` not implemented yet"),
        )),
    }
}
