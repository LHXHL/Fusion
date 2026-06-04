use std::io::{Error, ErrorKind};

use crate::app::config::TunnelEndpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialTarget {
    Tcp { addr: String },
    Ws { url: String },
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
        other => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("connect scheme `{other}` not implemented yet"),
        )),
    }
}
