use std::io::{Error, ErrorKind};

use crate::app::config::TunnelEndpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialTarget {
    Tcp { addr: String },
    Ws { url: String },
    Udp { addr: String },
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
}
