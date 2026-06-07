use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedUrl {
    pub original: String,
    pub scheme: String,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub path: String,
    pub query: BTreeMap<String, String>,
}

impl ParsedUrl {
    pub fn parse(input: &str) -> Result<Self, Error> {
        if let Some(port_url) = parse_port_forward_url(input)? {
            return Ok(port_url);
        }

        let url = Url::parse(input).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid url `{input}`: {e}"),
            )
        })?;

        let query = url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>();

        Ok(Self {
            original: input.to_string(),
            scheme: url.scheme().to_string(),
            host: url.host_str().map(ToString::to_string),
            port: url.port_or_known_default(),
            path: url.path().to_string(),
            query,
        })
    }

    pub fn is_secure(&self) -> bool {
        matches!(self.scheme.as_str(), "https" | "wss") || self.query.contains_key("tls")
    }
}

pub fn parse_url_json(input: &str) -> Result<String, String> {
    let parsed = ParsedUrl::parse(input).map_err(|err| err.to_string())?;
    serde_json::to_string(&parsed).map_err(|err| err.to_string())
}

fn parse_port_forward_url(input: &str) -> Result<Option<ParsedUrl>, Error> {
    let Some(spec) = input.strip_prefix("port://") else {
        return Ok(None);
    };

    let (listen, target) = spec.split_once("->").ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid port forward url `{input}`: missing `->`"),
        )
    })?;
    let (listen_host, listen_port) = split_host_port(listen, "listen side")?;
    let (target_host, target_port) = split_host_port(target, "target side")?;

    let mut query = BTreeMap::new();
    query.insert("target_host".to_string(), target_host);
    query.insert("target_port".to_string(), target_port.to_string());

    Ok(Some(ParsedUrl {
        original: input.to_string(),
        scheme: "port".to_string(),
        host: Some(listen_host),
        port: Some(listen_port),
        path: "/".to_string(),
        query,
    }))
}

fn split_host_port(spec: &str, side: &str) -> Result<(String, u16), Error> {
    let (host, port) = spec.rsplit_once(':').ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid {side} in port forward url: `{spec}`"),
        )
    })?;
    let port = port.parse::<u16>().map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid {side} port in `{spec}`: {err}"),
        )
    })?;
    if host.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("invalid {side} host in `{spec}`"),
        ));
    }
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::ParsedUrl;

    #[test]
    fn parse_tcp_url() {
        let parsed = ParsedUrl::parse("tcp://127.0.0.1:34996").unwrap();
        assert_eq!(parsed.scheme, "tcp");
        assert_eq!(parsed.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(parsed.port, Some(34996));
    }

    #[test]
    fn parse_ws_url_with_query() {
        let parsed = ParsedUrl::parse("wss://example.com:8443/tunnel?retry=10&tls=1").unwrap();
        assert_eq!(parsed.scheme, "wss");
        assert_eq!(parsed.host.as_deref(), Some("example.com"));
        assert_eq!(parsed.path, "/tunnel");
        assert_eq!(parsed.query.get("retry").map(String::as_str), Some("10"));
        assert!(parsed.is_secure());
    }

    #[test]
    fn parse_port_forward_url() {
        let parsed = ParsedUrl::parse("port://127.0.0.1:8080->example.com:80").unwrap();
        assert_eq!(parsed.scheme, "port");
        assert_eq!(parsed.port, Some(8080));
    }

    #[test]
    fn parse_dns_tunnel_url() {
        let parsed = ParsedUrl::parse("dns://127.0.0.1:5353/task.local").unwrap();
        assert_eq!(parsed.scheme, "dns");
        assert_eq!(parsed.host.as_deref(), Some("127.0.0.1"));
        assert_eq!(parsed.port, Some(5353));
        assert_eq!(parsed.path, "/task.local");
    }

    #[test]
    fn parse_h2_tunnel_urls() {
        let cleartext = ParsedUrl::parse("h2://127.0.0.1:39200/tunnel").unwrap();
        assert_eq!(cleartext.scheme, "h2");
        assert_eq!(cleartext.path, "/tunnel");

        let tls = ParsedUrl::parse(
            "h2s://example.com:443/tunnel?tls-insecure=1&tls-cert=/tmp/cert.pem",
        )
        .unwrap();
        assert_eq!(tls.scheme, "h2s");
        assert_eq!(tls.host.as_deref(), Some("example.com"));
        assert_eq!(tls.port, Some(443));
        assert!(tls.query.contains_key("tls-insecure"));
    }
}
