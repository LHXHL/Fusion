use std::io::{Error, ErrorKind};

use serde::{Deserialize, Serialize};
use url::Url;

use crate::utils::url::ParsedUrl;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HttpProxyService {
    pub bind_host: String,
    pub bind_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpProxyRequest {
    pub target_host: String,
    pub target_port: u16,
    pub initial_payload: Vec<u8>,
    pub connect_tunnel: bool,
}

impl HttpProxyService {
    pub fn from_url(url: &ParsedUrl) -> Result<Self, Error> {
        if url.scheme != "http" {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("expected http scheme, got {}", url.scheme),
            ));
        }
        Ok(Self {
            bind_host: url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "http proxy bind host is missing")
            })?,
            bind_port: url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "http proxy bind port is missing")
            })?,
        })
    }

    pub fn bind_label(&self) -> String {
        format!("{}:{}", self.bind_host, self.bind_port)
    }
}

pub fn parse_http_proxy_request(bytes: &[u8]) -> Result<HttpProxyRequest, Error> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|idx| idx + 4)
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing http header terminator"))?;
    let head = std::str::from_utf8(&bytes[..header_end]).map_err(|err| {
        Error::new(
            ErrorKind::InvalidData,
            format!("http proxy request headers are not valid utf-8: {err}"),
        )
    })?;
    let mut lines = head.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing http request line"))?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing http method"))?;
    let target = parts
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing http target"))?;
    let version = parts
        .next()
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing http version"))?;

    if method.eq_ignore_ascii_case("CONNECT") {
        let (target_host, target_port) = split_host_port(target)?;
        return Ok(HttpProxyRequest {
            target_host,
            target_port,
            initial_payload: Vec::new(),
            connect_tunnel: true,
        });
    }

    let absolute = Url::parse(target).map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("http proxy requires absolute-form target url: {err}"),
        )
    })?;
    let target_host = absolute.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "http proxy absolute-form target is missing host",
        )
    })?;
    let target_port = absolute.port_or_known_default().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            "http proxy absolute-form target is missing port",
        )
    })?;

    let mut rewritten = format!("{} {} {}\r\n", method, origin_form_path(&absolute), version);
    for header in lines.take_while(|line| !line.is_empty()) {
        rewritten.push_str(header);
        rewritten.push_str("\r\n");
    }
    rewritten.push_str("\r\n");
    rewritten.extend(std::str::from_utf8(&bytes[header_end..]).map_err(|err| {
        Error::new(
            ErrorKind::InvalidData,
            format!("http proxy request body prefix is not valid utf-8: {err}"),
        )
    })?);

    Ok(HttpProxyRequest {
        target_host: target_host.to_string(),
        target_port,
        initial_payload: rewritten.into_bytes(),
        connect_tunnel: false,
    })
}

fn origin_form_path(url: &Url) -> String {
    let mut path = url.path().to_string();
    if path.is_empty() {
        path.push('/');
    }
    if let Some(query) = url.query() {
        path.push('?');
        path.push_str(query);
    }
    path
}

fn split_host_port(target: &str) -> Result<(String, u16), Error> {
    let (host, port) = target.rsplit_once(':').ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid CONNECT target `{target}`"),
        )
    })?;
    Ok((
        host.to_string(),
        port.parse::<u16>().map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid CONNECT target port in `{target}`: {err}"),
            )
        })?,
    ))
}

#[cfg(test)]
mod tests {
    use super::{parse_http_proxy_request, HttpProxyService};
    use crate::utils::url::ParsedUrl;

    #[test]
    fn parse_http_proxy_service() {
        let service =
            HttpProxyService::from_url(&ParsedUrl::parse("http://127.0.0.1:8080").unwrap())
                .unwrap();
        assert_eq!(service.bind_label(), "127.0.0.1:8080");
    }

    #[test]
    fn parse_connect_request() {
        let request = parse_http_proxy_request(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n",
        )
        .unwrap();
        assert!(request.connect_tunnel);
        assert_eq!(request.target_host, "example.com");
        assert_eq!(request.target_port, 443);
    }

    #[test]
    fn rewrite_absolute_form_request() {
        let request = parse_http_proxy_request(
            b"GET http://example.com:8080/path?q=1 HTTP/1.1\r\nHost: example.com:8080\r\n\r\n",
        )
        .unwrap();
        assert!(!request.connect_tunnel);
        assert_eq!(request.target_host, "example.com");
        assert_eq!(request.target_port, 8080);
        assert!(std::str::from_utf8(&request.initial_payload)
            .unwrap()
            .starts_with("GET /path?q=1 HTTP/1.1\r\n"));
    }
}
