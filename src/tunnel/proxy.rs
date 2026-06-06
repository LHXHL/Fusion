use std::io::{Error, ErrorKind};

use data_encoding::BASE64;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::utils::url::ParsedUrl;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyEndpoint {
    Socks5 {
        host: String,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    },
    Http {
        host: String,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    },
}

impl ProxyEndpoint {
    pub fn parse(input: &str) -> Result<Self, Error> {
        let url = ParsedUrl::parse(input)?;
        let host = url
            .host
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "proxy url missing host"))?;
        let port = url
            .port
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "proxy url missing port"))?;
        let username = url.query.get("username").cloned();
        let password = url.query.get("password").cloned();
        if username.is_some() ^ password.is_some() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "proxy auth requires both username and password",
            ));
        }
        match url.scheme.as_str() {
            "socks5" => Ok(Self::Socks5 {
                host,
                port,
                username,
                password,
            }),
            "http" => Ok(Self::Http {
                host,
                port,
                username,
                password,
            }),
            other => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("unsupported proxy scheme `{other}`"),
            )),
        }
    }

    fn addr(&self) -> String {
        match self {
            Self::Socks5 { host, port, .. } | Self::Http { host, port, .. } => {
                format!("{host}:{port}")
            }
        }
    }
}

pub async fn connect_via_proxy_chain(
    target_host: &str,
    target_port: u16,
    chain: &[String],
) -> Result<TcpStream, Error> {
    if chain.is_empty() {
        return TcpStream::connect(format!("{target_host}:{target_port}")).await;
    }

    let proxies = chain
        .iter()
        .map(|value| ProxyEndpoint::parse(value))
        .collect::<Result<Vec<_>, _>>()?;
    let mut stream = TcpStream::connect(proxies[0].addr()).await?;
    for (idx, proxy) in proxies.iter().enumerate() {
        let (next_host, next_port) = if idx + 1 < proxies.len() {
            match &proxies[idx + 1] {
                ProxyEndpoint::Socks5 { host, port, .. }
                | ProxyEndpoint::Http { host, port, .. } => (host.as_str(), *port),
            }
        } else {
            (target_host, target_port)
        };
        match proxy {
            ProxyEndpoint::Socks5 {
                username,
                password,
                ..
            } => {
                establish_socks5_tunnel(
                    &mut stream,
                    next_host,
                    next_port,
                    username.as_deref(),
                    password.as_deref(),
                )
                .await?
            }
            ProxyEndpoint::Http {
                username,
                password,
                ..
            } => {
                establish_http_connect_tunnel(
                    &mut stream,
                    next_host,
                    next_port,
                    username.as_deref(),
                    password.as_deref(),
                )
                .await?
            }
        }
    }
    Ok(stream)
}

async fn establish_http_connect_tunnel(
    stream: &mut TcpStream,
    target_host: &str,
    target_port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), Error> {
    let mut request = format!(
        "CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    if let (Some(username), Some(password)) = (username, password) {
        let token = BASE64.encode(format!("{username}:{password}").as_bytes());
        request = format!(
            "CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\nProxy-Connection: Keep-Alive\r\nProxy-Authorization: Basic {token}\r\n\r\n"
        );
    }
    stream.write_all(request.as_bytes()).await?;
    stream.flush().await?;

    let mut buf = Vec::new();
    loop {
        let mut chunk = [0_u8; 1024];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "http proxy closed during CONNECT",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf);
    if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "http proxy CONNECT rejected: {}",
                head.lines().next().unwrap_or_default()
            ),
        ));
    }
    Ok(())
}

async fn establish_socks5_tunnel(
    stream: &mut TcpStream,
    target_host: &str,
    target_port: u16,
    username: Option<&str>,
    password: Option<&str>,
) -> Result<(), Error> {
    if username.is_some() && password.is_some() {
        stream.write_all(&[0x05, 0x01, 0x02]).await?;
    } else {
        stream.write_all(&[0x05, 0x01, 0x00]).await?;
    }
    stream.flush().await?;
    let mut method = [0_u8; 2];
    stream.read_exact(&mut method).await?;
    if username.is_some() && password.is_some() {
        if method != [0x05, 0x02] {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "socks5 proxy rejected username/password negotiation",
            ));
        }
        let username = username.unwrap_or_default().as_bytes();
        let password = password.unwrap_or_default().as_bytes();
        let mut auth = Vec::with_capacity(3 + username.len() + password.len());
        auth.push(0x01);
        auth.push(username.len() as u8);
        auth.extend_from_slice(username);
        auth.push(password.len() as u8);
        auth.extend_from_slice(password);
        stream.write_all(&auth).await?;
        stream.flush().await?;
        let mut auth_resp = [0_u8; 2];
        stream.read_exact(&mut auth_resp).await?;
        if auth_resp != [0x01, 0x00] {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "socks5 proxy authentication failed",
            ));
        }
    } else if method != [0x05, 0x00] {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            "socks5 proxy rejected no-auth negotiation",
        ));
    }

    let mut request = vec![0x05, 0x01, 0x00];
    if let Ok(ip) = target_host.parse::<std::net::Ipv4Addr>() {
        request.push(0x01);
        request.extend_from_slice(&ip.octets());
    } else if let Ok(ip) = target_host.parse::<std::net::Ipv6Addr>() {
        request.push(0x04);
        request.extend_from_slice(&ip.octets());
    } else {
        request.push(0x03);
        request.push(target_host.len() as u8);
        request.extend_from_slice(target_host.as_bytes());
    }
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await?;
    stream.flush().await?;

    let version = stream.read_u8().await?;
    if version != 0x05 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "invalid socks5 response version",
        ));
    }
    let status = stream.read_u8().await?;
    if status != 0x00 {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!("socks5 proxy CONNECT failed with status {status}"),
        ));
    }
    let _reserved = stream.read_u8().await?;
    let atyp = stream.read_u8().await?;
    match atyp {
        0x01 => {
            let mut skip = [0_u8; 4];
            stream.read_exact(&mut skip).await?;
        }
        0x03 => {
            let len = stream.read_u8().await? as usize;
            let mut skip = vec![0_u8; len];
            stream.read_exact(&mut skip).await?;
        }
        0x04 => {
            let mut skip = [0_u8; 16];
            stream.read_exact(&mut skip).await?;
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid socks5 bind atyp",
            ))
        }
    }
    let _ = stream.read_u16().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use data_encoding::BASE64;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use super::{connect_via_proxy_chain, ProxyEndpoint};

    #[test]
    fn parses_proxy_endpoint() {
        assert!(matches!(
            ProxyEndpoint::parse("socks5://127.0.0.1:1080").unwrap(),
            ProxyEndpoint::Socks5 { .. }
        ));
        assert!(matches!(
            ProxyEndpoint::parse("http://127.0.0.1:8080").unwrap(),
            ProxyEndpoint::Http { .. }
        ));
    }

    #[tokio::test]
    async fn connects_via_http_connect_proxy() {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target.local_addr().unwrap();
        let target_task = tokio::spawn(async move {
            let (mut stream, _) = target.accept().await.unwrap();
            let mut buf = [0_u8; 5];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        let proxy_task = tokio::spawn(async move {
            let (mut inbound, _) = proxy.accept().await.unwrap();
            let mut req = Vec::new();
            loop {
                let mut chunk = [0_u8; 256];
                let n = inbound.read(&mut chunk).await.unwrap();
                req.extend_from_slice(&chunk[..n]);
                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let upstream = TcpStream::connect(target_addr).await.unwrap();
            inbound
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let (mut ri, mut wi) = inbound.into_split();
            let (mut ru, mut wu) = upstream.into_split();
            let a = tokio::spawn(async move {
                tokio::io::copy(&mut ri, &mut wu).await.unwrap();
            });
            let b = tokio::spawn(async move {
                tokio::io::copy(&mut ru, &mut wi).await.unwrap();
            });
            let _ = a.await;
            let _ = b.await;
        });

        let mut stream = connect_via_proxy_chain(
            "127.0.0.1",
            target_addr.port(),
            &[format!("http://{}", proxy_addr)],
        )
        .await
        .unwrap();
        stream.write_all(b"hello").await.unwrap();
        let mut echoed = [0_u8; 5];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"hello");
        drop(stream);
        proxy_task.await.unwrap();
        target_task.await.unwrap();
    }

    #[test]
    fn parses_proxy_endpoint_with_auth() {
        assert!(matches!(
            ProxyEndpoint::parse("socks5://127.0.0.1:1080?username=demo&password=secret")
                .unwrap(),
            ProxyEndpoint::Socks5 {
                username: Some(_),
                password: Some(_),
                ..
            }
        ));
        assert!(matches!(
            ProxyEndpoint::parse("http://127.0.0.1:8080?username=demo&password=secret")
                .unwrap(),
            ProxyEndpoint::Http {
                username: Some(_),
                password: Some(_),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn connects_via_authenticated_http_and_socks5_proxy_chain() {
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target.local_addr().unwrap();
        let target_task = tokio::spawn(async move {
            let (mut stream, _) = target.accept().await.unwrap();
            let mut buf = [0_u8; 7];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        let socks_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socks_proxy_addr = socks_proxy.local_addr().unwrap();
        let socks_proxy_task = tokio::spawn(async move {
            let (mut inbound, _) = socks_proxy.accept().await.unwrap();
            let mut greeting = [0_u8; 3];
            inbound.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting, [0x05, 0x01, 0x02]);
            inbound.write_all(&[0x05, 0x02]).await.unwrap();

            let version = inbound.read_u8().await.unwrap();
            assert_eq!(version, 0x01);
            let ulen = inbound.read_u8().await.unwrap() as usize;
            let mut username = vec![0_u8; ulen];
            inbound.read_exact(&mut username).await.unwrap();
            let plen = inbound.read_u8().await.unwrap() as usize;
            let mut password = vec![0_u8; plen];
            inbound.read_exact(&mut password).await.unwrap();
            assert_eq!(String::from_utf8(username).unwrap(), "demo");
            assert_eq!(String::from_utf8(password).unwrap(), "secret");
            inbound.write_all(&[0x01, 0x00]).await.unwrap();

            let mut header = [0_u8; 4];
            inbound.read_exact(&mut header).await.unwrap();
            assert_eq!(&header[..3], &[0x05, 0x01, 0x00]);
            assert_eq!(header[3], 0x01);
            let mut addr = [0_u8; 4];
            inbound.read_exact(&mut addr).await.unwrap();
            let port = inbound.read_u16().await.unwrap();
            assert_eq!(addr, [127, 0, 0, 1]);
            assert_eq!(port, target_addr.port());
            let upstream = TcpStream::connect(target_addr).await.unwrap();
            inbound
                .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();
            let (mut ri, mut wi) = inbound.into_split();
            let (mut ru, mut wu) = upstream.into_split();
            let a = tokio::spawn(async move { tokio::io::copy(&mut ri, &mut wu).await.unwrap() });
            let b = tokio::spawn(async move { tokio::io::copy(&mut ru, &mut wi).await.unwrap() });
            let _ = a.await;
            let _ = b.await;
        });

        let http_proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_proxy_addr = http_proxy.local_addr().unwrap();
        let http_proxy_task = tokio::spawn(async move {
            let (mut inbound, _) = http_proxy.accept().await.unwrap();
            let mut req = Vec::new();
            loop {
                let mut chunk = [0_u8; 256];
                let n = inbound.read(&mut chunk).await.unwrap();
                req.extend_from_slice(&chunk[..n]);
                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let head = String::from_utf8_lossy(&req);
            let expected = format!("Basic {}", BASE64.encode(b"demo:secret"));
            assert!(head.contains(&format!("Proxy-Authorization: {expected}\r\n")));

            let upstream = TcpStream::connect(socks_proxy_addr).await.unwrap();
            inbound
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let (mut ri, mut wi) = inbound.into_split();
            let (mut ru, mut wu) = upstream.into_split();
            let a = tokio::spawn(async move { tokio::io::copy(&mut ri, &mut wu).await.unwrap() });
            let b = tokio::spawn(async move { tokio::io::copy(&mut ru, &mut wi).await.unwrap() });
            let _ = a.await;
            let _ = b.await;
        });

        let mut stream = connect_via_proxy_chain(
            "127.0.0.1",
            target_addr.port(),
            &[
                format!(
                    "http://{}?username=demo&password=secret",
                    http_proxy_addr
                ),
                format!(
                    "socks5://{}?username=demo&password=secret",
                    socks_proxy_addr
                ),
            ],
        )
        .await
        .unwrap();
        stream.write_all(b"fusion!").await.unwrap();
        let mut echoed = [0_u8; 7];
        stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"fusion!");
        drop(stream);
        http_proxy_task.await.unwrap();
        socks_proxy_task.await.unwrap();
        target_task.await.unwrap();
    }
}
