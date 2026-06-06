use std::io::{Error, ErrorKind};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::utils::url::ParsedUrl;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyEndpoint {
    Socks5 { host: String, port: u16 },
    Http { host: String, port: u16 },
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
        match url.scheme.as_str() {
            "socks5" => Ok(Self::Socks5 { host, port }),
            "http" => Ok(Self::Http { host, port }),
            other => Err(Error::new(
                ErrorKind::InvalidInput,
                format!("unsupported proxy scheme `{other}`"),
            )),
        }
    }

    fn addr(&self) -> String {
        match self {
            Self::Socks5 { host, port } | Self::Http { host, port } => format!("{host}:{port}"),
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
                ProxyEndpoint::Socks5 { host, port } | ProxyEndpoint::Http { host, port } => {
                    (host.as_str(), *port)
                }
            }
        } else {
            (target_host, target_port)
        };
        match proxy {
            ProxyEndpoint::Socks5 { .. } => {
                establish_socks5_tunnel(&mut stream, next_host, next_port).await?
            }
            ProxyEndpoint::Http { .. } => {
                establish_http_connect_tunnel(&mut stream, next_host, next_port).await?
            }
        }
    }
    Ok(stream)
}

async fn establish_http_connect_tunnel(
    stream: &mut TcpStream,
    target_host: &str,
    target_port: u16,
) -> Result<(), Error> {
    let request = format!(
        "CONNECT {target_host}:{target_port} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
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
) -> Result<(), Error> {
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    stream.flush().await?;
    let mut method = [0_u8; 2];
    stream.read_exact(&mut method).await?;
    if method != [0x05, 0x00] {
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
}
