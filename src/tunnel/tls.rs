use std::{
    fs,
    io::{Error, ErrorKind},
    path::Path,
    sync::Arc,
};

use native_tls::{Certificate, TlsConnector};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    ServerConfig,
};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::Connector;

use crate::utils::url::ParsedUrl;

pub fn build_ws_tls_acceptor(url: &ParsedUrl) -> Result<Option<TlsAcceptor>, Error> {
    if url.scheme != "wss" {
        return Ok(None);
    }

    let cert_path = query_required(url, "tls-cert")?;
    let key_path = query_required(url, "tls-key")?;
    let certs = load_certs(Path::new(cert_path))?;
    let key = load_private_key(Path::new(key_path))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid tls cert/key: {err}"),
            )
        })?;

    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

pub fn build_ws_tls_connector(url: &ParsedUrl) -> Result<Option<Connector>, Error> {
    if url.scheme != "wss" {
        return Ok(None);
    }

    let mut builder = TlsConnector::builder();
    if query_flag(url, "tls-insecure") {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }

    if let Some(ca_path) = url.query.get("tls-ca") {
        let pem = fs::read(ca_path).map_err(|err| {
            Error::new(
                err.kind(),
                format!("failed to read tls ca {}: {}", ca_path, err),
            )
        })?;
        let cert = Certificate::from_pem(&pem).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to parse tls ca {}: {}", ca_path, err),
            )
        })?;
        builder.add_root_certificate(cert);
    }

    let connector = builder.build().map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("failed to build tls connector: {err}"),
        )
    })?;
    Ok(Some(Connector::NativeTls(connector)))
}

fn query_required<'a>(url: &'a ParsedUrl, key: &str) -> Result<&'a str, Error> {
    url.query.get(key).map(String::as_str).ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("{} requires query parameter `{}`", url.scheme, key),
        )
    })
}

fn query_flag(url: &ParsedUrl, key: &str) -> bool {
    matches!(
        url.query.get(key).map(|v| v.as_str()),
        Some("") | Some("1") | Some("true") | Some("yes") | Some("on")
    )
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let pem = fs::read(path).map_err(|err| {
        Error::new(
            err.kind(),
            format!("failed to read tls cert {}: {}", path.display(), err),
        )
    })?;
    let mut reader = std::io::BufReader::new(pem.as_slice());
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to parse tls cert {}: {}", path.display(), err),
            )
        })
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    let pem = fs::read(path).map_err(|err| {
        Error::new(
            err.kind(),
            format!("failed to read tls key {}: {}", path.display(), err),
        )
    })?;
    let mut reader = std::io::BufReader::new(pem.as_slice());
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to parse tls key {}: {}", path.display(), err),
            )
        })?
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("no private key found in {}", path.display()),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use rcgen::generate_simple_self_signed;

    use super::{build_ws_tls_acceptor, build_ws_tls_connector};
    use crate::utils::url::ParsedUrl;

    fn temp_file(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fusion-{name}-{nanos}.pem"))
    }

    #[test]
    fn wss_acceptor_requires_cert_and_key() {
        let url = ParsedUrl::parse("wss://127.0.0.1:8443/tunnel").unwrap();
        let err = build_ws_tls_acceptor(&url).err().unwrap();
        assert!(err.to_string().contains("tls-cert"));
    }

    #[test]
    fn wss_connector_supports_insecure_and_custom_ca() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = cert.serialize_pem().unwrap();
        let cert_path = temp_file("ca");
        fs::write(&cert_path, cert_pem).unwrap();

        let url = ParsedUrl::parse(&format!(
            "wss://localhost:8443/tunnel?tls-insecure=1&tls-ca={}",
            cert_path.display()
        ))
        .unwrap();

        assert!(build_ws_tls_connector(&url).unwrap().is_some());
        let _ = fs::remove_file(cert_path);
    }
}
