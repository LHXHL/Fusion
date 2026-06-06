use std::{
    fs,
    io::{Error, ErrorKind},
    path::Path,
    sync::Arc,
};

use native_tls::{Certificate, Identity, TlsConnector};
#[cfg(target_os = "macos")]
use openssl::{pkcs12::Pkcs12, pkey::PKey, x509::X509};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore, ServerConfig,
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

    let builder = if let Some(client_ca_path) = url.query.get("tls-client-ca") {
        let roots = load_root_store(Path::new(client_ca_path))?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid tls client ca {}: {err}", client_ca_path),
                )
            })?;
        ServerConfig::builder().with_client_cert_verifier(verifier)
    } else {
        ServerConfig::builder().with_no_client_auth()
    };

    let config = builder.with_single_cert(certs, key).map_err(|err| {
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

    match (
        url.query.get("tls-client-cert"),
        url.query.get("tls-client-key"),
    ) {
        (Some(cert_path), Some(key_path)) => {
            let identity = load_client_identity(Path::new(cert_path), Path::new(key_path))?;
            builder.identity(identity);
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "wss client identity requires both `tls-client-cert` and `tls-client-key`",
            ));
        }
        (None, None) => {}
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

fn load_root_store(path: &Path) -> Result<RootCertStore, Error> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    let (_added, _ignored) = roots.add_parsable_certificates(certs);
    if roots.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("no parsable root certificates found in {}", path.display()),
        ));
    }
    Ok(roots)
}

fn load_client_identity(cert_path: &Path, key_path: &Path) -> Result<Identity, Error> {
    let cert_pem = fs::read(cert_path).map_err(|err| {
        Error::new(
            err.kind(),
            format!(
                "failed to read tls client cert {}: {}",
                cert_path.display(),
                err
            ),
        )
    })?;
    let key_pem = fs::read(key_path).map_err(|err| {
        Error::new(
            err.kind(),
            format!(
                "failed to read tls client key {}: {}",
                key_path.display(),
                err
            ),
        )
    })?;

    #[cfg(target_os = "macos")]
    {
        const CLIENT_IDENTITY_PASSPHRASE: &str = "fusion-client";
        let mut certs = X509::stack_from_pem(&cert_pem).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "failed to parse tls client cert {}: {}",
                    cert_path.display(),
                    err
                ),
            )
        })?;
        let leaf = certs.drain(..1).next().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("no client certificate found in {}", cert_path.display()),
            )
        })?;
        let key = PKey::private_key_from_pem(&key_pem).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "failed to parse tls client key {}: {}",
                    key_path.display(),
                    err
                ),
            )
        })?;
        let mut builder = Pkcs12::builder();
        builder.name("fusion-client");
        builder.pkey(&key);
        builder.cert(&leaf);
        if !certs.is_empty() {
            let mut chain = openssl::stack::Stack::new().map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    format!("failed to build client cert chain: {err}"),
                )
            })?;
            for cert in certs {
                chain.push(cert).map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("failed to extend client cert chain: {err}"),
                    )
                })?;
            }
            builder.ca(chain);
        }
        let pkcs12 = builder.build2(CLIENT_IDENTITY_PASSPHRASE).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to build tls client pkcs12 identity: {err}"),
            )
        })?;
        let der = pkcs12.to_der().map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to encode tls client pkcs12 identity: {err}"),
            )
        })?;
        return Identity::from_pkcs12(&der, CLIENT_IDENTITY_PASSPHRASE).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to parse tls client identity: {err}"),
            )
        });
    }

    #[cfg(not(target_os = "macos"))]
    {
        Identity::from_pkcs8(&cert_pem, &key_pem).map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("failed to parse tls client identity: {err}"),
            )
        })
    }
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

    #[test]
    fn wss_connector_accepts_client_identity_pair() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = temp_file("client-cert");
        let key_path = temp_file("client-key");
        fs::write(&cert_path, cert.serialize_pem().unwrap()).unwrap();
        fs::write(&key_path, cert.serialize_private_key_pem()).unwrap();

        let url = ParsedUrl::parse(&format!(
            "wss://localhost:8443/tunnel?tls-client-cert={}&tls-client-key={}",
            cert_path.display(),
            key_path.display()
        ))
        .unwrap();
        assert!(build_ws_tls_connector(&url).unwrap().is_some());

        let _ = fs::remove_file(cert_path);
        let _ = fs::remove_file(key_path);
    }
}
