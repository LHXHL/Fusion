use std::{
    fs,
    io::{Error, ErrorKind},
    path::Path,
    sync::Arc,
};

use native_tls::{Certificate, Identity, TlsConnector};
use serde::{Deserialize, Serialize};
#[cfg(target_os = "macos")]
use openssl::{pkcs12::Pkcs12, pkey::PKey, x509::X509};
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
    server::WebPkiClientVerifier,
};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector as TokioTlsConnector};
use tokio_tungstenite::Connector;

use crate::{
    error::{coded_io_error, ErrorCode},
    utils::url::ParsedUrl,
};

pub fn build_ws_tls_acceptor(url: &ParsedUrl) -> Result<Option<TlsAcceptor>, Error> {
    if url.scheme != "wss" {
        return Ok(None);
    }

    let cert_path = query_required(url, "tls-cert")?;
    let key_path = query_required(url, "tls-key")?;
    Ok(Some(build_tls_acceptor_from_pem(url, cert_path, key_path, None)?))
}

pub fn build_optional_tls_acceptor(url: &ParsedUrl) -> Result<Option<TlsAcceptor>, Error> {
    match (
        url.query.get("tls-cert"),
        url.query.get("tls-key"),
    ) {
        (None, None) => Ok(None),
        (Some(cert_path), Some(key_path)) => {
            Ok(Some(build_tls_acceptor_from_pem(url, cert_path, key_path, None)?))
        }
        _ => Err(coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsMissingParameter,
            "tls listener requires both `tls-cert` and `tls-key`",
            false,
            Some("provide both query parameters or remove both".to_string()),
        )),
    }
}

fn build_tls_acceptor_from_pem(
    url: &ParsedUrl,
    cert_path: &str,
    key_path: &str,
    alpn_protocols: Option<Vec<Vec<u8>>>,
) -> Result<TlsAcceptor, Error> {
    let certs = load_certs(Path::new(cert_path))?;
    let key = load_private_key(Path::new(key_path))?;

    let builder = if let Some(client_ca_path) = url.query.get("tls-client-ca") {
        let roots = load_root_store(Path::new(client_ca_path))?;
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|err| {
                coded_io_error(
                    ErrorKind::InvalidInput,
                    ErrorCode::TlsInvalidClientCa,
                    format!("invalid tls client ca {client_ca_path}"),
                    false,
                    Some(err.to_string()),
                )
            })?;
        ServerConfig::builder().with_client_cert_verifier(verifier)
    } else {
        ServerConfig::builder().with_no_client_auth()
    };

    let mut config = builder.with_single_cert(certs, key).map_err(|err| {
        coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsInvalidCertificate,
            "invalid tls cert/key pair",
            false,
            Some(err.to_string()),
        )
    })?;

    if let Some(protocols) = alpn_protocols {
        config.alpn_protocols = protocols;
    }

    Ok(TlsAcceptor::from(Arc::new(config)))
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
            coded_io_error(
                err.kind(),
                ErrorCode::TlsReadFailed,
                format!("failed to read tls ca {ca_path}"),
                false,
                Some(err.to_string()),
            )
        })?;
        let cert = Certificate::from_pem(&pem).map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientCa,
                format!("failed to parse tls ca {ca_path}"),
                false,
                Some(err.to_string()),
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
            return Err(coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsMissingParameter,
                "wss client identity requires both `tls-client-cert` and `tls-client-key`",
                false,
                Some("provide both query parameters or remove both".to_string()),
            ));
        }
        (None, None) => {}
    }

    let connector = builder.build().map_err(|err| {
        coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsBuildConnectorFailed,
            "failed to build tls connector",
            false,
            Some(err.to_string()),
        )
    })?;
    Ok(Some(Connector::NativeTls(connector)))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsUsageSummary {
    pub wss_listen_endpoints: usize,
    pub wss_connect_endpoints: usize,
    pub insecure_enabled: usize,
    pub custom_ca_configured: usize,
    pub client_identity_configured: usize,
    pub listener_mutual_tls_enabled: usize,
}

impl TlsUsageSummary {
    pub fn merge(&mut self, other: Self) {
        self.wss_listen_endpoints += other.wss_listen_endpoints;
        self.wss_connect_endpoints += other.wss_connect_endpoints;
        self.insecure_enabled += other.insecure_enabled;
        self.custom_ca_configured += other.custom_ca_configured;
        self.client_identity_configured += other.client_identity_configured;
        self.listener_mutual_tls_enabled += other.listener_mutual_tls_enabled;
    }
}

pub fn summarize_wss_url(url: &ParsedUrl, role_is_listener: bool) -> TlsUsageSummary {
    if url.scheme != "wss" {
        return TlsUsageSummary::default();
    }

    let mut summary = TlsUsageSummary::default();
    if role_is_listener {
        summary.wss_listen_endpoints = 1;
        if url.query.contains_key("tls-client-ca") {
            summary.listener_mutual_tls_enabled = 1;
        }
    } else {
        summary.wss_connect_endpoints = 1;
        if query_flag(url, "tls-insecure") {
            summary.insecure_enabled = 1;
        }
        if url.query.contains_key("tls-ca") {
            summary.custom_ca_configured = 1;
        }
        if url.query.contains_key("tls-client-cert") && url.query.contains_key("tls-client-key") {
            summary.client_identity_configured = 1;
        }
    }
    summary
}

pub fn summarize_tls_from_urls(listen_urls: &[ParsedUrl], connect_urls: &[ParsedUrl]) -> TlsUsageSummary {
    let mut summary = TlsUsageSummary::default();
    for url in listen_urls {
        summary.merge(summarize_wss_url(url, true));
    }
    for url in connect_urls {
        summary.merge(summarize_wss_url(url, false));
    }
    summary
}

fn query_required<'a>(url: &'a ParsedUrl, key: &str) -> Result<&'a str, Error> {
    url.query.get(key).map(String::as_str).ok_or_else(|| {
        coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsMissingParameter,
            format!("{} requires query parameter `{key}`", url.scheme),
            false,
            Some(format!("missing query parameter `{key}`")),
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
        coded_io_error(
            err.kind(),
            ErrorCode::TlsReadFailed,
            format!("failed to read tls cert {}", path.display()),
            false,
            Some(err.to_string()),
        )
    })?;
    let mut reader = std::io::BufReader::new(pem.as_slice());
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidCertificate,
                format!("failed to parse tls cert {}", path.display()),
                false,
                Some(err.to_string()),
            )
        })
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    let pem = fs::read(path).map_err(|err| {
        coded_io_error(
            err.kind(),
            ErrorCode::TlsReadFailed,
            format!("failed to read tls key {}", path.display()),
            false,
            Some(err.to_string()),
        )
    })?;
    let mut reader = std::io::BufReader::new(pem.as_slice());
    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidPrivateKey,
                format!("failed to parse tls key {}", path.display()),
                false,
                Some(err.to_string()),
            )
        })?
        .ok_or_else(|| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidPrivateKey,
                format!("no private key found in {}", path.display()),
                false,
                None,
            )
        })
}

fn load_root_store(path: &Path) -> Result<RootCertStore, Error> {
    let certs = load_certs(path)?;
    let mut roots = RootCertStore::empty();
    let (_added, _ignored) = roots.add_parsable_certificates(certs);
    if roots.is_empty() {
        return Err(coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsInvalidClientCa,
            format!("no parsable root certificates found in {}", path.display()),
            false,
            None,
        ));
    }
    Ok(roots)
}

fn load_client_identity(cert_path: &Path, key_path: &Path) -> Result<Identity, Error> {
    let cert_pem = fs::read(cert_path).map_err(|err| {
        coded_io_error(
            err.kind(),
            ErrorCode::TlsReadFailed,
            format!("failed to read tls client cert {}", cert_path.display()),
            false,
            Some(err.to_string()),
        )
    })?;
    let key_pem = fs::read(key_path).map_err(|err| {
        coded_io_error(
            err.kind(),
            ErrorCode::TlsReadFailed,
            format!("failed to read tls client key {}", key_path.display()),
            false,
            Some(err.to_string()),
        )
    })?;

    #[cfg(target_os = "macos")]
    {
        const CLIENT_IDENTITY_PASSPHRASE: &str = "fusion-client";
        let mut certs = X509::stack_from_pem(&cert_pem).map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                format!("failed to parse tls client cert {}", cert_path.display()),
                false,
                Some(err.to_string()),
            )
        })?;
        let leaf = certs.drain(..1).next().ok_or_else(|| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                format!("no client certificate found in {}", cert_path.display()),
                false,
                None,
            )
        })?;
        let key = PKey::private_key_from_pem(&key_pem).map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                format!("failed to parse tls client key {}", key_path.display()),
                false,
                Some(err.to_string()),
            )
        })?;
        let mut builder = Pkcs12::builder();
        builder.name("fusion-client");
        builder.pkey(&key);
        builder.cert(&leaf);
        if !certs.is_empty() {
            let mut chain = openssl::stack::Stack::new().map_err(|err| {
                coded_io_error(
                    ErrorKind::InvalidInput,
                    ErrorCode::TlsInvalidClientIdentity,
                    "failed to build client cert chain",
                    false,
                    Some(err.to_string()),
                )
            })?;
            for cert in certs {
                chain.push(cert).map_err(|err| {
                    coded_io_error(
                        ErrorKind::InvalidInput,
                        ErrorCode::TlsInvalidClientIdentity,
                        "failed to extend client cert chain",
                        false,
                        Some(err.to_string()),
                    )
                })?;
            }
            builder.ca(chain);
        }
        let pkcs12 = builder.build2(CLIENT_IDENTITY_PASSPHRASE).map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                "failed to build tls client pkcs12 identity",
                false,
                Some(err.to_string()),
            )
        })?;
        let der = pkcs12.to_der().map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                "failed to encode tls client pkcs12 identity",
                false,
                Some(err.to_string()),
            )
        })?;
        return Identity::from_pkcs12(&der, CLIENT_IDENTITY_PASSPHRASE).map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                "failed to parse tls client identity",
                false,
                Some(err.to_string()),
            )
        });
    }

    #[cfg(not(target_os = "macos"))]
    {
        Identity::from_pkcs8(&cert_pem, &key_pem).map_err(|err| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsInvalidClientIdentity,
                "failed to parse tls client identity",
                false,
                Some(err.to_string()),
            )
        })
    }
}

#[derive(Debug)]
struct H2SkipServerVerifier;

impl ServerCertVerifier for H2SkipServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::ED25519,
        ]
    }
}

pub fn build_h2_tls_acceptor(url: &ParsedUrl) -> Result<Option<TlsAcceptor>, Error> {
    if url.scheme != "h2s" {
        return Ok(None);
    }
    let cert_path = query_required(url, "tls-cert")?;
    let key_path = query_required(url, "tls-key")?;
    Ok(Some(build_tls_acceptor_from_pem(
        url,
        cert_path,
        key_path,
        Some(vec![b"h2".to_vec()]),
    )?))
}

fn load_h2_client_identity(url: &ParsedUrl) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>, Error> {
    match (
        url.query.get("tls-client-cert"),
        url.query.get("tls-client-key"),
    ) {
        (Some(cert_path), Some(key_path)) => Ok(Some((
            load_certs(Path::new(cert_path))?,
            load_private_key(Path::new(key_path))?,
        ))),
        (Some(_), None) | (None, Some(_)) => Err(coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsMissingParameter,
            "h2s client identity requires both `tls-client-cert` and `tls-client-key`",
            false,
            Some("provide both query parameters or remove both".to_string()),
        )),
        (None, None) => Ok(None),
    }
}

pub fn build_h2_tls_connector(url: &ParsedUrl) -> Result<Option<TokioTlsConnector>, Error> {
    if url.scheme != "h2s" {
        return Ok(None);
    }

    let client_identity = load_h2_client_identity(url)?;

    let mut config = if query_flag(url, "tls-insecure") {
        let builder = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(H2SkipServerVerifier));
        match client_identity {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .map_err(|err| {
                    coded_io_error(
                        ErrorKind::InvalidInput,
                        ErrorCode::TlsInvalidClientIdentity,
                        "invalid h2s client identity",
                        false,
                        Some(err.to_string()),
                    )
                })?,
            None => builder.with_no_client_auth(),
        }
    } else {
        let mut roots = RootCertStore::empty();
        let ca_path = url.query.get("tls-ca").ok_or_else(|| {
            coded_io_error(
                ErrorKind::InvalidInput,
                ErrorCode::TlsMissingParameter,
                "h2s connect requires `tls-ca` or `tls-insecure=1`",
                false,
                None,
            )
        })?;
        roots.add_parsable_certificates(load_certs(Path::new(ca_path))?);
        let builder = ClientConfig::builder().with_root_certificates(roots);
        match client_identity {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .map_err(|err| {
                    coded_io_error(
                        ErrorKind::InvalidInput,
                        ErrorCode::TlsInvalidClientIdentity,
                        "invalid h2s client identity",
                        false,
                        Some(err.to_string()),
                    )
                })?,
            None => builder.with_no_client_auth(),
        }
    };
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Some(TokioTlsConnector::from(Arc::new(config))))
}

pub async fn connect_tcp_for_url(parsed: &ParsedUrl) -> Result<TcpStream, Error> {
    let host = parsed.host.as_deref().ok_or_else(|| {
        Error::new(ErrorKind::InvalidInput, "missing host for tunnel connect")
    })?;
    let port = parsed
        .port
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "missing port for tunnel connect"))?;
    TcpStream::connect(format!("{host}:{port}"))
        .await
        .map_err(|err| Error::new(err.kind(), err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use rcgen::generate_simple_self_signed;

    use super::{build_h2_tls_acceptor, build_h2_tls_connector, build_ws_tls_acceptor, build_ws_tls_connector};
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
        assert!(err.to_string().contains("code=tls.missing_parameter"));
    }

    #[test]
    fn wss_connector_requires_complete_client_identity_pair() {
        let url =
            ParsedUrl::parse("wss://localhost:8443/tunnel?tls-client-cert=/tmp/fusion-client.pem")
                .unwrap();
        let err = build_ws_tls_connector(&url).err().unwrap();
        assert!(err.to_string().contains("code=tls.missing_parameter"));
        assert!(err.to_string().contains("tls-client-key"));
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

    #[test]
    fn h2s_acceptor_requires_cert_and_key() {
        let url = ParsedUrl::parse("h2s://127.0.0.1:8443/tunnel").unwrap();
        let err = build_h2_tls_acceptor(&url).err().unwrap();
        assert!(err.to_string().contains("tls-cert"));
    }

    #[test]
    fn h2s_connector_supports_insecure_and_custom_ca() {
        let cert = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_path = temp_file("h2s-ca");
        fs::write(&cert_path, cert.serialize_pem().unwrap()).unwrap();

        let url = ParsedUrl::parse(&format!(
            "h2s://localhost:8443/tunnel?tls-insecure=1&tls-ca={}",
            cert_path.display()
        ))
        .unwrap();

        assert!(build_h2_tls_connector(&url).unwrap().is_some());
        let _ = fs::remove_file(cert_path);
    }

    #[test]
    fn summarize_tls_usage_counts_wss_flags_without_paths() {
        use super::summarize_tls_from_urls;

        let listen = ParsedUrl::parse(
            "wss://0.0.0.0:8443/tunnel?tls-cert=/secret/cert.pem&tls-key=/secret/key.pem&tls-client-ca=/secret/client-ca.pem",
        )
        .unwrap();
        let connect = ParsedUrl::parse(
            "wss://127.0.0.1:8443/tunnel?tls-insecure=1&tls-ca=/secret/ca.pem&tls-client-cert=/secret/client.pem&tls-client-key=/secret/client-key.pem",
        )
        .unwrap();
        let summary = summarize_tls_from_urls(&[listen], &[connect]);
        assert_eq!(summary.wss_listen_endpoints, 1);
        assert_eq!(summary.wss_connect_endpoints, 1);
        assert_eq!(summary.insecure_enabled, 1);
        assert_eq!(summary.custom_ca_configured, 1);
        assert_eq!(summary.client_identity_configured, 1);
        assert_eq!(summary.listener_mutual_tls_enabled, 1);
    }
}
