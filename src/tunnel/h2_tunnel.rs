//! HTTP/2 tunnel schemes (`h2://` cleartext, `h2s://` TLS + ALPN h2).

pub fn is_h2_tunnel_scheme(scheme: &str) -> bool {
    matches!(scheme, "h2" | "h2s")
}
