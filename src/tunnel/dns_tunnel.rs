//! DNS query tunnel schemes (`dns://` and legacy `simplex+dns://`).

pub fn is_dns_tunnel_scheme(scheme: &str) -> bool {
    matches!(scheme, "dns" | "simplex+dns")
}
