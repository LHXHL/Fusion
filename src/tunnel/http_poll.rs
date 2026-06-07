//! HTTP long-poll tunnel schemes (`http://` and legacy `simplex+http://`).

pub fn is_http_poll_scheme(scheme: &str) -> bool {
    matches!(scheme, "http" | "simplex+http")
}

pub fn is_streamhttp_scheme(scheme: &str) -> bool {
    scheme == "streamhttp"
}
