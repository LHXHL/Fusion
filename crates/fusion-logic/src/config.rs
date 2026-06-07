use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::url::ParsedUrl;

pub const LOGIC_API_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConfigValidationSummary {
    pub logic_api_version: u32,
    pub listen_count: usize,
    pub connect_count: usize,
    pub up_connect_count: usize,
    pub down_connect_count: usize,
    pub local_serve_count: usize,
    pub remote_serve_count: usize,
    pub url_errors: Vec<String>,
}

fn strip_connect_prefix(value: &str) -> &str {
    value
        .strip_prefix("up-")
        .or_else(|| value.strip_prefix("down-"))
        .unwrap_or(value)
}

fn read_string_list(table: &toml::Table, keys: &[&str]) -> Vec<String> {
    for key in keys {
        if let Some(item) = table.get(*key) {
            return match item {
                toml::Value::Array(values) => values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .collect(),
                toml::Value::String(value) => vec![value.clone()],
                _ => Vec::new(),
            };
        }
    }
    Vec::new()
}

pub fn validate_config_toml(input: &str) -> Result<String, String> {
    let table: toml::Table = toml::from_str(input).map_err(|err| err.to_string())?;

    let listens = read_string_list(&table, &["listens", "listen"]);
    let connects = read_string_list(&table, &["connects", "connect"]);
    let up_connects = read_string_list(&table, &["up_connects", "up_connect"]);
    let down_connects = read_string_list(&table, &["down_connects", "down_connect"]);
    let local_serves = read_string_list(&table, &["local_serves", "local_serve"]);
    let remote_serves = read_string_list(&table, &["remote_serves", "remote_serve"]);

    let mut url_errors = Vec::new();
    for (label, values) in [
        ("listen", listens.iter()),
        ("connect", connects.iter()),
        ("up_connect", up_connects.iter()),
        ("down_connect", down_connects.iter()),
        ("local_serve", local_serves.iter()),
        ("remote_serve", remote_serves.iter()),
    ] {
        for value in values {
            if let Err(err) = ParsedUrl::parse(strip_connect_prefix(value)) {
                url_errors.push(format!("{label} `{value}`: {err}"));
            }
        }
    }

    let summary = ConfigValidationSummary {
        logic_api_version: LOGIC_API_VERSION,
        listen_count: listens.len(),
        connect_count: connects.len(),
        up_connect_count: up_connects.len(),
        down_connect_count: down_connects.len(),
        local_serve_count: local_serves.len(),
        remote_serve_count: remote_serves.len(),
        url_errors,
    };

    serde_json::to_string(&summary).map_err(|err| err.to_string())
}

pub fn validate_config_toml_or_error_json(input: &str) -> String {
    match validate_config_toml(input) {
        Ok(json) => json,
        Err(message) => json!({
            "logic_api_version": LOGIC_API_VERSION,
            "error": message,
        })
        .to_string(),
    }
}

pub fn filter_status_json(input: &str, scope: &str) -> Result<String, String> {
    let snapshot: Value =
        serde_json::from_str(input).map_err(|err| format!("invalid status json: {err}"))?;

    let filtered = match scope.to_lowercase().as_str() {
        "all" => json!({
            "generated_at_unix": snapshot.get("generated_at_unix"),
            "session_count": snapshot.get("sessions").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
            "peer_count": snapshot.get("peer_count"),
            "route_count": snapshot.get("route_count"),
            "stream_count": snapshot.get("stream_count"),
            "config": snapshot.get("config"),
            "sessions": snapshot.get("sessions"),
            "peers": snapshot.get("peers"),
            "routes": snapshot.get("routes"),
            "streams": snapshot.get("streams"),
            "relay_links": snapshot.get("relay_links"),
            "upstream_pools": snapshot.get("upstream_pools"),
            "recent_errors": snapshot.get("recent_errors"),
        }),
        "peers" => json!({
            "generated_at_unix": snapshot.get("generated_at_unix"),
            "sessions": snapshot.get("sessions"),
            "peers": snapshot.get("peers"),
        }),
        "routes" => json!({
            "generated_at_unix": snapshot.get("generated_at_unix"),
            "routes": snapshot.get("routes"),
        }),
        "streams" => json!({
            "generated_at_unix": snapshot.get("generated_at_unix"),
            "streams": snapshot.get("streams"),
            "relay_links": snapshot.get("relay_links"),
        }),
        other => {
            return Err(format!(
                "unknown status scope `{other}`; expected all|peers|routes|streams"
            ));
        }
    };

    serde_json::to_string(&filtered).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::{filter_status_json, validate_config_toml};

    #[test]
    fn validate_config_accepts_dns_and_h2_urls() {
        let summary = validate_config_toml(
            r#"
listens = ["dns://0.0.0.0:5353/task.local", "h2://0.0.0.0:39200/tunnel"]
connect = ["h2s://127.0.0.1:443/tunnel?tls-insecure=1"]
"#,
        )
        .unwrap();
        assert!(summary.contains("\"listen_count\":2"));
        assert!(summary.contains("\"connect_count\":1"));
        assert!(summary.contains("\"url_errors\":[]"));
    }

    #[test]
    fn validate_config_reports_url_errors() {
        let summary = validate_config_toml(
            r#"
connect = ["tcp://127.0.0.1:9000", "not-a-url"]
local_serve = ["socks5://127.0.0.1:1080"]
"#,
        )
        .unwrap();
        assert!(summary.contains("\"connect_count\":2"));
        assert!(summary.contains("not-a-url"));
    }

    #[test]
    fn filter_status_json_by_peers_scope() {
        let input = r#"{
            "generated_at_unix": 1,
            "peer_count": 2,
            "peers": [{"agent_id":"a"}],
            "routes": [{"destination_agent_id":"b"}]
        }"#;
        let filtered = filter_status_json(input, "peers").unwrap();
        assert!(filtered.contains("\"peers\""));
        assert!(!filtered.contains("\"routes\""));
    }
}
