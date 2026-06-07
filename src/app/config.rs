use std::{
    io::{Error, ErrorKind},
    path::PathBuf,
};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::{crypto::wrapper::WrapperConfig, protocol::message::TaskAction, utils::url::ParsedUrl};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
pub enum ConnPolicy {
    Fallback,
    Random,
    RoundRobin,
}

impl Default for ConnPolicy {
    fn default() -> Self {
        Self::Fallback
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: Option<u32>,
    pub interval_secs: u64,
    pub max_interval_secs: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: None,
            interval_secs: 10,
            max_interval_secs: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentIdentityConfig {
    pub name: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequestConfig {
    pub action: TaskAction,
    pub args: Vec<String>,
    pub data_hex: Option<String>,
    pub save_path: Option<PathBuf>,
    pub local_path: Option<PathBuf>,
    pub target_agent_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StatusScope {
    All,
    Peers,
    Routes,
    Streams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusCommandConfig {
    pub scope: StatusScope,
    pub json: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileRetryPolicy {
    pub max_retries: Option<u32>,
    pub interval_secs: Option<u64>,
    pub max_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileAgentIdentityConfig {
    pub name: Option<String>,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileAppConfig {
    pub listens: Option<Vec<String>>,
    pub connects: Option<Vec<String>>,
    pub up_connects: Option<Vec<String>>,
    pub down_connects: Option<Vec<String>>,
    pub local_serves: Option<Vec<String>>,
    pub remote_serves: Option<Vec<String>>,
    pub proxy_chain: Option<Vec<String>>,
    pub front_proxy: Option<String>,
    pub conn_policy: Option<ConnPolicy>,
    pub remote_peer_id: Option<String>,
    pub identity: Option<FileAgentIdentityConfig>,
    pub retry: Option<FileRetryPolicy>,
    pub wrapper: Option<WrapperConfig>,
    pub data_dir: Option<PathBuf>,
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlCommandConfig {
    PeersList { json: bool },
    PeersInfo { peer_id: String, json: bool },
    RoutesList { json: bool },
    ServicesList { json: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunnelEndpoint {
    pub url: ParsedUrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServeEndpoint {
    pub url: ParsedUrl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub listens: Vec<TunnelEndpoint>,
    pub connects: Vec<TunnelEndpoint>,
    pub up_connects: Vec<TunnelEndpoint>,
    pub down_connects: Vec<TunnelEndpoint>,
    pub local_serves: Vec<ServeEndpoint>,
    pub remote_serves: Vec<ServeEndpoint>,
    pub proxy_chain: Vec<String>,
    pub front_proxy: Option<String>,
    pub conn_policy: ConnPolicy,
    pub remote_peer_id: Option<String>,
    pub identity: AgentIdentityConfig,
    pub retry: RetryPolicy,
    pub wrapper: WrapperConfig,
    pub task_request: Option<TaskRequestConfig>,
    pub status_command: Option<StatusCommandConfig>,
    pub control_command: Option<ControlCommandConfig>,
    pub config_file: Option<PathBuf>,
    pub data_dir: PathBuf,
    pub log_level: String,
}

impl AppConfig {
    pub fn summary_lines(&self) -> Vec<String> {
        vec![
            format!(
                "agent.name={}",
                self.identity.name.as_deref().unwrap_or("<auto>")
            ),
            format!("listen.count={}", self.listens.len()),
            format!("connect.count={}", self.connects.len()),
            format!("up_connect.count={}", self.up_connects.len()),
            format!("down_connect.count={}", self.down_connects.len()),
            format!("local_serve.count={}", self.local_serves.len()),
            format!("remote_serve.count={}", self.remote_serves.len()),
            format!(
                "proxy.chain.count={}",
                self.proxy_chain.len() + usize::from(self.front_proxy.is_some())
            ),
            format!("conn.policy={:?}", self.conn_policy),
            format!(
                "remote.peer={}",
                self.remote_peer_id.as_deref().unwrap_or("<direct>")
            ),
            format!(
                "task.mode={}",
                self.task_request
                    .as_ref()
                    .map(|task| match &task.save_path {
                        Some(path) => format!("{:?}@{}", task.action, path.display()),
                        None => format!("{:?}", task.action),
                    })
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "wrapper.mode=compress:{},padding:{}",
                self.wrapper.compress,
                self.wrapper
                    .padding
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "status.mode={}",
                self.status_command
                    .as_ref()
                    .map(|s| format!("{:?}", s.scope))
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "control.mode={}",
                self.control_command
                    .as_ref()
                    .map(|cmd| match cmd {
                        ControlCommandConfig::PeersList { .. } => "PeersList".to_string(),
                        ControlCommandConfig::PeersInfo { peer_id, .. } => {
                            format!("PeersInfo@{peer_id}")
                        }
                        ControlCommandConfig::RoutesList { .. } => "RoutesList".to_string(),
                        ControlCommandConfig::ServicesList { .. } => "ServicesList".to_string(),
                    })
                    .unwrap_or_else(|| "disabled".to_string())
            ),
            format!(
                "config.file={}",
                self.config_file
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            ),
            format!("data_dir={}", self.data_dir.display()),
            format!("log_level={}", self.log_level),
            format!(
                "retry=max:{:?},interval:{}s,max_interval:{}s",
                self.retry.max_retries, self.retry.interval_secs, self.retry.max_interval_secs
            ),
        ]
    }
}

fn parse_tunnel_urls(values: &[String]) -> Result<Vec<TunnelEndpoint>, Error> {
    values
        .iter()
        .map(|value| parse_tunnel_endpoint(value).map(|(endpoint, _)| endpoint))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectDirection {
    Any,
    Up,
    Down,
}

pub fn split_connect_direction_prefix(input: &str) -> Result<(ConnectDirection, String), Error> {
    if let Some(rest) = input.strip_prefix("up-") {
        return Ok((ConnectDirection::Up, rest.to_string()));
    }
    if let Some(rest) = input.strip_prefix("down-") {
        return Ok((ConnectDirection::Down, rest.to_string()));
    }
    Ok((ConnectDirection::Any, input.to_string()))
}

pub fn parse_tunnel_endpoint(value: &str) -> Result<(TunnelEndpoint, ConnectDirection), Error> {
    let (direction, url) = split_connect_direction_prefix(value)?;
    Ok((
        TunnelEndpoint {
            url: ParsedUrl::parse(&url)?,
        },
        direction,
    ))
}

pub fn merge_connect_endpoints(
    connects: &[String],
    up_connects: &[String],
    down_connects: &[String],
) -> Result<
    (
        Vec<TunnelEndpoint>,
        Vec<TunnelEndpoint>,
        Vec<TunnelEndpoint>,
    ),
    Error,
> {
    let mut general = Vec::new();
    let mut up = parse_tunnel_urls(up_connects)?;
    let mut down = parse_tunnel_urls(down_connects)?;
    for value in connects {
        let (endpoint, direction) = parse_tunnel_endpoint(value)?;
        match direction {
            ConnectDirection::Any => general.push(endpoint),
            ConnectDirection::Up => up.push(endpoint),
            ConnectDirection::Down => down.push(endpoint),
        }
    }
    Ok((general, up, down))
}

fn parse_serve_urls(values: &[String]) -> Result<Vec<ServeEndpoint>, Error> {
    values
        .iter()
        .map(|value| {
            Ok(ServeEndpoint {
                url: ParsedUrl::parse(value)?,
            })
        })
        .collect()
}

pub fn app_config_from_file(path: &std::path::Path) -> Result<AppConfig, Error> {
    let path_buf = path.to_path_buf();
    let file = load_file_config(&path_buf)?;
    let retry = file.retry.unwrap_or_default();
    let (listens, connects, up_connects, down_connects) = {
        let listens = parse_tunnel_urls(&file.listens.unwrap_or_default())?;
        let (connects, up_connects, down_connects) = merge_connect_endpoints(
            &file.connects.unwrap_or_default(),
            &file.up_connects.unwrap_or_default(),
            &file.down_connects.unwrap_or_default(),
        )?;
        (listens, connects, up_connects, down_connects)
    };
    Ok(AppConfig {
        listens,
        connects,
        up_connects,
        down_connects,
        local_serves: parse_serve_urls(&file.local_serves.unwrap_or_default())?,
        remote_serves: parse_serve_urls(&file.remote_serves.unwrap_or_default())?,
        proxy_chain: file.proxy_chain.unwrap_or_default(),
        front_proxy: file.front_proxy,
        conn_policy: file.conn_policy.unwrap_or_default(),
        remote_peer_id: file.remote_peer_id,
        identity: AgentIdentityConfig {
            name: file.identity.as_ref().and_then(|id| id.name.clone()),
            key: file.identity.as_ref().and_then(|id| id.key.clone()),
        },
        retry: RetryPolicy {
            max_retries: retry.max_retries,
            interval_secs: retry.interval_secs.unwrap_or(10),
            max_interval_secs: retry.max_interval_secs.unwrap_or(300),
        },
        wrapper: file.wrapper.unwrap_or_default(),
        task_request: None,
        status_command: None,
        control_command: None,
        config_file: Some(path_buf),
        data_dir: file.data_dir.unwrap_or_else(|| PathBuf::from(".fusion")),
        log_level: file.log_level.unwrap_or_else(|| "warn".into()),
    })
}

pub fn load_file_config(path: &PathBuf) -> Result<FileAppConfig, Error> {
    let payload = std::fs::read_to_string(path).map_err(|err| {
        Error::new(
            err.kind(),
            format!("failed to read config file {}: {}", path.display(), err),
        )
    })?;
    toml::from_str(&payload).map_err(|err| {
        Error::new(
            ErrorKind::InvalidData,
            format!("failed to parse config file {}: {}", path.display(), err),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::{merge_connect_endpoints, parse_tunnel_endpoint, ConnectDirection, ParsedUrl};

    #[test]
    fn parse_tunnel_endpoint_strips_up_and_down_prefixes() {
        let (endpoint, direction) = parse_tunnel_endpoint("up-tcp://127.0.0.1:34996").unwrap();
        assert_eq!(direction, ConnectDirection::Up);
        assert_eq!(endpoint.url.scheme, "tcp");
        assert_eq!(endpoint.url.port, Some(34996));

        let (endpoint, direction) =
            parse_tunnel_endpoint("down-ws://127.0.0.1:8080/tunnel").unwrap();
        assert_eq!(direction, ConnectDirection::Down);
        assert_eq!(endpoint.url.scheme, "ws");
        assert_eq!(endpoint.url.path, "/tunnel");
    }

    #[test]
    fn merge_connect_endpoints_splits_directional_urls() {
        let (connects, up, down) = merge_connect_endpoints(
            &[
                "tcp://127.0.0.1:1".into(),
                "up-tcp://127.0.0.1:2".into(),
                "down-ws://127.0.0.1:3/tunnel".into(),
            ],
            &["tcp://127.0.0.1:9".into()],
            &[],
        )
        .unwrap();
        assert_eq!(connects.len(), 1);
        assert_eq!(connects[0].url.port, Some(1));
        assert_eq!(up.len(), 2);
        assert!(up.iter().any(|entry| entry.url.port == Some(2)));
        assert!(up.iter().any(|entry| entry.url.port == Some(9)));
        assert_eq!(down.len(), 1);
        assert_eq!(down[0].url.scheme, "ws");
    }

    #[test]
    fn parse_http_tunnel_url() {
        let (endpoint, direction) = parse_tunnel_endpoint("http://127.0.0.1:39090/task").unwrap();
        assert_eq!(direction, ConnectDirection::Any);
        assert_eq!(endpoint.url.scheme, "http");
        assert_eq!(endpoint.url.path, "/task");
    }

    #[test]
    fn parse_streamhttp_tunnel_url() {
        let parsed = ParsedUrl::parse("streamhttp://127.0.0.1:39100/events").unwrap();
        assert_eq!(parsed.scheme, "streamhttp");
        assert_eq!(parsed.path, "/events");
    }

    #[test]
    fn parse_dns_tunnel_url() {
        let parsed = ParsedUrl::parse("dns://127.0.0.1:5353/task.local").unwrap();
        assert_eq!(parsed.scheme, "dns");
        assert_eq!(parsed.path, "/task.local");
    }

    #[test]
    fn parse_h2_tunnel_url() {
        let parsed = ParsedUrl::parse("h2://127.0.0.1:39200/tunnel").unwrap();
        assert_eq!(parsed.scheme, "h2");
        assert_eq!(parsed.path, "/tunnel");
    }
}
