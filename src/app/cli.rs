use std::io::{Error, ErrorKind};
use std::path::PathBuf;

use clap::{parser::ValueSource, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use data_encoding::HEXLOWER;

use crate::app::config::{
    load_file_config, AgentIdentityConfig, AppConfig, ConnPolicy, ControlCommandConfig,
    FileAppConfig, RetryPolicy, ServeEndpoint, StatusCommandConfig, StatusScope, TaskRequestConfig,
    TunnelEndpoint,
};
use crate::crypto::wrapper::WrapperConfig;
use crate::protocol::message::TaskAction;
use crate::utils::url::ParsedUrl;

#[derive(Debug, Parser)]
#[command(name = "fusion")]
#[command(about = "Fusion unified peer-to-peer agent runtime", long_about = None)]
pub struct CliArgs {
    #[arg(short = 's', long = "listen", value_name = "URL", global = true)]
    pub listens: Vec<String>,

    #[arg(short = 'c', long = "connect", value_name = "URL", global = true)]
    pub connects: Vec<String>,

    #[arg(long = "up-connect", value_name = "URL", global = true)]
    pub up_connects: Vec<String>,

    #[arg(long = "down-connect", value_name = "URL", global = true)]
    pub down_connects: Vec<String>,

    #[arg(short = 'l', long = "local-serve", value_name = "URL", global = true)]
    pub local_serves: Vec<String>,

    #[arg(short = 'r', long = "remote-serve", value_name = "URL", global = true)]
    pub remote_serves: Vec<String>,

    #[arg(short = 'x', long = "proxy-chain", value_name = "URL", global = true)]
    pub proxy_chain: Vec<String>,

    #[arg(short = 'f', long = "front-proxy", value_name = "URL", global = true)]
    pub front_proxy: Option<String>,

    #[arg(
        long = "conn-policy",
        value_name = "POLICY",
        default_value = "fallback",
        global = true
    )]
    pub conn_policy: ConnPolicy,

    #[arg(long = "remote-peer", value_name = "AGENT_ID", global = true)]
    pub remote_peer: Option<String>,

    #[arg(short = 'a', long = "agent-name", value_name = "NAME", global = true)]
    pub agent_name: Option<String>,

    #[arg(short = 'k', long = "key", value_name = "SECRET", global = true)]
    pub key: Option<String>,

    #[arg(long = "wrap-compress", global = true)]
    pub wrap_compress: bool,

    #[arg(long = "wrap-padding", value_name = "BYTES", global = true)]
    pub wrap_padding: Option<usize>,

    #[arg(long = "retry", value_name = "N", global = true)]
    pub retry: Option<u32>,

    #[arg(
        long = "retry-interval",
        default_value_t = 10,
        value_name = "SECONDS",
        global = true
    )]
    pub retry_interval: u64,

    #[arg(
        long = "retry-max-interval",
        default_value_t = 300,
        value_name = "SECONDS",
        global = true
    )]
    pub retry_max_interval: u64,

    #[arg(
        long = "data-dir",
        default_value = ".fusion",
        value_name = "PATH",
        global = true
    )]
    pub data_dir: PathBuf,

    #[arg(long = "config", value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    #[arg(long = "task-shell", value_name = "COMMAND", global = true)]
    pub task_shell: Option<String>,

    #[arg(long = "task-screenshot", global = true)]
    pub task_screenshot: bool,

    #[arg(long = "task-download", value_name = "REMOTE_PATH", global = true)]
    pub task_download: Option<String>,

    #[arg(long = "task-upload", value_name = "LOCAL:REMOTE", global = true)]
    pub task_upload: Option<String>,

    #[arg(long = "task-save", value_name = "PATH", global = true)]
    pub task_save: Option<PathBuf>,

    #[arg(long = "task-peer", value_name = "AGENT_ID", global = true)]
    pub task_peer: Option<String>,

    #[arg(
        long = "log-level",
        default_value = "info",
        value_name = "LEVEL",
        global = true
    )]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Option<FusionCommand>,
}

#[derive(Debug, Subcommand)]
pub enum FusionCommand {
    Task(TaskCommand),
    Status(StatusCommand),
    Peers(PeersCommand),
    Routes(RoutesCommand),
    Services(ServicesCommand),
}

#[derive(Debug, Args)]
pub struct TaskCommand {
    #[command(subcommand)]
    pub kind: TaskKind,
}

#[derive(Debug, Args)]
pub struct StatusCommand {
    #[arg(long = "json")]
    pub json: bool,

    #[command(subcommand)]
    pub kind: Option<StatusKind>,
}

#[derive(Debug, Args)]
pub struct PeersCommand {
    #[arg(long = "json")]
    pub json: bool,

    #[command(subcommand)]
    pub kind: PeersKind,
}

#[derive(Debug, Args)]
pub struct RoutesCommand {
    #[arg(long = "json")]
    pub json: bool,

    #[command(subcommand)]
    pub kind: RoutesKind,
}

#[derive(Debug, Args)]
pub struct ServicesCommand {
    #[arg(long = "json")]
    pub json: bool,

    #[command(subcommand)]
    pub kind: ServicesKind,
}

#[derive(Debug, Subcommand)]
pub enum TaskKind {
    Shell {
        command: String,
    },
    Screenshot,
    Download {
        remote_path: String,
    },
    Upload {
        local_path: String,
        remote_path: String,
    },
}

#[derive(Debug, Subcommand, Clone)]
pub enum StatusKind {
    Peers,
    Routes,
    Streams,
}

#[derive(Debug, Subcommand)]
pub enum PeersKind {
    List,
    Info { peer_id: String },
}

#[derive(Debug, Subcommand)]
pub enum RoutesKind {
    List,
}

#[derive(Debug, Subcommand)]
pub enum ServicesKind {
    List,
}

impl CliArgs {
    pub fn parse_config() -> Result<AppConfig, Error> {
        let matches = Self::command().get_matches();
        let value_sources = CliValueSources::from_matches(&matches);
        let args = Self::from_arg_matches(&matches)
            .map_err(|e| Error::new(ErrorKind::InvalidInput, e.to_string()))?;
        build_app_config(args, value_sources)
    }
}

impl TryFrom<CliArgs> for AppConfig {
    type Error = Error;

    fn try_from(value: CliArgs) -> Result<Self, Self::Error> {
        build_app_config(value, CliValueSources::default())
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct CliValueSources {
    listens: bool,
    connects: bool,
    up_connects: bool,
    down_connects: bool,
    local_serves: bool,
    remote_serves: bool,
    proxy_chain: bool,
    front_proxy: bool,
    conn_policy: bool,
    remote_peer: bool,
    agent_name: bool,
    key: bool,
    wrap_compress: bool,
    wrap_padding: bool,
    retry: bool,
    retry_interval: bool,
    retry_max_interval: bool,
    data_dir: bool,
    log_level: bool,
    config: bool,
}

impl CliValueSources {
    fn from_matches(matches: &clap::ArgMatches) -> Self {
        Self {
            listens: matches.value_source("listens") == Some(ValueSource::CommandLine),
            connects: matches.value_source("connects") == Some(ValueSource::CommandLine),
            up_connects: matches.value_source("up_connects") == Some(ValueSource::CommandLine),
            down_connects: matches.value_source("down_connects") == Some(ValueSource::CommandLine),
            local_serves: matches.value_source("local_serves") == Some(ValueSource::CommandLine),
            remote_serves: matches.value_source("remote_serves") == Some(ValueSource::CommandLine),
            proxy_chain: matches.value_source("proxy_chain") == Some(ValueSource::CommandLine),
            front_proxy: matches.value_source("front_proxy") == Some(ValueSource::CommandLine),
            conn_policy: matches.value_source("conn_policy") == Some(ValueSource::CommandLine),
            remote_peer: matches.value_source("remote_peer") == Some(ValueSource::CommandLine),
            agent_name: matches.value_source("agent_name") == Some(ValueSource::CommandLine),
            key: matches.value_source("key") == Some(ValueSource::CommandLine),
            wrap_compress: matches.value_source("wrap_compress") == Some(ValueSource::CommandLine),
            wrap_padding: matches.value_source("wrap_padding") == Some(ValueSource::CommandLine),
            retry: matches.value_source("retry") == Some(ValueSource::CommandLine),
            retry_interval: matches.value_source("retry_interval")
                == Some(ValueSource::CommandLine),
            retry_max_interval: matches.value_source("retry_max_interval")
                == Some(ValueSource::CommandLine),
            data_dir: matches.value_source("data_dir") == Some(ValueSource::CommandLine),
            log_level: matches.value_source("log_level") == Some(ValueSource::CommandLine),
            config: matches.value_source("config") == Some(ValueSource::CommandLine),
        }
    }
}

fn build_app_config(value: CliArgs, value_sources: CliValueSources) -> Result<AppConfig, Error> {
    let config_file = resolve_config_file(&value, value_sources)?;
    let file_config = match &config_file {
        Some(path) => Some(load_file_config(path)?),
        None => None,
    };

    let listens = choose_string_list(
        &value.listens,
        value_sources.listens,
        file_config.as_ref().and_then(|cfg| cfg.listens.as_ref()),
    );
    let connects = choose_string_list(
        &value.connects,
        value_sources.connects,
        file_config.as_ref().and_then(|cfg| cfg.connects.as_ref()),
    );
    let local_serves = choose_string_list(
        &value.local_serves,
        value_sources.local_serves,
        file_config
            .as_ref()
            .and_then(|cfg| cfg.local_serves.as_ref()),
    );
    let remote_serves = choose_string_list(
        &value.remote_serves,
        value_sources.remote_serves,
        file_config
            .as_ref()
            .and_then(|cfg| cfg.remote_serves.as_ref()),
    );
    let up_connects = choose_string_list(
        &value.up_connects,
        value_sources.up_connects,
        file_config
            .as_ref()
            .and_then(|cfg| cfg.up_connects.as_ref()),
    );
    let down_connects = choose_string_list(
        &value.down_connects,
        value_sources.down_connects,
        file_config
            .as_ref()
            .and_then(|cfg| cfg.down_connects.as_ref()),
    );
    let proxy_chain = choose_string_list(
        &value.proxy_chain,
        value_sources.proxy_chain,
        file_config
            .as_ref()
            .and_then(|cfg| cfg.proxy_chain.as_ref()),
    );

    let retry = merged_retry_policy(&value, value_sources, file_config.as_ref());
    if retry.interval_secs == 0 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "retry interval must be greater than 0",
        ));
    }
    if retry.max_interval_secs < retry.interval_secs {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "retry max interval must be greater than or equal to retry interval",
        ));
    }

    let task_request = parse_task_request(&value)?;
    let status_command = parse_status_command(&value)?;
    let control_command = parse_control_command(&value)?;
    let wrapper = WrapperConfig {
        compress: if value_sources.wrap_compress {
            value.wrap_compress
        } else {
            file_config
                .as_ref()
                .and_then(|cfg| cfg.wrapper.as_ref().map(|wrapper| wrapper.compress))
                .unwrap_or(value.wrap_compress)
        },
        padding: if value_sources.wrap_padding {
            value.wrap_padding
        } else {
            file_config
                .as_ref()
                .and_then(|cfg| cfg.wrapper.as_ref().and_then(|wrapper| wrapper.padding))
                .or(value.wrap_padding)
        },
    };

    Ok(AppConfig {
        listens: parse_tunnel_list(&listens)?,
        connects: parse_tunnel_list(&connects)?,
        up_connects: parse_tunnel_list(&up_connects)?,
        down_connects: parse_tunnel_list(&down_connects)?,
        local_serves: parse_serve_list(&local_serves)?,
        remote_serves: parse_serve_list(&remote_serves)?,
        proxy_chain,
        front_proxy: choose_option_string(
            value.front_proxy,
            value_sources.front_proxy,
            file_config.as_ref().and_then(|cfg| cfg.front_proxy.clone()),
        ),
        conn_policy: if value_sources.conn_policy {
            value.conn_policy
        } else {
            file_config
                .as_ref()
                .and_then(|cfg| cfg.conn_policy.clone())
                .unwrap_or(value.conn_policy)
        },
        remote_peer_id: choose_option_string(
            value.remote_peer,
            value_sources.remote_peer,
            file_config
                .as_ref()
                .and_then(|cfg| cfg.remote_peer_id.clone()),
        ),
        identity: AgentIdentityConfig {
            name: choose_option_string(
                value.agent_name,
                value_sources.agent_name,
                file_config.as_ref().and_then(|cfg| {
                    cfg.identity
                        .as_ref()
                        .and_then(|identity| identity.name.clone())
                }),
            ),
            key: choose_option_string(
                value.key,
                value_sources.key,
                file_config.as_ref().and_then(|cfg| {
                    cfg.identity
                        .as_ref()
                        .and_then(|identity| identity.key.clone())
                }),
            ),
        },
        retry,
        wrapper,
        task_request,
        status_command,
        control_command,
        config_file,
        data_dir: choose_path(
            value.data_dir,
            value_sources.data_dir,
            file_config.as_ref().and_then(|cfg| cfg.data_dir.clone()),
        ),
        log_level: choose_string(
            value.log_level,
            value_sources.log_level,
            file_config.as_ref().and_then(|cfg| cfg.log_level.clone()),
        ),
    })
}

fn resolve_config_file(
    value: &CliArgs,
    value_sources: CliValueSources,
) -> Result<Option<PathBuf>, Error> {
    if value_sources.config {
        let path = value.config.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "--config was provided without a path",
            )
        })?;
        return Ok(Some(path));
    }

    let default = PathBuf::from("fusion.toml");
    if default.exists() {
        return Ok(Some(default));
    }

    Ok(None)
}

fn choose_string_list(
    cli: &[String],
    cli_explicit: bool,
    file: Option<&Vec<String>>,
) -> Vec<String> {
    if cli_explicit || file.is_none() {
        cli.to_vec()
    } else {
        file.cloned().unwrap_or_default()
    }
}

fn choose_option_string(
    cli: Option<String>,
    cli_explicit: bool,
    file: Option<String>,
) -> Option<String> {
    if cli_explicit || cli.is_some() {
        cli
    } else {
        file
    }
}

fn choose_string(cli: String, cli_explicit: bool, file: Option<String>) -> String {
    if cli_explicit {
        cli
    } else {
        file.unwrap_or(cli)
    }
}

fn choose_path(cli: PathBuf, cli_explicit: bool, file: Option<PathBuf>) -> PathBuf {
    if cli_explicit {
        cli
    } else {
        file.unwrap_or(cli)
    }
}

fn merged_retry_policy(
    value: &CliArgs,
    value_sources: CliValueSources,
    file: Option<&FileAppConfig>,
) -> RetryPolicy {
    let file_retry = file.and_then(|cfg| cfg.retry.as_ref());
    RetryPolicy {
        max_retries: if value_sources.retry || value.retry.is_some() {
            value.retry
        } else {
            file_retry.and_then(|retry| retry.max_retries)
        },
        interval_secs: if value_sources.retry_interval {
            value.retry_interval
        } else {
            file_retry
                .and_then(|retry| retry.interval_secs)
                .unwrap_or(value.retry_interval)
        },
        max_interval_secs: if value_sources.retry_max_interval {
            value.retry_max_interval
        } else {
            file_retry
                .and_then(|retry| retry.max_interval_secs)
                .unwrap_or(value.retry_max_interval)
        },
    }
}

fn parse_status_command(args: &CliArgs) -> Result<Option<StatusCommandConfig>, Error> {
    let Some(FusionCommand::Status(status)) = args.command.as_ref() else {
        return Ok(None);
    };
    let scope = match status.kind.clone() {
        None => StatusScope::All,
        Some(StatusKind::Peers) => StatusScope::Peers,
        Some(StatusKind::Routes) => StatusScope::Routes,
        Some(StatusKind::Streams) => StatusScope::Streams,
    };
    Ok(Some(StatusCommandConfig {
        scope,
        json: status.json,
    }))
}

fn parse_control_command(args: &CliArgs) -> Result<Option<ControlCommandConfig>, Error> {
    let command = match args.command.as_ref() {
        Some(FusionCommand::Peers(peers)) => match &peers.kind {
            PeersKind::List => Some(ControlCommandConfig::PeersList { json: peers.json }),
            PeersKind::Info { peer_id } => Some(ControlCommandConfig::PeersInfo {
                peer_id: peer_id.clone(),
                json: peers.json,
            }),
        },
        Some(FusionCommand::Routes(routes)) => match routes.kind {
            RoutesKind::List => Some(ControlCommandConfig::RoutesList { json: routes.json }),
        },
        Some(FusionCommand::Services(services)) => match services.kind {
            ServicesKind::List => Some(ControlCommandConfig::ServicesList {
                json: services.json,
            }),
        },
        _ => None,
    };
    Ok(command)
}

fn parse_task_request(args: &CliArgs) -> Result<Option<TaskRequestConfig>, Error> {
    let from_flags = parse_task_request_from_flags(args)?;
    let from_command = parse_task_request_from_command(args)?;

    match (from_flags, from_command) {
        (Some(_), Some(_)) => Err(Error::new(
            ErrorKind::InvalidInput,
            "cannot combine --task-* flags with `fusion task ...` subcommand",
        )),
        (Some(task), None) | (None, Some(task)) => Ok(Some(task)),
        (None, None) => Ok(None),
    }
}

fn parse_task_request_from_flags(args: &CliArgs) -> Result<Option<TaskRequestConfig>, Error> {
    let mut count = 0;
    if args.task_shell.is_some() {
        count += 1;
    }
    if args.task_screenshot {
        count += 1;
    }
    if args.task_download.is_some() {
        count += 1;
    }
    if args.task_upload.is_some() {
        count += 1;
    }
    if count == 0 {
        return Ok(None);
    }
    if count > 1 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "only one task mode can be specified at a time",
        ));
    }

    if let Some(command) = &args.task_shell {
        return Ok(Some(TaskRequestConfig {
            action: TaskAction::Shell,
            args: vec![command.clone()],
            data_hex: None,
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        }));
    }
    if args.task_screenshot {
        return Ok(Some(TaskRequestConfig {
            action: TaskAction::Screenshot,
            args: vec![],
            data_hex: None,
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        }));
    }
    if let Some(path) = &args.task_download {
        return Ok(Some(TaskRequestConfig {
            action: TaskAction::FileDownload,
            args: vec![path.clone()],
            data_hex: None,
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        }));
    }
    if let Some(spec) = &args.task_upload {
        let (local, remote) = spec.split_once(':').ok_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "task-upload expects LOCAL:REMOTE")
        })?;
        let bytes = std::fs::read(local)?;
        return Ok(Some(TaskRequestConfig {
            action: TaskAction::FileUpload,
            args: vec![remote.to_string()],
            data_hex: Some(HEXLOWER.encode(&bytes)),
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        }));
    }

    Ok(None)
}

fn parse_task_request_from_command(args: &CliArgs) -> Result<Option<TaskRequestConfig>, Error> {
    let Some(FusionCommand::Task(task)) = args.command.as_ref() else {
        return Ok(None);
    };

    let request = match &task.kind {
        TaskKind::Shell { command } => TaskRequestConfig {
            action: TaskAction::Shell,
            args: vec![command.clone()],
            data_hex: None,
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        },
        TaskKind::Screenshot => TaskRequestConfig {
            action: TaskAction::Screenshot,
            args: vec![],
            data_hex: None,
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        },
        TaskKind::Download { remote_path } => TaskRequestConfig {
            action: TaskAction::FileDownload,
            args: vec![remote_path.clone()],
            data_hex: None,
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        },
        TaskKind::Upload {
            local_path,
            remote_path,
        } => TaskRequestConfig {
            action: TaskAction::FileUpload,
            args: vec![remote_path.clone()],
            data_hex: Some(HEXLOWER.encode(&std::fs::read(local_path)?)),
            save_path: args.task_save.clone(),
            target_agent_id: args.task_peer.clone(),
        },
    };

    Ok(Some(request))
}

fn parse_tunnel_list(values: &[String]) -> Result<Vec<TunnelEndpoint>, Error> {
    values
        .iter()
        .map(|value| {
            Ok(TunnelEndpoint {
                url: ParsedUrl::parse(value)?,
            })
        })
        .collect()
}

fn parse_serve_list(values: &[String]) -> Result<Vec<ServeEndpoint>, Error> {
    values
        .iter()
        .map(|value| {
            Ok(ServeEndpoint {
                url: ParsedUrl::parse(value)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::CliArgs;
    use crate::app::config::AppConfig;

    #[test]
    fn convert_args_to_config() {
        let args = CliArgs::parse_from([
            "fusion",
            "-s",
            "tcp://0.0.0.0:34996",
            "-c",
            "ws://127.0.0.1:8080/tunnel",
            "-l",
            "socks5://127.0.0.1:1080",
            "-r",
            "raw://example.com:80",
            "-a",
            "node-a",
            "--task-shell",
            "whoami",
            "--remote-peer",
            "serve-456",
            "--task-save",
            "/tmp/task.txt",
            "--task-peer",
            "peer-123",
        ]);

        let cfg = AppConfig::try_from(args).unwrap();
        assert_eq!(cfg.listens.len(), 1);
        assert_eq!(cfg.connects.len(), 1);
        assert_eq!(cfg.local_serves.len(), 1);
        assert_eq!(cfg.remote_serves.len(), 1);
        assert_eq!(cfg.identity.name.as_deref(), Some("node-a"));
        assert_eq!(cfg.remote_peer_id.as_deref(), Some("serve-456"));
        assert!(cfg.task_request.is_some());
        assert_eq!(
            cfg.task_request.as_ref().unwrap().save_path.as_deref(),
            Some(std::path::Path::new("/tmp/task.txt"))
        );
        assert_eq!(
            cfg.task_request
                .as_ref()
                .unwrap()
                .target_agent_id
                .as_deref(),
            Some("peer-123")
        );
        assert!(cfg.status_command.is_none());
        assert!(cfg.control_command.is_none());
    }

    #[test]
    fn parse_task_subcommand_into_config() {
        let args = CliArgs::parse_from([
            "fusion",
            "-c",
            "tcp://127.0.0.1:12345",
            "task",
            "shell",
            "whoami",
            "--task-save",
            "/tmp/task-sub.txt",
            "--task-peer",
            "peer-sub",
        ]);

        let cfg = AppConfig::try_from(args).unwrap();
        let task = cfg.task_request.unwrap();
        assert!(matches!(
            task.action,
            crate::protocol::message::TaskAction::Shell
        ));
        assert_eq!(task.args, vec!["whoami"]);
        assert_eq!(
            task.save_path.as_deref(),
            Some(std::path::Path::new("/tmp/task-sub.txt"))
        );
    }

    #[test]
    fn parse_status_subcommand_into_config() {
        let args = CliArgs::parse_from(["fusion", "--data-dir", ".fusion", "status", "routes"]);
        let cfg = AppConfig::try_from(args).unwrap();
        let status = cfg.status_command.unwrap();
        assert!(matches!(
            status.scope,
            crate::app::config::StatusScope::Routes
        ));
        assert!(!status.json);
        assert!(cfg.task_request.is_none());
    }

    #[test]
    fn parse_status_json_subcommand_into_config() {
        let args = CliArgs::parse_from(["fusion", "status", "--json", "streams"]);
        let cfg = AppConfig::try_from(args).unwrap();
        let status = cfg.status_command.unwrap();
        assert!(matches!(
            status.scope,
            crate::app::config::StatusScope::Streams
        ));
        assert!(status.json);
        assert!(cfg.task_request.is_none());
    }

    #[test]
    fn parse_peers_info_subcommand_into_config() {
        let args = CliArgs::parse_from(["fusion", "peers", "--json", "info", "peer-z"]);
        let cfg = AppConfig::try_from(args).unwrap();
        match cfg.control_command.unwrap() {
            crate::app::config::ControlCommandConfig::PeersInfo { peer_id, json } => {
                assert_eq!(peer_id, "peer-z");
                assert!(json);
            }
            other => panic!("unexpected control command: {:?}", other),
        }
    }

    #[test]
    fn parse_services_list_subcommand_into_config() {
        let args = CliArgs::parse_from(["fusion", "services", "list"]);
        let cfg = AppConfig::try_from(args).unwrap();
        assert!(matches!(
            cfg.control_command.unwrap(),
            crate::app::config::ControlCommandConfig::ServicesList { json: false }
        ));
    }

    #[test]
    fn parse_phase5_connect_options() {
        let args = CliArgs::parse_from([
            "fusion",
            "--up-connect",
            "tcp://127.0.0.1:1001",
            "--down-connect",
            "ws://127.0.0.1:1002/tunnel",
            "-x",
            "socks5://127.0.0.1:1080",
            "-f",
            "http://127.0.0.1:8080",
            "--conn-policy",
            "round-robin",
        ]);
        let cfg = AppConfig::try_from(args).unwrap();
        assert_eq!(cfg.up_connects.len(), 1);
        assert_eq!(cfg.down_connects.len(), 1);
        assert_eq!(cfg.proxy_chain, vec!["socks5://127.0.0.1:1080"]);
        assert_eq!(cfg.front_proxy.as_deref(), Some("http://127.0.0.1:8080"));
        assert!(matches!(
            cfg.conn_policy,
            crate::app::config::ConnPolicy::RoundRobin
        ));
    }

    #[test]
    fn parse_wrapper_options() {
        let args = CliArgs::parse_from([
            "fusion",
            "--wrap-compress",
            "--wrap-padding",
            "64",
            "-c",
            "tcp://127.0.0.1:1001",
        ]);
        let cfg = AppConfig::try_from(args).unwrap();
        assert!(cfg.wrapper.compress);
        assert_eq!(cfg.wrapper.padding, Some(64));
    }
}
