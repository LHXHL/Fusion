use std::io::{Error, ErrorKind};

use chrono::Utc;
use log::info;

use crate::{
    agent::identity::AgentIdentity,
    app::config::AppConfig,
    protocol::{
        codec::encode_frame,
        frame::{Frame, MessageType},
        message::{HelloMessage, Message},
    },
    serve::service::{
        build_local_services, build_remote_services, validate_service_pairing, ServiceDefinition,
        ServiceKind,
    },
};

pub struct PreparedRuntime {
    pub local_services: Vec<ServiceDefinition>,
    pub remote_services: Vec<ServiceDefinition>,
    pub exposed_service_labels: Vec<String>,
    pub active_runtime: bool,
}

pub fn prepare_runtime_config(config: &AppConfig) -> Result<PreparedRuntime, Error> {
    let local_services = build_local_services(&config.local_serves)?;
    let remote_services = build_remote_services(&config.remote_serves)?;
    validate_service_pairing(&local_services, &remote_services)?;

    let local_service_labels: Vec<String> = local_services
        .iter()
        .map(|svc| svc.summary_line())
        .collect();
    let remote_service_labels: Vec<String> = remote_services
        .iter()
        .map(|svc| svc.summary_line())
        .collect();
    let exposed_service_labels: Vec<String> = local_service_labels
        .iter()
        .chain(remote_service_labels.iter())
        .cloned()
        .collect();

    let has_port_forward_service = remote_services
        .iter()
        .any(|svc| matches!(svc.kind, ServiceKind::RemotePortForward(_)));
    let active_runtime =
        !config.listens.is_empty() || !config.connects.is_empty() || has_port_forward_service;

    Ok(PreparedRuntime {
        local_services,
        remote_services,
        exposed_service_labels,
        active_runtime,
    })
}

pub fn emit_bootstrap_summary(config: &AppConfig, identity: &AgentIdentity) -> Result<(), Error> {
    let hello = Frame::new(
        MessageType::Hello,
        Some(identity.id.clone()),
        None,
        Message::Hello(HelloMessage {
            agent_id: identity.id.clone(),
            agent_name: identity.name.clone(),
            capabilities: identity.capability_labels(),
            protocol_version: 1,
        }),
    );

    let hello_json = String::from_utf8(encode_frame(&hello).map_err(|e| {
        Error::new(
            ErrorKind::InvalidData,
            format!("failed to encode hello frame: {e}"),
        )
    })?)
    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

    for line in config.summary_lines() {
        info!("{line}");
    }
    info!("agent.id={}", identity.id);
    info!("agent.hostname={}", identity.hostname);
    info!("bootstrap.hello={hello_json}");

    println!("Fusion unified runtime bootstrap complete.");
    for line in config.summary_lines() {
        println!("{line}");
    }
    println!("agent.id={}", identity.id);
    println!("agent.name={}", identity.name);
    println!("agent.hostname={}", identity.hostname);
    println!("agent.os={}", identity.os);
    println!("agent.arch={}", identity.arch);
    println!("bootstrap.timestamp={}", Utc::now().timestamp());
    println!("bootstrap.hello={hello_json}");

    Ok(())
}

pub fn emit_service_summary(
    local_services: &[ServiceDefinition],
    remote_services: &[ServiceDefinition],
) {
    for svc in local_services {
        println!("{}", svc.summary_line());
    }
    for svc in remote_services {
        println!("{}", svc.summary_line());
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        app::config::{AgentIdentityConfig, AppConfig, RetryPolicy, ServeEndpoint, TunnelEndpoint},
        utils::url::ParsedUrl,
    };

    use super::prepare_runtime_config;

    fn app_config(
        listens: Vec<&str>,
        connects: Vec<&str>,
        local_serves: Vec<&str>,
        remote_serves: Vec<&str>,
    ) -> AppConfig {
        AppConfig {
            listens: listens
                .into_iter()
                .map(|url| TunnelEndpoint {
                    url: ParsedUrl::parse(url).unwrap(),
                })
                .collect(),
            connects: connects
                .into_iter()
                .map(|url| TunnelEndpoint {
                    url: ParsedUrl::parse(url).unwrap(),
                })
                .collect(),
            local_serves: local_serves
                .into_iter()
                .map(|url| ServeEndpoint {
                    url: ParsedUrl::parse(url).unwrap(),
                })
                .collect(),
            remote_serves: remote_serves
                .into_iter()
                .map(|url| ServeEndpoint {
                    url: ParsedUrl::parse(url).unwrap(),
                })
                .collect(),
            remote_peer_id: None,
            identity: AgentIdentityConfig {
                name: Some("bootstrap-test".into()),
                key: None,
            },
            retry: RetryPolicy::default(),
            task_request: None,
            status_command: None,
            control_command: None,
            config_file: None,
            data_dir: std::env::temp_dir(),
            log_level: "info".into(),
        }
    }

    #[test]
    fn prepared_runtime_is_inactive_without_tunnels_or_port_forward() {
        let config = app_config(vec![], vec![], vec![], vec!["raw://example.com:80"]);
        let prepared = prepare_runtime_config(&config).unwrap();
        assert!(!prepared.active_runtime);
        assert_eq!(prepared.exposed_service_labels.len(), 1);
    }

    #[test]
    fn prepared_runtime_is_active_with_port_forward_service() {
        let config = app_config(
            vec![],
            vec![],
            vec![],
            vec!["port://127.0.0.1:8080->example.com:80"],
        );
        let prepared = prepare_runtime_config(&config).unwrap();
        assert!(prepared.active_runtime);
        assert!(prepared
            .exposed_service_labels
            .iter()
            .any(|line| line.contains("service.remote=port://")));
    }
}
