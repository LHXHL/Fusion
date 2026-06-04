use std::io::Error;

use log::info;
use tokio::task::JoinHandle;

use crate::{
    agent::identity::AgentIdentity,
    app::{
        config::AppConfig,
        runtime_bootstrap::{emit_bootstrap_summary, emit_service_summary, prepare_runtime_config},
        runtime_orchestrator::{
            find_runtime_services, print_runtime_summary, spawn_inbound_tasks,
            spawn_outbound_tasks, spawn_remote_service_tasks, RuntimeShared,
        },
        runtime_status::{print_control_snapshot, print_status_snapshot},
    },
};

pub(crate) fn build_stream_target_label(host: &Option<String>, port: Option<u16>) -> String {
    match (host.as_deref(), port) {
        (Some(host), Some(port)) => format!("{}:{}", host, port),
        _ => "dynamic".to_string(),
    }
}

pub async fn run(config: AppConfig) -> Result<(), Error> {
    if let Some(status) = &config.status_command {
        return print_status_snapshot(&config.data_dir, status.scope.clone(), status.json).await;
    }
    if let Some(control) = &config.control_command {
        return print_control_snapshot(&config.data_dir, control).await;
    }

    info!("starting fusion unified runtime");
    let identity = AgentIdentity::from_config(&config.identity);
    emit_bootstrap_summary(&config, &identity)?;

    let prepared = prepare_runtime_config(&config)?;
    emit_service_summary(&prepared.local_services, &prepared.remote_services);
    if !prepared.active_runtime {
        return Ok(());
    }

    let shared =
        RuntimeShared::new(prepared.exposed_service_labels.clone(), &config.data_dir).await;
    let (
        inbound_raw_service,
        outbound_socks5_service,
        outbound_egress_service,
        remote_port_forward_services,
    ) = find_runtime_services(&prepared.local_services, &prepared.remote_services);

    let mut tasks: Vec<JoinHandle<()>> =
        spawn_remote_service_tasks(&remote_port_forward_services).await?;
    tasks.extend(
        spawn_inbound_tasks(
            &config,
            &identity,
            &shared,
            inbound_raw_service,
            !prepared.local_services.is_empty(),
            &prepared.exposed_service_labels,
        )
        .await,
    );
    tasks.extend(spawn_outbound_tasks(
        &config,
        &identity,
        &shared,
        outbound_socks5_service,
        outbound_egress_service,
        &prepared.exposed_service_labels,
    ));

    for task in tasks {
        let _ = task.await;
    }

    print_runtime_summary(&shared).await;

    Ok(())
}
