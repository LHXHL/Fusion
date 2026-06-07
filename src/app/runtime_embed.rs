use std::{io::Error, sync::Arc};

use tokio::task::JoinHandle;

use crate::{
    agent::identity::AgentIdentity,
    app::{
        config::AppConfig,
        runtime_bootstrap::prepare_runtime_config,
        runtime_mode::direct_port_forward_services,
        runtime_orchestrator::{
            find_runtime_services, spawn_inbound_tasks, spawn_outbound_tasks,
            spawn_remote_service_tasks, RuntimeShared,
        },
        runtime_status::{read_status_json, refresh_status_json, RuntimeConfigSummary},
        runtime_task::run_outbound_task_once_with_result,
    },
    crypto::wrapper::set_global_wrapper_config,
    protocol::message::TaskResultMessage,
};

pub struct RunningRuntime {
    pub shared: Arc<RuntimeShared>,
    pub config_summary: RuntimeConfigSummary,
    pub tasks: Vec<JoinHandle<()>>,
}

pub async fn spawn_runtime_from_config(config: AppConfig) -> Result<RunningRuntime, Error> {
    set_global_wrapper_config(config.wrapper.clone());
    let identity = AgentIdentity::from_config(&config.identity);
    let prepared = prepare_runtime_config(&config)?;
    if !prepared.active_runtime {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "runtime has no listeners, connects, or remote port-forward services",
        ));
    }

    let config_summary = RuntimeConfigSummary::from_app_config(&config);
    let shared = Arc::new(
        RuntimeShared::new(
            prepared.exposed_service_labels.clone(),
            &config.data_dir,
            config_summary.clone(),
        )
        .await,
    );

    let (
        inbound_raw_service,
        outbound_socks5_service,
        outbound_http_proxy_service,
        outbound_shadowsocks_service,
        outbound_trojan_service,
        outbound_egress_service,
        remote_port_forward_services,
    ) = find_runtime_services(&prepared.local_services, &prepared.remote_services);

    let mut tasks = spawn_remote_service_tasks(&direct_port_forward_services(
        &config.connects,
        &remote_port_forward_services,
    ))
    .await?;
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
        outbound_http_proxy_service,
        outbound_shadowsocks_service,
        outbound_trojan_service,
        outbound_egress_service,
        &remote_port_forward_services,
        &prepared.exposed_service_labels,
    ));

    Ok(RunningRuntime {
        shared,
        config_summary,
        tasks,
    })
}

pub fn stop_runtime_tasks(tasks: Vec<JoinHandle<()>>) {
    for task in tasks {
        task.abort();
    }
}

pub async fn runtime_status_json(
    data_dir: &std::path::Path,
    shared: Option<&RuntimeShared>,
    config_summary: &RuntimeConfigSummary,
    scope: crate::app::config::StatusScope,
) -> Result<String, Error> {
    if let Some(shared) = shared {
        refresh_status_json(
            &data_dir.to_path_buf(),
            &shared.hub,
            &shared.registry,
            &shared.relay_links,
            &shared.upstream_pools,
            config_summary,
            scope,
        )
        .await
    } else {
        read_status_json(&data_dir.to_path_buf(), scope)
    }
}

pub async fn execute_task_request(
    config: &AppConfig,
    task: crate::app::config::TaskRequestConfig,
) -> Result<TaskResultMessage, Error> {
    let prepared = prepare_runtime_config(config)?;
    let exposed = prepared.exposed_service_labels.clone();
    let shared = RuntimeShared::new(
        exposed.clone(),
        &config.data_dir,
        RuntimeConfigSummary::from_app_config(config),
    )
    .await;
    let identity = AgentIdentity::from_config(&config.identity);
    let proxy_chain =
        crate::app::conn_hub::build_proxy_chain(config.front_proxy.as_deref(), &config.proxy_chain);
    run_outbound_task_once_with_result(
        identity,
        &config.connects,
        task,
        config.data_dir.clone(),
        exposed,
        shared.hub,
        shared.registry,
        config.conn_policy.clone(),
        proxy_chain,
    )
    .await
}
