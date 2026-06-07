use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::{
    agent::{identity::AgentIdentity, registry::AgentRegistry},
    app::{
        config::{ConnPolicy, TunnelEndpoint},
        conn_hub::order_endpoints,
        runtime_status::{publish_upstream_pool_status, UpstreamPoolStatusMap},
    },
    session::hub::SessionHub,
    tunnel::{tcp_mux, ws_mux},
};

#[derive(Clone)]
pub struct TcpMuxUpstreamPool {
    peers: Arc<Mutex<HashMap<String, tcp_mux::MuxTcpPeer>>>,
    status: Option<(UpstreamPoolStatusMap, String)>,
}

impl Default for TcpMuxUpstreamPool {
    fn default() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            status: None,
        }
    }
}

impl TcpMuxUpstreamPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_status(status: UpstreamPoolStatusMap, handler: impl Into<String>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            status: Some((status, handler.into())),
        }
    }

    async fn sync_status(&self) {
        let Some((status, handler)) = self.status.as_ref() else {
            return;
        };
        publish_upstream_pool_status(status, handler, "tcp_mux", self.snapshot_keys().await).await;
    }

    async fn invalidate_if_stale(
        &self,
        key: &str,
        peer: &tcp_mux::MuxTcpPeer,
        hub: &Arc<Mutex<SessionHub>>,
        registry: &Arc<Mutex<AgentRegistry>>,
    ) -> bool {
        let peer_id = &peer.session.remote.agent_id;
        let hub_active = hub.lock().await.is_active(peer_id);
        let registry_active = registry.lock().await.is_peer_active(peer_id);
        if hub_active && registry_active {
            return false;
        }
        self.peers.lock().await.remove(key);
        true
    }

    pub async fn acquire(
        &self,
        identity: AgentIdentity,
        endpoints: &[TunnelEndpoint],
        conn_policy: &ConnPolicy,
        proxy_chain: &[String],
        hub: &Arc<Mutex<SessionHub>>,
        registry: &Arc<Mutex<AgentRegistry>>,
    ) -> Result<(String, tcp_mux::MuxTcpPeer), Error> {
        let ordered = order_endpoints(endpoints, conn_policy)?;
        let mut last_err = None;
        for endpoint in ordered {
            let key = endpoint.url.original.clone();
            if let Some(peer) = self.peers.lock().await.get(&key).cloned() {
                if self.invalidate_if_stale(&key, &peer, hub, registry).await {
                    continue;
                }
                self.sync_status().await;
                return Ok((key, peer));
            }
            let host = endpoint.url.host.clone().ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing host for tcp connect")
            })?;
            let port = endpoint.url.port.ok_or_else(|| {
                Error::new(ErrorKind::InvalidInput, "missing port for tcp connect")
            })?;
            let addr = format!("{host}:{port}");
            match tcp_mux::connect_mux_peer_via_proxy_chain(identity.clone(), &addr, proxy_chain)
                .await
            {
                Ok(peer) => {
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    self.peers.lock().await.insert(key.clone(), peer.clone());
                    self.sync_status().await;
                    return Ok((key, peer));
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| Error::other("no tcp upstream endpoint succeeded")))
    }

    pub async fn invalidate(&self, key: &str) {
        self.peers.lock().await.remove(key);
        self.sync_status().await;
    }

    pub async fn snapshot_keys(&self) -> Vec<String> {
        let mut keys: Vec<_> = self.peers.lock().await.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub async fn prune_stale(
        &self,
        hub: &Arc<Mutex<SessionHub>>,
        registry: &Arc<Mutex<AgentRegistry>>,
    ) -> usize {
        let snapshot = self.peers.lock().await.clone();
        let mut removed = 0;
        for (key, peer) in snapshot {
            if self.invalidate_if_stale(&key, &peer, hub, registry).await {
                removed += 1;
            }
        }
        self.sync_status().await;
        removed
    }
}

#[derive(Clone)]
pub struct WsMuxUpstreamPool {
    peers: Arc<Mutex<HashMap<String, ws_mux::MuxWsPeer>>>,
    status: Option<(UpstreamPoolStatusMap, String)>,
}

impl Default for WsMuxUpstreamPool {
    fn default() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            status: None,
        }
    }
}

impl WsMuxUpstreamPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_status(status: UpstreamPoolStatusMap, handler: impl Into<String>) -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            status: Some((status, handler.into())),
        }
    }

    async fn sync_status(&self) {
        let Some((status, handler)) = self.status.as_ref() else {
            return;
        };
        publish_upstream_pool_status(status, handler, "ws_mux", self.snapshot_keys().await).await;
    }

    async fn invalidate_if_stale(
        &self,
        key: &str,
        peer: &ws_mux::MuxWsPeer,
        hub: &Arc<Mutex<SessionHub>>,
        registry: &Arc<Mutex<AgentRegistry>>,
    ) -> bool {
        let peer_id = &peer.session.remote.agent_id;
        let hub_active = hub.lock().await.is_active(peer_id);
        let registry_active = registry.lock().await.is_peer_active(peer_id);
        if hub_active && registry_active {
            return false;
        }
        self.peers.lock().await.remove(key);
        true
    }

    pub async fn acquire(
        &self,
        identity: AgentIdentity,
        endpoints: &[TunnelEndpoint],
        conn_policy: &ConnPolicy,
        hub: &Arc<Mutex<SessionHub>>,
        registry: &Arc<Mutex<AgentRegistry>>,
    ) -> Result<(String, ws_mux::MuxWsPeer), Error> {
        let ordered = order_endpoints(endpoints, conn_policy)?;
        let mut last_err = None;
        for endpoint in ordered {
            let key = endpoint.url.original.clone();
            if let Some(peer) = self.peers.lock().await.get(&key).cloned() {
                if self.invalidate_if_stale(&key, &peer, hub, registry).await {
                    continue;
                }
                self.sync_status().await;
                return Ok((key, peer));
            }
            match ws_mux::connect_mux_peer(identity.clone(), &endpoint.url.original).await {
                Ok(peer) => {
                    hub.lock().await.upsert(peer.session.clone());
                    registry.lock().await.upsert_peer(peer.session.clone());
                    self.peers.lock().await.insert(key.clone(), peer.clone());
                    self.sync_status().await;
                    return Ok((key, peer));
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| Error::other("no ws upstream endpoint succeeded")))
    }

    pub async fn invalidate(&self, key: &str) {
        self.peers.lock().await.remove(key);
        self.sync_status().await;
    }

    pub async fn snapshot_keys(&self) -> Vec<String> {
        let mut keys: Vec<_> = self.peers.lock().await.keys().cloned().collect();
        keys.sort();
        keys
    }

    pub async fn prune_stale(
        &self,
        hub: &Arc<Mutex<SessionHub>>,
        registry: &Arc<Mutex<AgentRegistry>>,
    ) -> usize {
        let snapshot = self.peers.lock().await.clone();
        let mut removed = 0;
        for (key, peer) in snapshot {
            if self.invalidate_if_stale(&key, &peer, hub, registry).await {
                removed += 1;
            }
        }
        self.sync_status().await;
        removed
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{timeout, Duration};

    use crate::{
        agent::identity::AgentIdentity,
        app::{
            config::{AgentIdentityConfig, ConnPolicy, TunnelEndpoint},
            runtime_orchestrator::RuntimeShared,
            runtime_status::RuntimeConfigSummary,
        },
        tunnel::tcp_mux::accept_mux_peer_on,
        utils::url::ParsedUrl,
    };

    use super::TcpMuxUpstreamPool;

    #[tokio::test]
    async fn tcp_upstream_pool_reuses_existing_mux_peer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-server".into()),
            key: None,
        });
        let shared = RuntimeShared::new(
            Vec::new(),
            &std::env::temp_dir(),
            RuntimeConfigSummary::default(),
        )
        .await;
        let pool = TcpMuxUpstreamPool::new();
        let endpoint = TunnelEndpoint {
            url: ParsedUrl::parse(&format!("tcp://{}", addr)).unwrap(),
        };
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let _peer = accept_mux_peer_on(server_identity, &listener)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let _first = pool
            .acquire(
                client_identity.clone(),
                std::slice::from_ref(&endpoint),
                &ConnPolicy::Fallback,
                &[],
                &shared.hub,
                &shared.registry,
            )
            .await
            .unwrap();

        timeout(
            Duration::from_millis(100),
            pool.acquire(
                client_identity,
                std::slice::from_ref(&endpoint),
                &ConnPolicy::Fallback,
                &[],
                &shared.hub,
                &shared.registry,
            ),
        )
        .await
        .unwrap()
        .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_upstream_pool_drops_stale_peer_before_reuse() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-server-stale".into()),
            key: None,
        });
        let shared = RuntimeShared::new(
            Vec::new(),
            &std::env::temp_dir(),
            RuntimeConfigSummary::default(),
        )
        .await;
        let pool = TcpMuxUpstreamPool::new();
        let endpoint = TunnelEndpoint {
            url: ParsedUrl::parse(&format!("tcp://{}", addr)).unwrap(),
        };
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-client-stale".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let _peer = accept_mux_peer_on(server_identity, &listener)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let (_key, peer) = pool
            .acquire(
                client_identity,
                std::slice::from_ref(&endpoint),
                &ConnPolicy::Fallback,
                &[],
                &shared.hub,
                &shared.registry,
            )
            .await
            .unwrap();
        let peer_id = peer.session.remote.agent_id.clone();
        shared.hub.lock().await.mark_closed(&peer_id);
        shared.registry.lock().await.mark_peer_closed(&peer_id);

        let stale_removed = pool
            .invalidate_if_stale(&endpoint.url.original, &peer, &shared.hub, &shared.registry)
            .await;
        assert!(stale_removed);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_upstream_pool_prune_stale_removes_closed_entries() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-server-prune".into()),
            key: None,
        });
        let shared = RuntimeShared::new(
            Vec::new(),
            &std::env::temp_dir(),
            RuntimeConfigSummary::default(),
        )
        .await;
        let pool = TcpMuxUpstreamPool::new();
        let endpoint = TunnelEndpoint {
            url: ParsedUrl::parse(&format!("tcp://{}", addr)).unwrap(),
        };
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-client-prune".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let _peer = accept_mux_peer_on(server_identity, &listener)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let (_key, peer) = pool
            .acquire(
                client_identity,
                std::slice::from_ref(&endpoint),
                &ConnPolicy::Fallback,
                &[],
                &shared.hub,
                &shared.registry,
            )
            .await
            .unwrap();
        let peer_id = peer.session.remote.agent_id.clone();
        shared.hub.lock().await.mark_closed(&peer_id);
        shared.registry.lock().await.mark_peer_closed(&peer_id);

        let removed = pool.prune_stale(&shared.hub, &shared.registry).await;
        assert_eq!(removed, 1);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_upstream_pool_failover_skips_unreachable_endpoint() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-failover-server".into()),
            key: None,
        });
        let shared = RuntimeShared::new(
            Vec::new(),
            &std::env::temp_dir(),
            RuntimeConfigSummary::default(),
        )
        .await;
        let pool = TcpMuxUpstreamPool::new();
        let bad = TunnelEndpoint {
            url: ParsedUrl::parse("tcp://127.0.0.1:1").unwrap(),
        };
        let good = TunnelEndpoint {
            url: ParsedUrl::parse(&format!("tcp://{}", addr)).unwrap(),
        };
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-failover-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let _peer = accept_mux_peer_on(server_identity, &listener)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let (_key, _peer) = pool
            .acquire(
                client_identity,
                &[bad, good],
                &ConnPolicy::Fallback,
                &[],
                &shared.hub,
                &shared.registry,
            )
            .await
            .unwrap();

        let keys = pool.snapshot_keys().await;
        assert_eq!(keys.len(), 1);
        assert!(keys[0].contains(&addr.port().to_string()));

        server.await.unwrap();
    }

    #[tokio::test]
    async fn tcp_upstream_pool_round_robin_policy_acquires_successfully() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-rr-server".into()),
            key: None,
        });
        let shared = RuntimeShared::new(
            Vec::new(),
            &std::env::temp_dir(),
            RuntimeConfigSummary::default(),
        )
        .await;
        let pool = TcpMuxUpstreamPool::new();
        let bad = TunnelEndpoint {
            url: ParsedUrl::parse("tcp://127.0.0.1:1").unwrap(),
        };
        let good = TunnelEndpoint {
            url: ParsedUrl::parse(&format!("tcp://{}", addr)).unwrap(),
        };
        let client_identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("pool-rr-client".into()),
            key: None,
        });

        let server = tokio::spawn(async move {
            let _peer = accept_mux_peer_on(server_identity, &listener)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let (_key, _peer) = pool
            .acquire(
                client_identity,
                &[bad, good],
                &ConnPolicy::RoundRobin,
                &[],
                &shared.hub,
                &shared.registry,
            )
            .await
            .unwrap();

        server.await.unwrap();
    }
}
