use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    agent::capabilities::CapabilityRegistry,
    app::config::AgentIdentityConfig,
};
use crate::utils::random::random_name;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentIdentity {
    pub id: String,
    pub name: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
}

impl AgentIdentity {
    pub fn from_config(config: &AgentIdentityConfig) -> Self {
        let hostname = detect_hostname();
        let name = config.name.clone().unwrap_or_else(|| random_name("agent"));
        let id = stable_agent_id(&hostname, Some(&name));

        Self {
            id,
            name,
            hostname,
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
        }
    }

    pub fn capability_labels(&self) -> Vec<String> {
        CapabilityRegistry::default_for_platform(&self.os, &self.arch).labels()
    }
}

fn detect_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "unknown-host".to_string())
}

fn stable_agent_id(hostname: &str, explicit_name: Option<&str>) -> String {
    let base = if let Some(name) = explicit_name {
        format!("{}::{}", hostname, name)
    } else {
        mac_address::get_mac_address()
            .ok()
            .flatten()
            .map(|mac| mac.to_string())
            .unwrap_or_else(|| hostname.to_string())
    };

    Uuid::new_v5(&Uuid::NAMESPACE_DNS, base.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::AgentIdentity;
    use crate::app::config::AgentIdentityConfig;

    #[test]
    fn identity_from_config_respects_name_override() {
        let identity = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("node-a".to_string()),
            key: None,
        });

        assert_eq!(identity.name, "node-a");
        assert!(!identity.id.is_empty());
    }

    #[test]
    fn different_names_produce_different_ids() {
        let a = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("node-a".to_string()),
            key: None,
        });
        let b = AgentIdentity::from_config(&AgentIdentityConfig {
            name: Some("node-b".to_string()),
            key: None,
        });

        assert_ne!(a.id, b.id);
    }
}
