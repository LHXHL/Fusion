use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StreamLifecycle {
    Opening,
    Active,
    Closing,
    Closed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamDescriptor {
    pub stream_id: u32,
    pub service: String,
    pub target: String,
    pub destination_agent_id: Option<String>,
}

impl StreamDescriptor {
    pub fn summary_line(&self) -> String {
        match &self.destination_agent_id {
            Some(dst) => format!(
                "stream.id={} service={} target={} dst={}",
                self.stream_id, self.service, self.target, dst
            ),
            None => format!(
                "stream.id={} service={} target={}",
                self.stream_id, self.service, self.target
            ),
        }
    }
}
