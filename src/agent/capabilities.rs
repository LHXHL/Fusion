use crate::protocol::message::TaskAction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Capability {
    TransportTcp,
    TransportWs,
    TransportUdp,
    TransportUnix,
    TransportMemory,
    TransportIcmp,
    TransportWg,
    TransportSimplexHttp,
    TransportSimplexOss,
    TransportSimplexDns,
    Shell,
    Screenshot,
    FileUpload,
    FileDownload,
    WhoAmI,
    PlatformOs(String),
    PlatformArch(String),
}

impl Capability {
    pub fn label(&self) -> String {
        match self {
            Self::TransportTcp => "transport:tcp".to_string(),
            Self::TransportWs => "transport:ws".to_string(),
            Self::TransportUdp => "transport:udp".to_string(),
            Self::TransportUnix => "transport:unix".to_string(),
            Self::TransportMemory => "transport:memory".to_string(),
            Self::TransportIcmp => "transport:icmp".to_string(),
            Self::TransportWg => "transport:wg".to_string(),
            Self::TransportSimplexHttp => "transport:simplex-http".to_string(),
            Self::TransportSimplexOss => "transport:simplex-oss".to_string(),
            Self::TransportSimplexDns => "transport:simplex-dns".to_string(),
            Self::Shell => "task:shell".to_string(),
            Self::Screenshot => "task:screenshot".to_string(),
            Self::FileUpload => "task:file-upload".to_string(),
            Self::FileDownload => "task:file-download".to_string(),
            Self::WhoAmI => "task:whoami".to_string(),
            Self::PlatformOs(os) => format!("os:{os}"),
            Self::PlatformArch(arch) => format!("arch:{arch}"),
        }
    }

    pub fn supports_task_action(&self, action: &TaskAction) -> bool {
        matches!(
            (self, action),
            (Self::Shell, TaskAction::Shell | TaskAction::InteractiveShell)
                | (Self::Screenshot, TaskAction::Screenshot)
                | (Self::FileUpload, TaskAction::FileUpload)
                | (Self::FileDownload, TaskAction::FileDownload)
        )
    }
}

#[derive(Debug, Clone)]
pub struct CapabilityRegistry {
    capabilities: Vec<Capability>,
}

impl CapabilityRegistry {
    pub fn new(capabilities: Vec<Capability>) -> Self {
        Self { capabilities }
    }

    pub fn default_for_platform(os: &str, arch: &str) -> Self {
        Self::new(vec![
            Capability::PlatformOs(os.to_string()),
            Capability::PlatformArch(arch.to_string()),
            Capability::TransportTcp,
            Capability::TransportWs,
            Capability::TransportUdp,
            Capability::TransportUnix,
            Capability::TransportMemory,
            Capability::TransportIcmp,
            Capability::TransportWg,
            Capability::TransportSimplexHttp,
            Capability::TransportSimplexOss,
            Capability::TransportSimplexDns,
            Capability::Shell,
            Capability::Screenshot,
            Capability::FileUpload,
            Capability::FileDownload,
        ])
    }

    pub fn labels(&self) -> Vec<String> {
        self.capabilities.iter().map(Capability::label).collect()
    }

    pub fn supports_task_action(&self, action: &TaskAction) -> bool {
        self.capabilities
            .iter()
            .any(|capability| capability.supports_task_action(action))
    }
}
