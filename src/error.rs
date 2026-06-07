use std::{
    collections::VecDeque,
    fmt, io,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    WrapperEncryptFailed,
    WrapperEnvelopeDecodeFailed,
    WrapperMissingEncryption,
    WrapperDecryptFailed,
    WrapperCompressionFailed,
    WrapperMissingCompression,
    WrapperPaddingTooLarge,
    WrapperMissingPadding,
    WrapperPaddingTruncated,
    WrapperPaddingLengthInvalid,
    TlsMissingParameter,
    TlsReadFailed,
    TlsInvalidCertificate,
    TlsInvalidPrivateKey,
    TlsInvalidClientCa,
    TlsInvalidClientIdentity,
    TlsBuildConnectorFailed,
    RouteNoRoute,
    RouteRejected,
    RoutePruned,
    RouteSwitched,
    RuntimeOperationFailed,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WrapperEncryptFailed => "wrapper.encrypt_failed",
            Self::WrapperEnvelopeDecodeFailed => "wrapper.envelope_decode_failed",
            Self::WrapperMissingEncryption => "wrapper.missing_encryption",
            Self::WrapperDecryptFailed => "wrapper.decrypt_failed",
            Self::WrapperCompressionFailed => "wrapper.compression_failed",
            Self::WrapperMissingCompression => "wrapper.missing_compression",
            Self::WrapperPaddingTooLarge => "wrapper.padding_too_large",
            Self::WrapperMissingPadding => "wrapper.missing_padding",
            Self::WrapperPaddingTruncated => "wrapper.padding_truncated",
            Self::WrapperPaddingLengthInvalid => "wrapper.padding_length_invalid",
            Self::TlsMissingParameter => "tls.missing_parameter",
            Self::TlsReadFailed => "tls.read_failed",
            Self::TlsInvalidCertificate => "tls.invalid_certificate",
            Self::TlsInvalidPrivateKey => "tls.invalid_private_key",
            Self::TlsInvalidClientCa => "tls.invalid_client_ca",
            Self::TlsInvalidClientIdentity => "tls.invalid_client_identity",
            Self::TlsBuildConnectorFailed => "tls.build_connector_failed",
            Self::RouteNoRoute => "route.no_route",
            Self::RouteRejected => "route.rejected",
            Self::RoutePruned => "route.pruned",
            Self::RouteSwitched => "route.switched",
            Self::RuntimeOperationFailed => "runtime.operation_failed",
        }
    }

    pub fn component(self) -> &'static str {
        self.as_str()
            .split_once('.')
            .map(|(head, _)| head)
            .unwrap_or("runtime")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FusionError {
    pub code: &'static str,
    pub component: &'static str,
    pub message: String,
    pub retryable: bool,
    pub details: Option<String>,
}

impl FusionError {
    pub fn new(
        code: ErrorCode,
        message: impl Into<String>,
        retryable: bool,
        details: Option<String>,
    ) -> Self {
        Self {
            code: code.as_str(),
            component: code.component(),
            message: message.into(),
            retryable,
            details,
        }
    }
}

impl fmt::Display for FusionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "code={} component={} retryable={} message={}",
            self.code, self.component, self.retryable, self.message
        )?;
        if let Some(details) = &self.details {
            write!(f, " details={details}")?;
        }
        Ok(())
    }
}

impl std::error::Error for FusionError {}

pub fn coded_io_error(
    kind: io::ErrorKind,
    code: ErrorCode,
    message: impl Into<String>,
    retryable: bool,
    details: Option<String>,
) -> io::Error {
    io::Error::new(kind, FusionError::new(code, message, retryable, details))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedError {
    pub at_unix: i64,
    pub code: String,
    pub component: String,
    pub message: String,
    pub retryable: bool,
    pub details: Option<String>,
    pub context: Option<String>,
}

impl RecordedError {
    pub fn summary_line(&self) -> String {
        format!(
            "recent.error at={} code={} component={} retryable={} message={} context={} details={}",
            self.at_unix,
            self.code,
            self.component,
            self.retryable,
            self.message,
            self.context.as_deref().unwrap_or("-"),
            self.details.as_deref().unwrap_or("-"),
        )
    }
}

#[derive(Debug, Default)]
struct RecentErrorLog {
    entries: VecDeque<RecordedError>,
    capacity: usize,
}

impl RecentErrorLog {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            capacity,
        }
    }

    fn push(&mut self, entry: RecordedError) {
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    fn snapshot(&self) -> Vec<RecordedError> {
        self.entries.iter().cloned().collect()
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

static RECENT_ERRORS: OnceLock<Mutex<RecentErrorLog>> = OnceLock::new();

const DEFAULT_RECENT_ERROR_CAPACITY: usize = 32;

fn recent_error_log() -> &'static Mutex<RecentErrorLog> {
    RECENT_ERRORS
        .get_or_init(|| Mutex::new(RecentErrorLog::with_capacity(DEFAULT_RECENT_ERROR_CAPACITY)))
}

fn unix_now_i64() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

pub fn record_error(
    code: ErrorCode,
    message: impl Into<String>,
    retryable: bool,
    details: Option<String>,
    context: Option<String>,
) {
    let entry = RecordedError {
        at_unix: unix_now_i64(),
        code: code.as_str().to_string(),
        component: code.component().to_string(),
        message: message.into(),
        retryable,
        details,
        context,
    };
    if let Ok(mut guard) = recent_error_log().lock() {
        guard.push(entry);
    }
}

pub fn record_io_error(context: &str, err: &io::Error) {
    if let Some(inner) = err
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<FusionError>())
    {
        record_error(
            ErrorCode::from_str(&inner.code).unwrap_or(ErrorCode::RuntimeOperationFailed),
            inner.message.clone(),
            inner.retryable,
            inner.details.clone(),
            Some(context.to_string()),
        );
        return;
    }
    record_error(
        ErrorCode::RuntimeOperationFailed,
        err.to_string(),
        false,
        None,
        Some(context.to_string()),
    );
}

impl ErrorCode {
    fn from_str(code: &str) -> Option<Self> {
        Some(match code {
            "wrapper.encrypt_failed" => Self::WrapperEncryptFailed,
            "wrapper.envelope_decode_failed" => Self::WrapperEnvelopeDecodeFailed,
            "wrapper.missing_encryption" => Self::WrapperMissingEncryption,
            "wrapper.decrypt_failed" => Self::WrapperDecryptFailed,
            "wrapper.compression_failed" => Self::WrapperCompressionFailed,
            "wrapper.missing_compression" => Self::WrapperMissingCompression,
            "wrapper.padding_too_large" => Self::WrapperPaddingTooLarge,
            "wrapper.missing_padding" => Self::WrapperMissingPadding,
            "wrapper.padding_truncated" => Self::WrapperPaddingTruncated,
            "wrapper.padding_length_invalid" => Self::WrapperPaddingLengthInvalid,
            "tls.missing_parameter" => Self::TlsMissingParameter,
            "tls.read_failed" => Self::TlsReadFailed,
            "tls.invalid_certificate" => Self::TlsInvalidCertificate,
            "tls.invalid_private_key" => Self::TlsInvalidPrivateKey,
            "tls.invalid_client_ca" => Self::TlsInvalidClientCa,
            "tls.invalid_client_identity" => Self::TlsInvalidClientIdentity,
            "tls.build_connector_failed" => Self::TlsBuildConnectorFailed,
            "route.no_route" => Self::RouteNoRoute,
            "route.rejected" => Self::RouteRejected,
            "route.pruned" => Self::RoutePruned,
            "route.switched" => Self::RouteSwitched,
            "runtime.operation_failed" => Self::RuntimeOperationFailed,
            _ => return None,
        })
    }
}

pub fn recent_errors_snapshot() -> Vec<RecordedError> {
    recent_error_log()
        .lock()
        .map(|guard| guard.snapshot())
        .unwrap_or_default()
}

pub fn clear_recent_errors_for_tests() {
    if let Ok(mut guard) = recent_error_log().lock() {
        guard.clear();
    }
}

#[cfg(test)]
static RECENT_ERROR_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
pub fn recent_error_test_guard() -> std::sync::MutexGuard<'static, ()> {
    RECENT_ERROR_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn report_relay_no_route(destination_agent_id: &str) {
    record_error(
        ErrorCode::RouteNoRoute,
        format!("no route to destination {destination_agent_id}"),
        false,
        None,
        Some(format!("destination={destination_agent_id}")),
    );
    eprintln!("relay.drop.no_route dst={destination_agent_id}");
}

pub fn report_route_pruned(count: usize) {
    if count == 0 {
        return;
    }
    record_error(
        ErrorCode::RoutePruned,
        format!("removed {count} stale route(s)"),
        false,
        Some(format!("count={count}")),
        Some("registry".to_string()),
    );
    eprintln!("registry.route_pruned={count}");
}

pub fn report_route_rejected(origin_agent_id: &str, source_peer: &str, reason: &str) {
    record_error(
        ErrorCode::RouteRejected,
        format!("route announcement rejected for {origin_agent_id}"),
        false,
        Some(format!("source_peer={source_peer} reason={reason}")),
        Some(format!("origin={origin_agent_id}")),
    );
    eprintln!(
        "registry.route_rejected origin={origin_agent_id} source_peer={source_peer} reason={reason}"
    );
}

pub fn report_route_switched(
    destination_agent_id: &str,
    previous_next_hop: &str,
    next_hop_agent_id: &str,
    selection_reason: &str,
) {
    record_error(
        ErrorCode::RouteSwitched,
        format!("route to {destination_agent_id} switched next hop"),
        false,
        Some(format!(
            "previous_next_hop={previous_next_hop} next_hop={next_hop_agent_id} reason={selection_reason}"
        )),
        Some(format!("destination={destination_agent_id}")),
    );
    eprintln!(
        "registry.route_switched destination={destination_agent_id} previous_next_hop={previous_next_hop} next_hop={next_hop_agent_id} reason={selection_reason}"
    );
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use super::{
        clear_recent_errors_for_tests, coded_io_error, recent_error_test_guard,
        recent_errors_snapshot, record_error, report_route_pruned, report_route_switched,
        ErrorCode,
    };

    #[test]
    fn coded_io_error_has_operator_facing_fields() {
        let err = coded_io_error(
            ErrorKind::InvalidInput,
            ErrorCode::TlsMissingParameter,
            "wss requires tls-cert",
            false,
            Some("missing query parameter `tls-cert`".to_string()),
        );
        let rendered = err.to_string();
        assert!(rendered.contains("code=tls.missing_parameter"));
        assert!(rendered.contains("component=tls"));
        assert!(rendered.contains("retryable=false"));
        assert!(rendered.contains("wss requires tls-cert"));
    }

    #[test]
    fn recent_error_log_keeps_latest_entries() {
        let _guard = recent_error_test_guard();
        clear_recent_errors_for_tests();
        for idx in 0..40 {
            record_error(
                ErrorCode::RouteNoRoute,
                format!("missing route {idx}"),
                false,
                None,
                None,
            );
        }
        let snapshot = recent_errors_snapshot();
        assert_eq!(snapshot.len(), 32);
        assert!(snapshot.last().unwrap().message.contains("39"));
    }

    #[test]
    fn route_event_helpers_record_recent_errors() {
        let _guard = recent_error_test_guard();
        clear_recent_errors_for_tests();
        report_route_pruned(2);
        report_route_switched("peer-z", "peer-b", "peer-c", "prefer_active_next_hop");
        let snapshot = recent_errors_snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0].code, "route.pruned");
        assert_eq!(snapshot[1].code, "route.switched");
    }
}
