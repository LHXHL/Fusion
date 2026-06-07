pub mod config;
pub mod url;

pub use config::{
    filter_status_json, validate_config_toml, validate_config_toml_or_error_json,
    ConfigValidationSummary, LOGIC_API_VERSION,
};
pub use url::{parse_url_json, ParsedUrl};

#[cfg(feature = "wasm")]
mod wasm;

pub fn logic_api_version() -> u32 {
    LOGIC_API_VERSION
}

pub fn package_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
