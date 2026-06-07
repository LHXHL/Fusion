use wasm_bindgen::prelude::*;

use crate::{filter_status_json, logic_api_version, parse_url_json, validate_config_toml_or_error_json};

#[wasm_bindgen(js_name = fusionLogicApiVersion)]
pub fn wasm_logic_api_version() -> u32 {
    logic_api_version()
}

#[wasm_bindgen(js_name = fusionLogicParseUrl)]
pub fn wasm_parse_url(input: &str) -> Result<String, JsValue> {
    parse_url_json(input).map_err(|err| JsValue::from_str(&err))
}

#[wasm_bindgen(js_name = fusionLogicValidateConfigToml)]
pub fn wasm_validate_config_toml(input: &str) -> String {
    validate_config_toml_or_error_json(input)
}

#[wasm_bindgen(js_name = fusionLogicFilterStatusJson)]
pub fn wasm_filter_status_json(input: &str, scope: &str) -> Result<String, JsValue> {
    filter_status_json(input, scope).map_err(|err| JsValue::from_str(&err))
}
