use std::{
    ffi::{c_char, CStr, CString},
    path::Path,
    ptr,
    sync::{Mutex, OnceLock},
};

use crate::{
    app::{
        config::{app_config_from_file, StatusScope, TaskRequestConfig},
        runtime_embed::{
            execute_task_request, runtime_status_json, spawn_runtime_from_config,
            stop_runtime_tasks, RunningRuntime,
        },
        runtime_status::RuntimeConfigSummary,
    },
    utils::url::ParsedUrl,
};
use fusion_logic::{filter_status_json, validate_config_toml_or_error_json, LOGIC_API_VERSION};

pub const FUSION_ABI_VERSION: u32 = 2;

const FUSION_OK: i32 = 0;
const FUSION_ERR_INVALID_ARGUMENT: i32 = -1;
const FUSION_ERR_RUNTIME: i32 = -2;
const FUSION_ERR_NOT_RUNNING: i32 = -3;
const FUSION_ERR_ALREADY_RUNNING: i32 = -4;

static LAST_FFI_ERROR: OnceLock<Mutex<String>> = OnceLock::new();

fn last_error_store() -> &'static Mutex<String> {
    LAST_FFI_ERROR.get_or_init(|| Mutex::new(String::new()))
}

fn set_last_error(message: impl Into<String>) {
    if let Ok(mut guard) = last_error_store().lock() {
        *guard = message.into();
    }
}

fn clear_last_error() {
    if let Ok(mut guard) = last_error_store().lock() {
        guard.clear();
    }
}

fn c_string_or_null(value: String) -> *mut c_char {
    CString::new(value)
        .ok()
        .map(CString::into_raw)
        .unwrap_or(ptr::null_mut())
}

fn parse_cstr<'a>(input: *const c_char) -> Result<&'a str, i32> {
    if input.is_null() {
        set_last_error("null pointer argument");
        return Err(FUSION_ERR_INVALID_ARGUMENT);
    }
    unsafe { CStr::from_ptr(input) }.to_str().map_err(|_| {
        set_last_error("invalid UTF-8 in C string argument");
        FUSION_ERR_INVALID_ARGUMENT
    })
}

fn parse_status_scope(scope: &str) -> Result<StatusScope, i32> {
    match scope {
        "all" | "All" => Ok(StatusScope::All),
        "peers" | "Peers" => Ok(StatusScope::Peers),
        "routes" | "Routes" => Ok(StatusScope::Routes),
        "streams" | "Streams" => Ok(StatusScope::Streams),
        other => {
            set_last_error(format!("unknown status scope `{other}`"));
            Err(FUSION_ERR_INVALID_ARGUMENT)
        }
    }
}

#[derive(serde::Deserialize)]
struct FfiTaskRequestInput {
    action: String,
    #[serde(default)]
    args: Vec<String>,
    data_hex: Option<String>,
    save_path: Option<String>,
    local_path: Option<String>,
    target_agent_id: Option<String>,
}

fn parse_task_action(action: &str) -> Result<crate::protocol::message::TaskAction, i32> {
    use crate::protocol::message::TaskAction;
    match action {
        "shell" => Ok(TaskAction::Shell),
        "interactive-shell" | "interactive_shell" | "interactive" => Ok(TaskAction::InteractiveShell),
        "screenshot" => Ok(TaskAction::Screenshot),
        "download" | "file-download" => Ok(TaskAction::FileDownload),
        "upload" | "file-upload" => Ok(TaskAction::FileUpload),
        other => {
            set_last_error(format!("unknown task action `{other}`"));
            Err(FUSION_ERR_INVALID_ARGUMENT)
        }
    }
}

pub struct FusionRuntime {
    tokio: tokio::runtime::Runtime,
    config: Option<crate::app::config::AppConfig>,
    running: Option<RunningRuntime>,
}

#[no_mangle]
pub extern "C" fn fusion_logic_api_version() -> u32 {
    LOGIC_API_VERSION
}

#[no_mangle]
pub unsafe extern "C" fn fusion_validate_config_toml_json(input: *const c_char) -> *mut c_char {
    clear_last_error();
    let input = match parse_cstr(input) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    c_string_or_null(validate_config_toml_or_error_json(input))
}

#[no_mangle]
pub unsafe extern "C" fn fusion_filter_status_json(
    snapshot_json: *const c_char,
    scope: *const c_char,
) -> *mut c_char {
    clear_last_error();
    let snapshot_json = match parse_cstr(snapshot_json) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    let scope = match parse_cstr(scope) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    match filter_status_json(snapshot_json, scope) {
        Ok(json) => c_string_or_null(json),
        Err(err) => {
            set_last_error(err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn fusion_abi_version() -> u32 {
    FUSION_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn fusion_version_string() -> *mut c_char {
    c_string_or_null(env!("CARGO_PKG_VERSION").to_string())
}

#[no_mangle]
pub unsafe extern "C" fn fusion_last_error() -> *mut c_char {
    let message = last_error_store()
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    if message.is_empty() {
        return ptr::null_mut();
    }
    c_string_or_null(message)
}

#[no_mangle]
pub extern "C" fn fusion_clear_last_error() {
    clear_last_error();
}

#[no_mangle]
pub unsafe extern "C" fn fusion_parse_url_json(input: *const c_char) -> *mut c_char {
    clear_last_error();
    let input = match parse_cstr(input) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    let parsed = match ParsedUrl::parse(input) {
        Ok(value) => value,
        Err(err) => {
            set_last_error(err.to_string());
            return ptr::null_mut();
        }
    };
    match serde_json::to_string(&parsed) {
        Ok(json) => c_string_or_null(json),
        Err(err) => {
            set_last_error(err.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = CString::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn fusion_runtime_create() -> *mut FusionRuntime {
    clear_last_error();
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("fusion-ffi")
        .build()
    {
        Ok(tokio) => Box::into_raw(Box::new(FusionRuntime {
            tokio,
            config: None,
            running: None,
        })),
        Err(err) => {
            set_last_error(format!("failed to create tokio runtime: {err}"));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_runtime_destroy(handle: *mut FusionRuntime) {
    if handle.is_null() {
        return;
    }
    let mut runtime = Box::from_raw(handle);
    if let Some(running) = runtime.running.take() {
        stop_runtime_tasks(running.tasks);
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_runtime_load_config_file(
    handle: *mut FusionRuntime,
    path: *const c_char,
) -> i32 {
    clear_last_error();
    if handle.is_null() {
        set_last_error("null runtime handle");
        return FUSION_ERR_INVALID_ARGUMENT;
    }
    let path = match parse_cstr(path) {
        Ok(value) => value,
        Err(code) => return code,
    };
    let runtime = &mut *handle;
    if runtime.running.is_some() {
        set_last_error("cannot load config while runtime is running");
        return FUSION_ERR_ALREADY_RUNNING;
    }
    match app_config_from_file(Path::new(path)) {
        Ok(config) => {
            runtime.config = Some(config);
            FUSION_OK
        }
        Err(err) => {
            set_last_error(err.to_string());
            FUSION_ERR_RUNTIME
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_runtime_start(handle: *mut FusionRuntime) -> i32 {
    clear_last_error();
    if handle.is_null() {
        set_last_error("null runtime handle");
        return FUSION_ERR_INVALID_ARGUMENT;
    }
    let runtime = &mut *handle;
    if runtime.running.is_some() {
        set_last_error("runtime is already running");
        return FUSION_ERR_ALREADY_RUNNING;
    }
    let Some(config) = runtime.config.clone() else {
        set_last_error("runtime config is not loaded");
        return FUSION_ERR_INVALID_ARGUMENT;
    };
    match runtime.tokio.block_on(spawn_runtime_from_config(config)) {
        Ok(running) => {
            runtime.running = Some(running);
            FUSION_OK
        }
        Err(err) => {
            set_last_error(err.to_string());
            FUSION_ERR_RUNTIME
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_runtime_stop(handle: *mut FusionRuntime) -> i32 {
    clear_last_error();
    if handle.is_null() {
        set_last_error("null runtime handle");
        return FUSION_ERR_INVALID_ARGUMENT;
    }
    let runtime = &mut *handle;
    let Some(running) = runtime.running.take() else {
        set_last_error("runtime is not running");
        return FUSION_ERR_NOT_RUNNING;
    };
    stop_runtime_tasks(running.tasks);
    FUSION_OK
}

#[no_mangle]
pub unsafe extern "C" fn fusion_runtime_status_json(
    handle: *mut FusionRuntime,
    scope: *const c_char,
) -> *mut c_char {
    clear_last_error();
    if handle.is_null() {
        set_last_error("null runtime handle");
        return ptr::null_mut();
    }
    let scope = match parse_cstr(scope) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    let scope = match parse_status_scope(scope) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    let runtime = &*handle;
    let Some(config) = runtime.config.as_ref() else {
        set_last_error("runtime config is not loaded");
        return ptr::null_mut();
    };
    let shared = runtime
        .running
        .as_ref()
        .map(|running| running.shared.as_ref());
    let config_summary = runtime
        .running
        .as_ref()
        .map(|running| running.config_summary.clone())
        .unwrap_or_else(|| RuntimeConfigSummary::from_app_config(config));
    match runtime.tokio.block_on(runtime_status_json(
        &config.data_dir,
        shared,
        &config_summary,
        scope,
    )) {
        Ok(json) => c_string_or_null(json),
        Err(err) => {
            set_last_error(err.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_runtime_task_request_json(
    handle: *mut FusionRuntime,
    request_json: *const c_char,
) -> *mut c_char {
    clear_last_error();
    if handle.is_null() {
        set_last_error("null runtime handle");
        return ptr::null_mut();
    }
    let request_json = match parse_cstr(request_json) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    let runtime = &*handle;
    let Some(config) = runtime.config.clone() else {
        set_last_error("runtime config is not loaded");
        return ptr::null_mut();
    };
    if config.connects.is_empty() {
        set_last_error("runtime config has no connect endpoints for task request");
        return ptr::null_mut();
    }
    let input: FfiTaskRequestInput = match serde_json::from_str(request_json) {
        Ok(value) => value,
        Err(err) => {
            set_last_error(format!("invalid task request json: {err}"));
            return ptr::null_mut();
        }
    };
    let action = match parse_task_action(&input.action) {
        Ok(value) => value,
        Err(_) => return ptr::null_mut(),
    };
    let task = TaskRequestConfig {
        action,
        args: input.args,
        data_hex: input.data_hex,
        save_path: input.save_path.map(std::path::PathBuf::from),
        local_path: input.local_path.map(std::path::PathBuf::from),
        target_agent_id: input.target_agent_id,
    };
    match runtime.tokio.block_on(execute_task_request(&config, task)) {
        Ok(result) => match serde_json::to_string(&result) {
            Ok(json) => c_string_or_null(json),
            Err(err) => {
                set_last_error(err.to_string());
                ptr::null_mut()
            }
        },
        Err(err) => {
            set_last_error(err.to_string());
            ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::{
        fusion_abi_version, fusion_clear_last_error, fusion_filter_status_json,
        fusion_last_error, fusion_logic_api_version, fusion_parse_url_json,
        fusion_runtime_create, fusion_runtime_destroy, fusion_runtime_load_config_file,
        fusion_string_free, fusion_validate_config_toml_json, fusion_version_string,
        FUSION_ERR_RUNTIME, FUSION_OK,
    };

    #[test]
    fn ffi_version_and_url_parse_work() {
        assert_eq!(fusion_abi_version(), 2);
        let version_ptr = fusion_version_string();
        assert!(!version_ptr.is_null());
        unsafe { fusion_string_free(version_ptr) };

        let input = CString::new("tcp://127.0.0.1:9000").unwrap();
        let parsed = unsafe { fusion_parse_url_json(input.as_ptr()) };
        assert!(!parsed.is_null());
        let json = unsafe { CStr::from_ptr(parsed).to_str().unwrap().to_string() };
        assert!(json.contains("tcp"));
        unsafe { fusion_string_free(parsed) };
    }

    #[test]
    fn ffi_logic_api_validate_and_filter_work() {
        assert_eq!(fusion_logic_api_version(), 1);

        let toml = CString::new(
            r#"
connect = ["tcp://127.0.0.1:9000"]
"#,
        )
        .unwrap();
        let validated = unsafe { fusion_validate_config_toml_json(toml.as_ptr()) };
        assert!(!validated.is_null());
        let json = unsafe { CStr::from_ptr(validated).to_str().unwrap() };
        assert!(json.contains("\"connect_count\":1"));
        unsafe { fusion_string_free(validated) };

        let snapshot = CString::new(
            r#"{"generated_at_unix":1,"peers":[{"agent_id":"a"}],"routes":[]}"#,
        )
        .unwrap();
        let scope = CString::new("peers").unwrap();
        let filtered = unsafe { fusion_filter_status_json(snapshot.as_ptr(), scope.as_ptr()) };
        assert!(!filtered.is_null());
        let json = unsafe { CStr::from_ptr(filtered).to_str().unwrap() };
        assert!(json.contains("\"peers\""));
        assert!(!json.contains("\"routes\""));
        unsafe { fusion_string_free(filtered) };
    }

    #[test]
    fn ffi_runtime_load_config_reports_missing_file() {
        fusion_clear_last_error();
        let handle = fusion_runtime_create();
        assert!(!handle.is_null());
        let path = CString::new("/tmp/fusion-does-not-exist.toml").unwrap();
        let code = unsafe { fusion_runtime_load_config_file(handle, path.as_ptr()) };
        assert_eq!(code, FUSION_ERR_RUNTIME);
        let err_ptr = unsafe { fusion_last_error() };
        assert!(!err_ptr.is_null());
        let err = unsafe { CStr::from_ptr(err_ptr).to_str().unwrap().to_string() };
        assert!(err.contains("failed to read config file"));
        unsafe { fusion_string_free(err_ptr) };
        unsafe { fusion_runtime_destroy(handle) };
    }

    #[test]
    fn ffi_runtime_load_config_accepts_toml_file() {
        let dir = std::env::temp_dir().join(format!("fusion-ffi-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fusion.toml");
        std::fs::write(
            &path,
            r#"
connect = ["tcp://127.0.0.1:9000"]
data_dir = ".fusion-test"
"#,
        )
        .unwrap();

        fusion_clear_last_error();
        let handle = fusion_runtime_create();
        let path_c = CString::new(path.to_string_lossy().as_bytes()).unwrap();
        let code = unsafe { fusion_runtime_load_config_file(handle, path_c.as_ptr()) };
        assert_eq!(code, FUSION_OK);

        unsafe { fusion_runtime_destroy(handle) };
        let _ = std::fs::remove_dir_all(dir);
    }
}
