use std::ffi::{c_char, CStr, CString};

use crate::utils::url::ParsedUrl;

pub const FUSION_ABI_VERSION: u32 = 1;

#[no_mangle]
pub extern "C" fn fusion_abi_version() -> u32 {
    FUSION_ABI_VERSION
}

#[no_mangle]
pub extern "C" fn fusion_version_string() -> *mut c_char {
    CString::new(env!("CARGO_PKG_VERSION"))
        .expect("package version is valid CString")
        .into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn fusion_parse_url_json(input: *const c_char) -> *mut c_char {
    if input.is_null() {
        return std::ptr::null_mut();
    }
    let input = match CStr::from_ptr(input).to_str() {
        Ok(v) => v,
        Err(_) => return std::ptr::null_mut(),
    };
    let parsed = match ParsedUrl::parse(input) {
        Ok(v) => v,
        Err(_) => return std::ptr::null_mut(),
    };
    match serde_json::to_string(&parsed) {
        Ok(json) => CString::new(json)
            .ok()
            .map(CString::into_raw)
            .unwrap_or(std::ptr::null_mut()),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn fusion_string_free(ptr: *mut c_char) {
    if !ptr.is_null() {
        let _ = CString::from_raw(ptr);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::{
        fusion_abi_version, fusion_parse_url_json, fusion_string_free, fusion_version_string,
    };

    #[test]
    fn ffi_version_and_url_parse_work() {
        assert_eq!(fusion_abi_version(), 1);
        let version_ptr = fusion_version_string();
        assert!(!version_ptr.is_null());
        unsafe { fusion_string_free(version_ptr) };

        let input = CString::new("tcp://127.0.0.1:9000").unwrap();
        let parsed = unsafe { fusion_parse_url_json(input.as_ptr()) };
        assert!(!parsed.is_null());
        let json = unsafe { CStr::from_ptr(parsed).to_str().unwrap().to_string() };
        assert!(json.contains('"'));
        assert!(json.contains("tcp"));
        unsafe { fusion_string_free(parsed) };
    }
}
