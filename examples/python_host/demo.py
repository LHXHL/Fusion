#!/usr/bin/env python3
"""Minimal Fusion C ABI demo via ctypes."""

from __future__ import annotations

import ctypes
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load_library() -> ctypes.CDLL:
    target = os.environ.get("FUSION_LIB")
    if target:
        return ctypes.CDLL(target)

    release_dir = ROOT / "target" / "release"
    candidates = [
        release_dir / "libfusion.dylib",
        release_dir / "libfusion.so",
        release_dir / "fusion.dll",
    ]
    for candidate in candidates:
        if candidate.exists():
            return ctypes.CDLL(str(candidate))

    raise FileNotFoundError(
        "libfusion not found; run `cargo build --release` or set FUSION_LIB"
    )


def owned_c_string(lib: ctypes.CDLL, fn_name: str):
    fn = getattr(lib, fn_name)
    fn.restype = ctypes.c_void_p
    return fn


def read_and_free(lib: ctypes.CDLL, ptr) -> str:
    if not ptr:
        return ""
    text = ctypes.string_at(ptr).decode()
    lib.fusion_string_free(ptr)
    return text


def main() -> int:
    lib = load_library()

    lib.fusion_abi_version.restype = ctypes.c_uint32
    lib.fusion_string_free.argtypes = [ctypes.c_void_p]
    lib.fusion_runtime_create.restype = ctypes.c_void_p
    lib.fusion_runtime_destroy.argtypes = [ctypes.c_void_p]
    lib.fusion_runtime_load_config_file.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.fusion_runtime_load_config_file.restype = ctypes.c_int32
    lib.fusion_runtime_status_json.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    lib.fusion_runtime_status_json.restype = ctypes.c_void_p

    version_fn = owned_c_string(lib, "fusion_version_string")
    parse_fn = owned_c_string(lib, "fusion_parse_url_json")
    parse_fn.argtypes = [ctypes.c_char_p]
    last_error_fn = owned_c_string(lib, "fusion_last_error")

    print("abi_version", lib.fusion_abi_version())

    version = read_and_free(lib, version_fn())
    print("version", version)

    parsed = read_and_free(lib, parse_fn(b"tcp://127.0.0.1:9000"))
    print("parsed", json.loads(parsed))

    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    if not config_path:
        print("hint: pass fusion.toml to test runtime load/status")
        return 0

    runtime = lib.fusion_runtime_create()
    if not runtime:
        err = read_and_free(lib, last_error_fn())
        print("runtime_create_error", err)
        return 1

    code = lib.fusion_runtime_load_config_file(runtime, config_path.encode())
    print("load_config_code", code)
    if code != 0:
        err = read_and_free(lib, last_error_fn())
        print("load_config_error", err)
        lib.fusion_runtime_destroy(runtime)
        return 1

    status = read_and_free(lib, lib.fusion_runtime_status_json(runtime, b"all"))
    if status:
        print("status", json.dumps(json.loads(status), indent=2))
    else:
        err = read_and_free(lib, last_error_fn())
        print("status_error", err)

    lib.fusion_runtime_destroy(runtime)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
