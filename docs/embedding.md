# Fusion 嵌入指南

本文说明如何通过 C ABI 将 Fusion 作为原生库嵌入外部宿主程序。

## 构建库

```bash
cargo build --release
```

产物：

- macOS/Linux：`target/release/libfusion.dylib` 或 `libfusion.so`
- 静态库：`target/release/libfusion.a`

头文件：[`include/fusion.h`](../include/fusion.h)

## ABI 版本

调用任何其他 API 之前，应先调用 `fusion_abi_version()`。当前版本为 **2**。

纯逻辑 API（URL 解析、配置校验、status 过滤）另有 `fusion_logic_api_version()`（当前 **1**），与运行时 ABI 独立。策略见 [`abi-stability.md`](abi-stability.md)。

所有返回 `char *` 的函数，都必须用 `fusion_string_free()` 释放内存。

## 纯逻辑 API（无需 runtime）

可在不创建 `FusionRuntime` 的情况下调用：

```c
char *parsed = fusion_parse_url_json("tcp://127.0.0.1:9000");
char *summary = fusion_validate_config_toml_json("connect = [\"tcp://127.0.0.1:9000\"]\n");
char *peers = fusion_filter_status_json(status_json, "peers");
fusion_string_free(parsed);
fusion_string_free(summary);
fusion_string_free(peers);
```

WASM 构建与 JS 导出见 [`release.md`](release.md) §3。

## 错误处理

返回 `int32_t` 的函数使用以下错误码：

| 错误码 | 含义 |
|--------|------|
| `FUSION_OK` | 成功 |
| `FUSION_ERR_INVALID_ARGUMENT` | 空指针或无效输入 |
| `FUSION_ERR_RUNTIME` | I/O、配置或网络失败 |
| `FUSION_ERR_NOT_RUNNING` | 在未运行时调用 stop |
| `FUSION_ERR_ALREADY_RUNNING` | 重复 start，或在运行中重新加载配置 |

失败后，可调用 `fusion_last_error()` 获取可读错误信息。

## 典型宿主流程

```c
FusionRuntime *rt = fusion_runtime_create();
fusion_runtime_load_config_file(rt, "fusion.toml");
fusion_runtime_start(rt);

char *status = fusion_runtime_status_json(rt, "all");
/* 使用 status */
fusion_string_free(status);

fusion_runtime_stop(rt);
fusion_runtime_destroy(rt);
```

## 一次性 task 请求

确保已加载的配置中包含 `connect = [...]`，然后传入如下 JSON：

```json
{
  "action": "shell",
  "args": ["whoami"],
  "target_agent_id": null
}
```

```c
char *result = fusion_runtime_task_request_json(rt, request_json);
fusion_string_free(result);
```

支持的 action：`shell`、`screenshot`、`download`、`upload`。

## Memory 隧道（`memory://`）

同进程内两个 Agent 经内存 duplex 互联，无需 TCP/Unix socket。实现：[`src/tunnel/memory.rs`](../src/tunnel/memory.rs)。

| 能力 | 说明 |
|------|------|
| URL | `memory://MESH_NAME`（`-s` listen / `-c` connect 同名） |
| 用途 | 集成测试、同进程双 Agent、嵌入宿主内多 runtime |

**限制：** 不能跨进程；无 rem 式 `MemoryRead`/`MemoryWrite` 细粒度 C API。Fusion 提供 **mesh 级 memory 隧道**（整会话 Hello/Frame 路径），非裸内存读写 FFI。

同进程双 Agent 示例：

```toml
# agent-a.toml
listens = ["memory://mesh-a"]

# agent-b.toml
connects = ["memory://mesh-a"]
```

两个 `FusionRuntime` handle（或两个线程各跑 `fusion_runtime_start`）即可互通。一般无需额外 Memory FFI，在配置里写 `memory://` 即可。

Fusion **不**兼容 rem 旧 `InitDialer` / `RemDial` 符号。

## WASM

**W1 纯逻辑 API 已交付**（`crates/fusion-logic`）：URL 解析、TOML 校验、status JSON 过滤。Native C 与 wasm-bindgen 导出见 [`release.md`](release.md) §3。

**完整 Agent runtime 不在 WASM 范围**（无 tokio 网络栈、无 TCP/relay/mux）。生产嵌入请使用本页 C ABI / staticlib。

## 示例

- C：[`examples/c_host/main.c`](../examples/c_host/main.c)
- Python：[`examples/python_host/demo.py`](../examples/python_host/demo.py)
