# C ABI 稳定化策略

Fusion 通过 C ABI 向宿主（C/C++/Python ctypes 等）暴露能力。本文定义 **ABI 版本**、**语义化版本** 与破坏性变更纪律。

相关头文件：[`include/fusion.h`](../include/fusion.h)  
嵌入指南：[`embedding.md`](embedding.md)

---

## 1. 两套版本号

| 版本 | 函数 / 宏 | 含义 |
|------|-----------|------|
| **ABI 版本** | `fusion_abi_version()` / `FUSION_ABI_VERSION` | C 接口布局与符号契约；不兼容变更必须递增 |
| **Logic API 版本** | `fusion_logic_api_version()` / `FUSION_LOGIC_API_VERSION` | 纯逻辑 API（URL/config/status 过滤）；独立于运行时 ABI |
| **包版本** | `fusion_version_string()` / `CARGO_PKG_VERSION` | Cargo semver（当前 `0.1.0`） |

宿主程序启动时应检查：

```c
if (fusion_abi_version() < FUSION_ABI_VERSION) {
    /* 链接的 libfusion 过旧 */
}
```

---

## 2. ABI 版本递增规则（必须递增 `FUSION_ABI_VERSION`）

- 删除或重命名已导出 `#[no_mangle]` 符号
- 改变函数参数、返回值类型或错误码语义
- 改变 `FusionRuntime` 不透明结构体的使用契约
- 改变 `char *` 返回值的所有权规则（当前：**调用方** `fusion_string_free`）

**不必**递增 ABI 版本：

- 新增 C 函数（旧宿主可忽略）
- Rust 内部重构，C 头文件不变
- 文档、日志、默认配置行为微调（不改变 C 契约）

---

## 3. Logic API（W1）变更

纯逻辑函数（无 tokio / 无 socket）：

- `fusion_parse_url_json`
- `fusion_validate_config_toml_json`
- `fusion_filter_status_json`

由 [`crates/fusion-logic`](../crates/fusion-logic) 实现，WASM 与 native 共用。  
Logic API 破坏性变更递增 `FUSION_LOGIC_API_VERSION`，**不一定**递增 ABI 版本（除非 C 函数签名变化）。

---

## 4. Semver 与发布

| 阶段 | Cargo 版本 | 说明 |
|------|------------|------|
| 当前 | `0.1.x` | 快速迭代；ABI 2 已冻结于本系列文档 |
| 稳定后 | `1.0.0` | 承诺 ABI 2 在 1.x 内仅做加法扩展 |

发布检查清单见 [`release.md`](release.md)。

---

## 5. 内存与线程

- 所有 `char *` 返回值由 Fusion 分配，**必须** `fusion_string_free`
- `FusionRuntime` 非线程安全；每线程一个 handle，或外部串行化
- `fusion_last_error()` 返回的字符串同样需 `fusion_string_free`

---

## 6. 与 rem 旧 ABI 的关系

Fusion **不**兼容 rem 旧 `InitDialer` / `RemDial` 符号。迁移请对照 [`embedding.md`](embedding.md)（含 `memory://` 说明）。
