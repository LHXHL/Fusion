# 构建与发布

本文说明如何构建 Fusion 原生库、CLI 二进制与 WASM 纯逻辑包（Phase J / W1）。

---

## 1. 环境要求

- Rust stable（2021 edition）
- macOS / Linux 推荐；Windows 未在 CI 中验证
- WASM：`rustup target add wasm32-unknown-unknown`

---

## 2. 日常开发

```bash
# 全库单元测试
cargo test --lib

# 仅纯逻辑 crate
cargo test -p fusion-logic

# Release 库 + CLI
cargo build --release --lib
cargo build --release --bin fusion
```

产物：

| 平台 | 动态库 | 静态库 | CLI |
|------|--------|--------|-----|
| Linux | `target/release/libfusion.so` | `libfusion.a` | `target/release/fusion` |
| macOS | `libfusion.dylib` | `libfusion.a` | `target/release/fusion` |

头文件：[`include/fusion.h`](../include/fusion.h)

---

## 3. WASM Phase W1（纯逻辑）

```bash
chmod +x scripts/build-wasm.sh
./scripts/build-wasm.sh
```

构建 `fusion-logic` crate（`--features wasm`），导出：

| JS 名（wasm-bindgen） | 说明 |
|------------------------|------|
| `fusionLogicApiVersion` | Logic API 版本 |
| `fusionLogicParseUrl` | URL → JSON |
| `fusionLogicValidateConfigToml` | TOML 校验摘要 JSON |
| `fusionLogicFilterStatusJson` | 按 scope 过滤 status JSON |

Native 侧等价 C API 见 [`abi-stability.md`](abi-stability.md)。

**不**包含完整 Agent runtime（无 tokio 网络栈、TCP/relay/mux）。浏览器内完整组网不在当前范围；后续若做 W2，也仅限 WebSocket 客户端子集。

---

## 4. 交叉编译（手动）

Fusion 未内置交叉编译矩阵；常用模式：

```bash
# 示例：Linux x86_64 GNU（在 macOS 上需相应 linker）
rustup target add x86_64-unknown-linux-gnu
cargo build --release --target x86_64-unknown-linux-gnu --lib
```

Android / iOS 需额外 NDK / Xcode 配置，按宿主项目自行集成 `libfusion.a`。

---

## 5. CI

GitHub Actions：[`.github/workflows/ci.yml`](../.github/workflows/ci.yml)

- `cargo test --lib`
- `cargo test -p fusion-logic`
- `wasm32-unknown-unknown` 构建
- Ubuntu + macOS release 编译

---

## 6. 发布检查清单

- [ ] `cargo test --lib` 通过
- [ ] `cargo test -p fusion-logic` 通过
- [ ] ABI 变更已更新 `FUSION_ABI_VERSION` 与 [`include/fusion.h`](../include/fusion.h)
- [ ] 更新 [`docs/abi-stability.md`](abi-stability.md) / [`embedding.md`](embedding.md)
- [ ] 能力边界变更同步 [`baseline-current-capabilities.md`](baseline-current-capabilities.md)

---

## 7. 嵌入示例

- C：[`examples/c_host/main.c`](../examples/c_host/main.c)
- Python：[`examples/python_host/demo.py`](../examples/python_host/demo.py)
