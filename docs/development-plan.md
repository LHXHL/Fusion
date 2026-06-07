# Fusion 开发规划

本文档对照原 **rem 目标设计**（历史文件 `设计文档和构想.md`，已移除）与当前 Fusion 实现，给出**已实现摘要**、**仍未实现清单**和**后续开发阶段**。

原则：

- 以代码与 `cargo test --lib` 为准，不以旧文档表述为准
- 已完成阶段（A–E）只作基线记录，不重复立项
- 新阶段聚焦设计文档中尚未落地、且对交付有价值的能力

---

## 1. 项目定位

Fusion 继承 rem 的核心理念：**对等 Agent**、`-s`/`-c` 传输层组合、`-l`/`-r` 应用层配对、多跳 relay 与 route 自发现。

当前阶段判断：

> **单体 Agent 主链路已收口，Simplex 与 C ABI 已具备第一版交付面；距离 rem 设计文档的全量传输/服务/平台化目标仍有明显差距。**

---

## 2. 与原设计文档的能力对照

### 2.1 传输层（Tunnel）

| 设计文档 | 当前 Fusion | 状态 |
|----------|-------------|------|
| `tcp://` | 已支持，mux/relay/task | **已交付** |
| `udp://` | 已支持 datagram 会话 | **已交付**（无 KCP 层） |
| `ws://` / `wss://` | 已支持，含 TLS/mTLS 参数面 | **已交付**（TLS 配置面仍偏 PEM 显式） |
| `unix://` / `memory://` | 已支持 | **已交付** |
| `icmp://` / `wg://` | sandbox 级 datagram 实现 | **实验性** |
| `simplex+http://` | direct task、mux、raw、relay | **已交付** |
| `simplex+dns://` | direct task、mux、raw、relay | **已交付**（UDP DNS 信道，非公网 DNS 隧道产品化） |
| `simplex+oss://` | 本地目录 mailbox 模型 | **已交付**（非阿里云 OSS SDK 集成） |
| `http://` 长轮询隧道 | direct task；mux/relay 同 simplex+http | **已交付** |
| `h2://` HTTP/2 隧道 | direct task、mux、raw、relay；`h2s://` TLS | **已交付**（见 [`h2-transport.md`](h2-transport.md)） |
| `streamhttp://` SSE+POST | direct task | **已交付**（无 mux/relay） |
| `dns://` 常规 DNS 隧道 | 与 `simplex+dns://` 同栈别名 | **已交付**（K1） |
| UDP + **KCP** 可靠传输 | 无独立 KCP 层 | **未实现** |
| URL `up-`/`down-` 前缀拆分 | `-c up-tcp://` / `down-ws://` 与 CLI 等价 | **已交付** |
| 异构协议上下行分离（如 TCP 上 + UDP 下） | 连接池拆分，非 tunnel 级双信道绑定 | **部分实现** |

### 2.2 应用层（Serve）

| 设计文档 | 当前 Fusion | 状态 |
|----------|-------------|------|
| `socks5://` | 本地入口 + relay 路径 | **已交付** |
| `http://` 正向代理 | 本地入口 + relay 路径 | **已交付** |
| `raw://` / `port://` | 已支持 | **已交付** |
| `ss://` Shadowsocks | `method=none` + `aes-256-gcm-siv` 请求帧 | **实验性**（无 UDP associate） |
| `trojan://` | TCP CONNECT + SHA224 密码；可选 TLS 入口 | **已交付**（无 UDP associate） |
| `externalc2://` | 无 | **未实现**（H3 选型 Trojan；见 [`trojan-transport.md`](trojan-transport.md)） |
| Simplex 承载 socks5/http 本地入口 | mux 可用，无专门集成测试 | **实验性** |
| Simplex 承载 `port://` | 无 | **未实现** |

### 2.3 流量处理（Wrapper / 加密）

| 设计文档 | 当前 Fusion | 状态 |
|----------|-------------|------|
| 预共享密钥帧加密 `-k` | 已支持 | **已交付** |
| Wrapper pipeline（compress/padding/AEAD） | 已支持 | **已交付** |
| 双端 wrapper capability 交换 | Hello capability 暴露 shared-key/compress/padding | **部分实现**（仍需手工对齐配置） |
| AES / XOR 等多算法注册 | 仅 AEAD + compress/padding | **部分实现** |
| SM4 / Twofish / 3DES / CAST5 / Blowfish / TEA / XTEA | 无 | **未实现** |
| Snappy 压缩 | 使用 flate2，非 Snappy | **未实现** |
| URL `?wrapper=` / `?compress` 查询参数 | CLI/`fusion.toml` 为主 | **部分实现** |

### 2.4 组网与高级特性

| 设计文档 | 当前 Fusion | 状态 |
|----------|-------------|------|
| 多跳 relay + RouteAnnounce | TCP/WS/Simplex，3/5 跳回归 | **已交付** |
| 10+ 跳大规模拓扑 | 有机制，缺压测与运维验证 | **部分实现** |
| `-d` / 指定目标 Agent 路由 | 等价为 `--remote-peer` + registry | **部分实现**（CLI 命名不同） |
| 代理链 `-x` / 前置 `-f` | 已支持 | **已交付** |
| ConnHub `random/fallback/round-robin` | `--conn-policy` | **已交付** |
| 上游连接池复用与 stale 清理 | 已支持 | **已交付** |
| 本地入口多上游 failover | 单元测试 + 运行时逻辑 | **已交付**（socks5/http 端到端集成测试仍少） |
| Route 冲突选优 / 最近成功 / 抖动收敛 | Phase C 已落地 | **已交付** |
| 指数退避重连 | `--retry*` | **已交付** |
| URL 内嵌 `retry` / `lb` 参数 | 无 | **未实现** |

### 2.5 可观测性与错误模型

| 能力 | 状态 |
|------|------|
| 统一错误码（`src/error.rs`） | **已交付**（覆盖 wrapper/TLS/route/runtime，service/task 仍少） |
| runtime status JSON + recent errors | **已交付** |
| upstream pool 写入 status 快照 | **已交付**（`upstream_pools` + connect 配置摘要） |
| TLS 配置摘要（不含敏感材料） | **未实现** |
| `docs/test-matrix.md` 回归矩阵 | **已交付** |

### 2.6 平台化与交付

| 设计文档 | 当前 Fusion | 状态 |
|----------|-------------|------|
| C ABI（`.so`/`.dylib`/`.a`） | ABI v2，runtime handle/config/status/task | **已交付**（API 与 rem 旧设计不同） |
| C 示例 / Python ctypes | `examples/c_host`、`examples/python_host` | **已交付** |
| 嵌入指南 | [`embedding.md`](embedding.md) | **已交付** |
| rem 式 `InitDialer`/`RemDial`/`Memory*` API | 无 | **未实现** |
| WASM 完整 runtime | 未实现；W1 纯逻辑见 [`embedding.md`](embedding.md)、[`release.md`](release.md) |
| TinyGo / 极小体积构建 | 无 | **未实现** |
| `build.sh` 模块化编译 / build tags | Cargo 统一构建 | **未实现** |
| 编译期内置默认参数 | 无 | **未实现** |
| `--list` 组件注册表 | 无 | **未实现** |
| 旧版 `-m` 兼容模式 | 无 | **未实现** |

---

## 3. 已完成阶段基线（A–E）

以下阶段已在当前仓库落地，后续不必重复立项：

| 阶段 | 摘要 |
|------|------|
| **A** 文档与交付面 | README/baseline/simplex/test-matrix/fusion.toml.example 已对齐 |
| **B** 错误与可观测性 | 错误码、recent errors、status 扩展、wrapper/TLS 错误文案 |
| **C** Route / Failover | 选优策略、收敛测试、upstream pool / conn_policy 回归 |
| **D** Simplex  transport 化 | mux/relay/task 测试、[`simplex-transport.md`](simplex-transport.md) |
| **E** 平台化 | C ABI v2、嵌入示例、WASM 可行性文档 |

当前测试基线：`cargo test --lib`（213 项）。

---

## 4. 后续开发阶段

### Phase F：文档与配置可信（**已完成**）

**目标**：消除设计文档、README 与实现之间的偏差。

| 任务 | 说明 | 验收 |
|------|------|------|
| F1 同步 README / baseline | 移除“Simplex 未接入 mux/relay”等过时表述 | ✅ 与 [`simplex-transport.md`](simplex-transport.md)、baseline 一致 |
| F2 扩充 `fusion.toml.example` | WSS/mTLS、wrapper、multi-upstream、Simplex、SS 边界示例 | ✅ |
| F3 重建回归索引 | [`test-matrix.md`](test-matrix.md) | ✅ |
| F4 清理绝对路径 | baseline / quick-start / protocol | ✅ |

### Phase G：传输层扩展

**目标**：补齐 rem 设计中的常规 tunnel，而非重复 Simplex。

| 任务 | 说明 | 验收 |
|------|------|------|
| G1 `http://` 隧道 | HTTP 长轮询全双工（复用 Simplex HTTP 栈） | ✅ 单跳 task；mux/relay 与 `simplex+http` 同路径 |
| G2 `streamhttp://` | SSE 下行 + POST 上行（CDN 友好） | ✅ direct task + 文档 |
| G3 UDP/KCP（可选） | 与现有 `udp://` 边界文档化 | ✅ 见 [`http-transport.md`](http-transport.md) §4 |
| G4 URL 级 `up-`/`down-` 前缀 | 与 `--up-connect` 语义一致 | ✅ config/CLI 解析 |

### Phase H：应用层与 Wrapper 完整度

| 任务 | 说明 | 验收 |
|------|------|------|
| H1 Shadowsocks 真实加密矩阵 | `aes-256-gcm-siv` TCP 请求帧 | ✅ 与 `method=none` 边界分离；UDP associate 未实现 |
| H2 Wrapper 自动协商 | Hello 阶段 capability 交换 | ✅ capability 已交换；仍不自动改本地 wrapper |
| H3 Trojan / ExternalC2 | 择 Trojan 最小交付 | ✅ Trojan TCP/WS + mux→raw；ExternalC2 文档化未实现 |
| H4 Simplex 上 port forward / socks5 正式交付 | socks5 + port over `simplex+http` | ✅ 端到端回归；http proxy over Simplex 仍待扩展 |

### Phase I：生产化 TLS、状态与运维

| 任务 | 说明 | 验收 |
|------|------|------|
| I1 TLS 配置矩阵 | 系统 CA、SNI、校验策略文档化 | ✅ [`tls-transport.md`](tls-transport.md)；status `config.tls.*` 摘要 |
| I2 证书热更新（可选） | listener 轮转 | ✅ 文档明确不支持；无热更新实现 |
| I3 status 扩展 | upstream pool 快照、connect 配置摘要 | ✅ `status --json` 可读；TLS 摘要见 I1 |
| I4 10+ 跳 / 多出口压测 | 拓扑模拟与 route 收敛 | ✅ TCP / Simplex HTTP 10-hop 集成回归 |

### Phase J：平台化深化 — **已完成**

| 任务 | 说明 | 验收 |
|------|------|------|
| J1 C ABI 稳定化 | 版本策略、破坏性变更纪律 | ✅ [`abi-stability.md`](abi-stability.md)；Logic API 版本导出 |
| J2 Memory tunnel FFI | 对接 `memory://` 的读写 API（若宿主需要） | ✅ [`embedding.md`](embedding.md) §Memory 隧道 |
| J3 WASM Phase W1 | 导出 URL/config/status 纯逻辑 API | ✅ [`crates/fusion-logic`](../crates/fusion-logic)；`wasm32` 构建 |
| J4 交叉编译与发布 | CI 产出多平台 `libfusion` / `fusion` | ✅ [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)；[`release.md`](release.md) |

### Phase K：HTTP/2 与 DNS 常规隧道 — **已完成**

详细设计见 [`h2-dns-transport-plan.md`](h2-dns-transport-plan.md)。

| 任务 | 说明 | 验收 |
|------|------|------|
| K1 `dns://` scheme | 与 `simplex+dns://` 同栈的 rem 风格 URL | ✅ scheme 别名 + task 回归 |
| K2 `h2://` 单会话 | HTTP/2 连接 + hello/task | ✅ `h2_mux` + task 回归 |
| K3 `h2_mux` | 控制 stream 上 Fusion mux 帧 | ✅ mux 单元测试 |
| K4 h2 relay | raw 入站 + relay 路径接入 | ✅ runtime relay/task/raw |
| K5 `h2s://` + TLS | ALPN h2 + TLS query | ✅ `build_h2_tls_*` + cleartext 默认 |

---

## 5. 推荐实施顺序

### 第一批（1–2 周）：低风险、减误判 — **已完成**

1. ✅ F1 + F4 文档对齐
2. ✅ F2 配置示例
3. ✅ F3 回归索引
4. ✅ I3 status 小扩展（upstream pool + connect 摘要）

### 第二批（2–4 周）：能力补洞 — **当前优先**

1. ✅ H4 Simplex socks5 正式交付
2. ✅ H1 Shadowsocks 加密（最小可用 AEAD）
3. ✅ H2 Wrapper capability 交换原型
4. C3 补 socks5/http failover 端到端测试（若 F3 未覆盖）

### 第三批（按需）：传输与平台 — **I/J 批次完成**

1. ~~G1 HTTP 隧道 / G2 streamhttp / G4 URL 前缀~~ ✅
2. ~~H4 Simplex port forward~~ ✅
3. ~~I1 TLS 矩阵文档 / I4 多跳压测~~ ✅
4. ~~J1–J4 平台化（ABI / Memory 文档 / WASM W1 / CI）~~ ✅
5. G3 KCP（按需）、C3 failover 端到端 — 按实际需求排期
6. **Phase K** `h2://` + `dns://` — 见 [`h2-dns-transport-plan.md`](h2-dns-transport-plan.md)

---

## 6. 任务完成定义

任何新任务合并前应满足：

- 实现代码 + `cargo test --lib` 回归
- 更新 README 或对应专题文档（Simplex/TLS/relay/embedding 等）
- 若涉及配置，同步 `fusion.toml.example`
- 若涉及能力边界变更，更新 [`baseline-current-capabilities.md`](baseline-current-capabilities.md)
- 错误与 status 行为与 [`src/error.rs`](../src/error.rs) 模型一致

---

## 7. 风险与约束

- **Simplex `oss` 为本地目录模型**，扩展为真实 OSS SDK 属于新产品能力，不与当前实现混为一谈。
- **Route 选优变更**必须附带多跳与等价 route 回归。
- **Wrapper 协商**需定义与旧节点共存策略，避免全网握手失败。
- **C ABI** 变更必须递增 `fusion_abi_version()` 并更新 [`include/fusion.h`](../include/fusion.h)。
- **WASM** 不承诺完整 tokio 网络栈，见 [`embedding.md`](embedding.md) 与 [`release.md`](release.md)。

---

## 8. 里程碑

| 里程碑 | 范围 | 完成标志 |
|--------|------|----------|
| M1 文档可信 | F1–F4 | ✅ |
| M2 服务完整度 | H1、H3、H4 | ✅ SS、Trojan、socks5/port over Simplex 可演示 |
| M3 传输补洞 | G1/G2 之一 | 新增一种常规 tunnel 进入 baseline |
| M4 运维可观测 | I3、I4 | ✅ I1/I3 status + I4 10-hop 回归 |
| M5 平台深化 | J1–J4 | ✅ ABI 文档 + Memory 对照 + WASM W1 + CI/release |
| M6 传输扩展 | K1–K4 | `dns://` 别名 + `h2://` mux/relay 可演示 |

---

## 9. 相关文档

- 当前能力基线：[`baseline-current-capabilities.md`](baseline-current-capabilities.md)
- 开发规划：[`development-plan.md`](development-plan.md)
- 回归矩阵：[`test-matrix.md`](test-matrix.md)
- Simplex 边界：[`simplex-transport.md`](simplex-transport.md)
- Trojan 边界：[`trojan-transport.md`](trojan-transport.md)
- H2 / DNS 隧道规划：[`h2-dns-transport-plan.md`](h2-dns-transport-plan.md)
- 快速上手：[`quick-start.md`](quick-start.md)
