# Fusion 当前能力基线（单体 Agent）

本文档记录当前仓库 **实际可运行** 的能力边界，作为回归与交接基线。

## 1. 单体入口

当前仅保留一个二进制：

- `fusion`

入口文件：
- `src/bin/fusion.rs`

## 2. Tunnel 类型

### 2.1 已验证可用
- `tcp://`
- `ws://`
- `wss://`
- `udp://`
- `unix://`
- `memory://`
- `icmp://`
- `wg://`
- `h2://` / `h2s://`（HTTP/2 mux；单 h2 控制 stream 承载 Fusion 帧，详见 [`h2-transport.md`](h2-transport.md)）
- `dns://`（`simplex+dns://` 别名，UDP DNS 查询信道）

说明：
- `memory://` 当前面向同进程测试与嵌入模式，不是跨独立 CLI 进程的持久监听 transport
- `icmp://` / `wg://` 当前为 sandbox 兼容的 datagram transport 实现，复用现有 datagram 会话语义

### 2.2 `wss://` 当前配置方式

当前 `wss://` 的状态：
- URL 解析支持
- CLI 接受该 scheme
- runtime 监听/连接分支支持该 scheme
- 已提供独立 TLS 收口模块：
  - `src/tunnel/tls.rs`
- 服务端监听支持：
  - `tls-cert=/absolute/path/to/cert.pem`
  - `tls-key=/absolute/path/to/key.pem`
  - `tls-client-ca=/absolute/path/to/client-ca.pem`
- 客户端连接支持：
  - `tls-ca=/absolute/path/to/ca.pem`
  - `tls-client-cert=/absolute/path/to/client-cert.pem`
  - `tls-client-key=/absolute/path/to/client-key.pem`
  - `tls-insecure=1`

说明：
- 当前 TLS 能力已可用于本地/测试场景和显式 PEM 配置场景
- 当前已具备基础 mTLS 参数面
- 尚未扩展到更完整的双向认证策略、证书热更新、复杂证书来源矩阵

## 3. Service 类型

### 3.1 本地入口服务
- `socks5://HOST:PORT`
- `http://HOST:PORT`
- `ss://HOST:PORT?method=none`
- `ss://HOST:PORT?method=aes-256-gcm-siv&password=...`
- `trojan://HOST:PORT?password=...`（可选 `tls-cert`/`tls-key`）

### 3.2 远端出口/暴露服务
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN_HOST:LISTEN_PORT->TARGET_HOST:TARGET_PORT`

其中：
- `raw://HOST:PORT` 表示固定出口目标
- `raw://` 表示动态出口目标，由上游请求在运行时决定最终连接的 `host:port`

说明：
- `raw://` 用于动态目标
- `port://...->...` 用于固定监听 + 固定目标转发

## 4. Task 类型

已统一到 `task` 能力：
- shell
- screenshot
- upload
- download

对应实现：
- `src/task/shell.rs`
- `src/task/screenshot.rs`
- `src/task/file.rs`
- `src/task/dispatcher.rs`

## 5. CLI 参数

### 5.1 连接/服务参数
- `-s, --listen <URL>`
- `-c, --connect <URL>`
- `-l, --local-serve <URL>`
- `-r, --remote-serve <URL>`
- `--remote-peer <AGENT_ID>`

### 5.2 身份/重连参数
- `-a, --agent-name <NAME>`
- `-k, --key <SECRET>`
- `--retry <N>`
- `--retry-interval <SECONDS>`
- `--retry-max-interval <SECONDS>`
- `-x, --proxy-chain <URL>`
- `-f, --front-proxy <URL>`
- `--conn-policy <fallback|random|round-robin>`
- `--up-connect <URL>`
- `--down-connect <URL>`

### 5.3 运行态参数
- `--data-dir <PATH>`
- `--config <PATH>`
- `--log-level <LEVEL>`

### 5.4 task flags
- `--task-shell <COMMAND>`
- `--task-screenshot`
- `--task-download <REMOTE_PATH>`
- `--task-upload <LOCAL:REMOTE>`
- `--task-save <PATH>`
- `--task-peer <AGENT_ID>`

## 6. 控制子命令

- `status`
- `peers`
- `routes`
- `services`
- `task`

## 7. 运行态持久化

当前会在 `data-dir` 下写入：
- `runtime-status.json`

快照 JSON 主要字段：
- `peers` / `routes` / `streams` / `relay_links`
- `config`：wrapper、shared-key、connect/up-connect/down-connect、conn_policy、proxy chain 摘要
- `upstream_pools`：本地入口（socks5/http/shadowsocks）上游 mux 池的 handler、transport、cached_keys
- `recent_errors`：最近 32 条统一错误码记录（见 `src/error.rs`）

默认目录：
- `.fusion/`

## 7.1 平台化输出

当前库构建支持：
- `rlib`
- `cdylib`
- `staticlib`

当前已导出 C ABI v2（详见 [embedding.md](embedding.md)、[abi-stability.md](abi-stability.md)）：
- 版本 / 字符串：`fusion_abi_version`、`fusion_logic_api_version`、`fusion_version_string`、`fusion_string_free`
- 错误：`fusion_last_error`、`fusion_clear_last_error`
- 纯逻辑（W1）：`fusion_parse_url_json`、`fusion_validate_config_toml_json`、`fusion_filter_status_json`
- Runtime：`fusion_runtime_create`、`fusion_runtime_destroy`
- Config / 生命周期：`fusion_runtime_load_config_file`、`fusion_runtime_start`、`fusion_runtime_stop`
- 查询 / 任务：`fusion_runtime_status_json`、`fusion_runtime_task_request_json`

头文件：[`include/fusion.h`](../include/fusion.h)

## 8. 协议消息类型

当前统一协议消息：
- `Hello`
- `HelloAck`
- `Heartbeat`
- `AgentAnnounce`
- `RouteUpdate`
- `TaskRequest`
- `TaskResult`
- `StreamOpen`
- `StreamData`
- `StreamClose`

协议说明见：
- `docs/protocol.md`

## 9. 已验证场景

- `fusion --help`
- TCP 单跳互连
- WS 单跳互连
- WSS 单跳互连（单元测试）
- UDP 单跳互连
- Unix 单跳互连
- Memory 单跳互连
- icmp/wg URL 解析与 listener/dialer 分流
- `-k` 预共享密钥下的 TCP / WS / mux 链路（单元测试）
- relay 基础转发
- 3 跳 / 5 跳 TCP mux relay 回归
- socks5 over relay
- HTTP proxy over relay
- Shadowsocks service parsing / runtime mode / stream-open builder（`method=none` + Fusion AEAD 请求帧）
- socks5 over `simplex+http://` 到远端 `raw://`
- task shell/screenshot/upload/download
- `dns://` / `h2://` direct task 与 mux relay（3/5 跳回归）
- runtime status snapshot 输出
- `port://...->...` 固定端口转发

## 10. 已知限制

### 10.1 `wss://` 仍属精简 TLS 配置面
当前已支持显式证书/私钥、客户端 CA/insecure 模式与基础 mTLS 参数面，但仍未覆盖完整企业级 TLS 配置矩阵。

### 10.2 `ss://` 当前边界
当前行为：
- 支持本地 `ss://HOST:PORT?method=none` 入口
- 支持本地 `ss://HOST:PORT?method=aes-256-gcm-siv&password=SECRET` 入口
- 支持 TCP/WS 上游 mux 路径
- 支持 Shadowsocks 地址头解析后转成 `StreamOpen(raw)`
- `aes-256-gcm-siv` 当前使用 Fusion 自定义请求帧（magic + nonce + length + ciphertext）保护首个 Shadowsocks 地址请求帧

当前未覆盖：
- UDP 关联
- 与完整 Shadowsocks 生态逐字节协议兼容
- 多算法矩阵（当前仅 `aes-256-gcm-siv`）

### 10.3 `trojan://` 当前边界

当前行为：
- 支持 `trojan://HOST:PORT?password=SECRET` 本地 TCP 入口
- 密码校验为 `hex(SHA224(password))`（56 字符）
- 支持 TCP CONNECT 后 mux 转发到远端 `raw://`
- 可选 `tls-cert`/`tls-key` 在本地入口启用 TLS

当前未覆盖：
- UDP ASSOCIATE
- `externalc2://`（见 [`trojan-transport.md`](trojan-transport.md)）
- Simplex 专用 Trojan 路径（可经 TCP/WS 上游 mux 间接使用）

### 10.4 `-k` 当前为预共享密钥帧级加密
当前行为：
- 对 Fusion 协议帧统一加密
- 覆盖 hello / heartbeat / task / stream 等消息
- 双端必须配置相同密钥

当前未覆盖：
- 自动密钥协商
- 多算法切换
- 独立压缩 / padding 处理链

### 10.5 结构上未新增以下计划文件
本轮收尾**明确不补空壳文件**：
- `src/utils/fs.rs`

原因：
- 当前没有实际功能依赖它们
- 为避免“凑目录”式空模块，本轮以文档收口代替

### 10.5 `protocol/stream.rs` / `agent/state.rs` / `tunnel/listener.rs` / `tunnel/dialer.rs`
本轮已补上，用于让计划结构与实际工程更加一致。

### 10.6 runtime 职责开始拆分
当前仍以 `src/app/runtime.rs` 为主入口，
但以下职责已开始独立收口：
- `src/app/runtime_status.rs`
- `src/app/runtime_task.rs`
- `src/app/runtime_peer.rs`
- `src/app/runtime_service.rs`
- `src/app/runtime_orchestrator.rs`
- `src/app/runtime_socks5.rs`
- `src/app/runtime_tests.rs`
- `src/app/runtime_relay.rs`
- `src/app/runtime_mode.rs`

其中本轮进一步将以下路径从 `runtime.rs` 拆出：
- listener startup / dialer startup / runtime summary 的编排逻辑
- mux task/relay peer 的接入、控制帧处理与 route 转发主循环
- port forward listener 与 inbound raw service 的挂载路径
- socks5 本地入口到远端 raw stream 的挂载与转发路径
- 主动 task 请求的发起、结果输出与 artifact 落盘路径
- runtime 相关回归测试模块

此外，stream 生命周期状态已开始统一收口：
- `src/protocol/stream.rs` 中的 `StreamLifecycle`
- `src/agent/registry.rs` 中的运行态 stream 记录

当前已不再各自维护两套独立但同名语义的状态枚举。

### 10.6 route 多跳稳定性已补第一轮收口
当前已补上的能力：
- route TTL 与 stale route 清理
- peer 断开后的 route 回收
- route path 全量保留，不再只剩 next-hop / hop-count
- route snapshot 会携带完整已知 path，避免多跳信息在重新同步时丢失
- 对明显异常的 route update 增加拒绝逻辑：
  - source peer 与 path 首跳不一致
  - path 中包含本地 agent
  - path 自身出现回环
  - path 未以 origin agent 收尾
- 同一 destination 的路由冲突，当前按“更短路径优先；同 hop 数下 next-hop 字典序稳定选优”

当前已验证：
- 3 跳 TCP mux relay 数据往返
- 5 跳 TCP mux relay 数据往返
- 中间 next-hop 断开后，旧 route 会被回收，并可通过新的 route announcement 恢复

### 10.7 wrapper pipeline 已建立基础挂载点
当前已补上的能力：
- `src/crypto/wrapper.rs` 已新增统一 wrapper pipeline 抽象
- `src/crypto/transport.rs` 现已经由 wrapper pipeline 处理 transport payload
- 当前已落地的 wrapper stage：
  - passthrough
  - shared-key AEAD
  - compression
  - padding

当前补充说明：
- 已支持 multi-stage pipeline roundtrip 测试：
  - compression
  - padding
  - compression + padding + AEAD
- 当前已支持基础配置面：
  - `--wrap-compress`
  - `--wrap-padding <BYTES>`
- Hello capability 当前会暴露 `wrapper:shared-key`、`wrapper:compress`、`wrapper:padding:<BYTES>`
- 当前仍不会自动改写本地 wrapper 配置，双端仍需手工对齐

当前意义：
- transport 层已具备统一挂载点
- 后续 compression / padding / TLS 补强不必再直接侵入 `transport.rs` 主逻辑

当前仍未覆盖：
- 更强的 TLS 补强 wrapper

### 10.8 高级网络与 Simplex service 边界

以下仍属于后续规划阶段：
- 全量异构 tunnel 级上下行分离（当前为连接池拆分）
- Simplex 上 http 本地入口正式端到端交付（H4 后续）
- WASM 完整 runtime（W1 纯逻辑已交付，见 [`embedding.md`](embedding.md)、[`release.md`](release.md)）

当前已交付的相关能力：
- 本地 socks5/http 多上游 failover、长生命周期 upstream mux 池（status 可观测）
- socks5 本地入口可经 `simplex+http://` mux 到远端 `raw://`
- route 选优与 recent errors（Phase C）
- Simplex direct task / mux / raw / relay（详见 [simplex-transport.md](simplex-transport.md)）
- `dns://` 与 `simplex+dns://` 等价；`h2://` / `h2s://` mux/relay（详见 [h2-transport.md](h2-transport.md)）
- 特殊 tunnel：
  - `memory://`
  - `unix://`
  - `icmp://`
  - `wg://`
- 平台化：
  - `cdylib` / `staticlib`
  - C ABI v2 + Logic API v1（runtime handle、config、status、task、URL 校验）
  - WASM W1：`crates/fusion-logic`（`wasm32-unknown-unknown`）
  - 嵌入示例与 [embedding.md](embedding.md)、[release.md](release.md)

当前已补上的 Phase 5 第一版能力：
- TCP 出站链路可通过 `-x/-f` 走 SOCKS5 / HTTP CONNECT 代理链
- 多上游 endpoint 已支持 `fallback / random / round-robin`
- 已支持独立 `up-connect` / `down-connect` 连接池配置
- socks5/http 本地入口已支持按连接策略选择上游，并在上游失败时切换
- socks5/http 本地入口已支持复用长生命周期上游 mux peer，避免每个 client 单独重建上游连接
- 当复用的上游 peer 在 stream 建立阶段失败时，当前会从池中剔除并尝试下一候选上游
- 上游池复用前会检查本地 session/registry 中该 peer 是否仍为 `Active`，发现陈旧连接会自动剔除
- 上游池当前还会周期性扫描 hub/registry，对已失活 peer 做后台清理

## 11. 快速验收命令

```bash
cargo build --bin fusion
cargo test --lib
```
