# Fusion 当前能力基线（单体 Agent）

本文档记录当前仓库 **实际可运行** 的能力边界，作为回归与交接基线。

## 1. 单体入口

当前仅保留一个二进制：

- `fusion`

入口文件：
- `/Users/qi4l/lang/Rust/Fusion-master/src/bin/fusion.rs`

## 2. Tunnel 类型

### 2.1 已验证可用
- `tcp://`
- `ws://`
- `wss://`

### 2.2 `wss://` 当前配置方式

当前 `wss://` 的状态：
- URL 解析支持
- CLI 接受该 scheme
- runtime 监听/连接分支支持该 scheme
- 已提供独立 TLS 收口模块：
  - `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/tls.rs`
- 服务端监听支持：
  - `tls-cert=/absolute/path/to/cert.pem`
  - `tls-key=/absolute/path/to/key.pem`
- 客户端连接支持：
  - `tls-ca=/absolute/path/to/ca.pem`
  - `tls-insecure=1`

说明：
- 当前 TLS 能力已可用于本地/测试场景和显式 PEM 配置场景
- 尚未扩展到更完整的双向认证、证书热更新、复杂证书来源矩阵

## 3. Service 类型

### 3.1 本地入口服务
- `socks5://HOST:PORT`

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
- `/Users/qi4l/lang/Rust/Fusion-master/src/task/shell.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/task/screenshot.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/task/file.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/task/dispatcher.rs`

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

默认目录：
- `.fusion/`

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
- `/Users/qi4l/lang/Rust/Fusion-master/docs/protocol.md`

## 9. 已验证场景

- `fusion --help`
- TCP 单跳互连
- WS 单跳互连
- WSS 单跳互连（单元测试）
- `-k` 预共享密钥下的 TCP / WS / mux 链路（单元测试）
- relay 基础转发
- 3 跳 / 5 跳 TCP mux relay 回归
- socks5 over relay
- task shell/screenshot/upload/download
- runtime status snapshot 输出
- `port://...->...` 固定端口转发

## 10. 已知限制

### 10.1 `wss://` 仍属精简 TLS 配置面
当前已支持显式证书/私钥与客户端 CA/insecure 模式，但仍未覆盖完整企业级 TLS 配置矩阵。

### 10.2 `-k` 当前为预共享密钥帧级加密
当前行为：
- 对 Fusion 协议帧统一加密
- 覆盖 hello / heartbeat / task / stream 等消息
- 双端必须配置相同密钥

当前未覆盖：
- 自动密钥协商
- 多算法切换
- 独立压缩 / padding 处理链

### 10.3 结构上未新增以下计划文件
本轮收尾**明确不补空壳文件**：
- `src/utils/fs.rs`

原因：
- 当前没有实际功能依赖它们
- 为避免“凑目录”式空模块，本轮以文档收口代替

### 10.4 `protocol/stream.rs` / `agent/state.rs` / `tunnel/listener.rs` / `tunnel/dialer.rs`
本轮已补上，用于让计划结构与实际工程更加一致。

### 10.5 runtime 职责开始拆分
当前仍以 `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime.rs` 为主入口，
但以下职责已开始独立收口：
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_status.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_task.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_peer.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_service.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_orchestrator.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_socks5.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_tests.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_relay.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_mode.rs`

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

当前意义：
- transport 层已具备统一挂载点
- 后续 compression / padding / TLS 补强不必再直接侵入 `transport.rs` 主逻辑

当前仍未覆盖：
- 多 stage 组合配置面
- compression wrapper
- padding wrapper
- 更强的 TLS 补强 wrapper

## 11. 快速验收命令

```bash
cargo build --bin fusion
cargo test
bash scripts/regression/manual-smoke.sh
```
