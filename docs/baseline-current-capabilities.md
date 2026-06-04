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

### 2.2 已接入 URL/运行时分支，但未完全收口
- `wss://`

当前 `wss://` 的状态：
- URL 解析支持
- CLI 接受该 scheme
- runtime 连接分支接受该 scheme
- **未提供独立 TLS listener/server 收口模块**
- **未声明完整证书配置矩阵**
- 因此当前只能视为：**保留入口，不作为完全验收能力承诺**

## 3. Service 类型

### 3.1 本地入口服务
- `socks5://HOST:PORT`

### 3.2 远端出口/暴露服务
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN_HOST:LISTEN_PORT->TARGET_HOST:TARGET_PORT`

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
- relay 基础转发
- socks5 over relay
- task shell/screenshot/upload/download
- runtime status snapshot 输出
- `port://...->...` 固定端口转发

## 10. 已知限制

### 10.1 `wss://` 未完全收口
当前不承诺完整 TLS listener / cert 配置能力。

### 10.2 结构上未新增以下计划文件
本轮收尾**明确不补空壳文件**：
- `src/tunnel/tls.rs`
- `src/crypto/wrapper.rs`
- `src/utils/fs.rs`

原因：
- 当前没有实际功能依赖它们
- 为避免“凑目录”式空模块，本轮以文档收口代替

### 10.3 `protocol/stream.rs` / `agent/state.rs` / `tunnel/listener.rs` / `tunnel/dialer.rs`
本轮已补上，用于让计划结构与实际工程更加一致。

## 11. 快速验收命令

```bash
cargo build --bin fusion
cargo test
bash scripts/regression/manual-smoke.sh
```
