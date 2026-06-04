# Fusion

Fusion 当前是一个**单体 Agent** 项目。

## 核心形态

- 单一二进制：`fusion`
- 不再区分 `server / client / implant`
- 每个节点都是 Agent
- 通过参数组合承担不同职责：
  - `-s`：监听 tunnel
  - `-c`：连接上游 tunnel
  - `-l`：开放本地入口服务
  - `-r`：暴露远端出口/固定转发服务
  - `task`：发起任务请求
  - `status / peers / routes / services`：查看本地运行态

---

## 快速开始

在仓库根目录执行：

```bash
cargo build --bin fusion
cargo run --bin fusion -- --help
```

### TCP 两节点

终端 A：

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -a node-a
```

终端 B：

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -a node-b
```

### 查看状态

```bash
cargo run --bin fusion -- status
cargo run --bin fusion -- peers list
cargo run --bin fusion -- routes list
cargo run --bin fusion -- services list
```

示例输出（节选）：

```text
status.generated_at=...
session.count=1
registry.peer_count=1
registry.route_count=1
service.exposed_count=1
```

---

## 已实现能力

### Tunnel
- `tcp://`
- `ws://`
- `wss://`（仅保留入口与部分运行时分支，未作为完整 TLS 能力承诺）

### Service
- `socks5://HOST:PORT`
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN_HOST:LISTEN_PORT->TARGET_HOST:TARGET_PORT`

### Task
- shell
- screenshot
- upload
- download

---

## 示例

### relay

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -s tcp://0.0.0.0:35000 \
  -a relay
```

### socks5 -> raw

出口节点：

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -r raw:// \
  -a exit-node
```

入口节点：

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -l socks5://127.0.0.1:1080 \
  -r raw:// \
  -a entry-node
```

### 固定端口转发

```bash
cargo run --bin fusion -- \
  -r "port://127.0.0.1:8080->example.com:80" \
  -a port-node
```

### task shell

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

示例输出（节选）：

```text
task.result.id=task-...
task.result.ok=true
task.result.output=...
```

---

## 配置

命令行参数优先，也支持可选 `fusion.toml`：

```bash
cp fusion.toml.example fusion.toml
cargo run --bin fusion -- --config ./fusion.toml
```

---

## 当前未完全实现/边界说明

- `wss://`：当前不承诺完整 TLS listener / cert 配置能力
- `src/tunnel/tls.rs`：本轮未新增空壳模块
- `src/crypto/wrapper.rs` / `src/utils/fs.rs`：本轮明确不补空壳文件
- 当前协议错误传播未形成统一错误码体系

---

## 验收与回归

```bash
cargo test
bash scripts/regression/manual-smoke.sh
```

---

## 文档索引

- `docs/quick-start.md`
- `docs/relay.md`
- `docs/socks5-and-raw.md`
- `docs/task.md`
- `docs/protocol.md`
- `docs/baseline-current-capabilities.md`
- `docs/test-matrix.md`
- `docs/plan-alignment.md`
