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
- `wss://`（支持显式 TLS 证书/私钥配置）

### Service
- `socks5://HOST:PORT`
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN_HOST:LISTEN_PORT->TARGET_HOST:TARGET_PORT`

其中：
- `raw://HOST:PORT`：固定出口目标
- `raw://`：动态出口目标，由上游请求决定最终连接地址

### Task
- shell
- screenshot
- upload
- download

### 传输加密
- `-k, --key <SECRET>`
  - 对 `tcp://` / `ws://` / `wss://` 链路上的 **统一协议帧** 做预共享密钥加密
  - 当前为**预共享密钥模式**
  - 双端必须配置相同密钥，否则握手失败

---

### WSS / TLS

`wss://` 通过 URL query 指定 TLS 参数：

- 服务端监听：
  - `tls-cert=/absolute/path/to/cert.pem`
  - `tls-key=/absolute/path/to/key.pem`
- 客户端连接：
  - `tls-ca=/absolute/path/to/ca.pem`：附加自定义 CA
  - `tls-insecure=1`：跳过证书与主机名校验（仅测试场景建议使用）

示例：

```bash
cargo run --bin fusion -- \
  -s "wss://0.0.0.0:8443/tunnel?tls-cert=/tmp/fusion-cert.pem&tls-key=/tmp/fusion-key.pem" \
  -a wss-server

cargo run --bin fusion -- \
  -c "wss://localhost:8443/tunnel?tls-insecure=1" \
  -a wss-client
```

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

## 共享密钥（`-k`）

`-k` 当前会对 Fusion 的传输帧进行统一封装：

- hello / hello-ack / heartbeat
- route / task
- stream open / stream data / stream close

适用链路：

- `tcp://`
- `ws://`
- `wss://`

示例：

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -k "my-shared-secret"

cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -k "my-shared-secret" \
  task shell "whoami"
```

说明：

- 当前 `-k` 是 **PSK（预共享密钥）模式**
- 不做自动协商
- 若双端密钥不一致，连接会在握手阶段失败
- 当前加密粒度是 **Fusion 协议帧**，不是目标业务流量的额外独立 wrapper pipeline

---

## 当前未完全实现/边界说明

- `wss://`：当前已支持显式证书/私钥配置，但 TLS 能力仍是精简版，暂未覆盖更完整的证书矩阵与双向认证场景
- `src/tunnel/tls.rs`：当前为实际落地模块，不是空壳
- `-k`：当前为预共享密钥的帧级加密，尚未扩展为设计文档中的完整 wrapper pipeline / 多算法可插拔体系
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
