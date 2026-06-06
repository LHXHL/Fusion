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
- `udp://`
- `unix://`
- `memory://`（当前为同进程/嵌入模式 transport）
- `icmp://`（当前为 sandbox 下的 datagram transport 实现）
- `wg://`（当前为 sandbox 下的 datagram transport 实现）

### Service
- `socks5://HOST:PORT`
- `http://HOST:PORT`
- `ss://HOST:PORT?method=none`
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN_HOST:LISTEN_PORT->TARGET_HOST:TARGET_PORT`

其中：
- `raw://HOST:PORT`：固定出口目标
- `raw://`：动态出口目标，由上游请求决定最终连接地址
- `socks5://` 当前支持可选本地认证：
  - `?username=USER&password=PASS`
- `http://` 当前支持可选本地 Basic 代理认证：
  - `?username=USER&password=PASS`

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

### 平台化输出
- `cargo build --lib`
- 默认同时产出：
  - `rlib`
  - `cdylib`
  - `staticlib`
- 已导出基础 C ABI：
  - `fusion_abi_version`
  - `fusion_version_string`
  - `fusion_parse_url_json`
  - `fusion_string_free`
- C 头文件：
  - `/Users/qi4l/lang/Rust/Fusion-master/include/fusion.h`

### Phase 6 基础能力
- `src/tunnel/simplex.rs`
  - 已提供 `simplex+http` / SR-ARQ 所需的基础构件：
    - 分片
    - 重组
    - ACK 窗口
    - 重传队列
- `src/tunnel/simplex_http.rs`
  - 已接入最小可运行的 `simplex+http://` direct session
  - 当前覆盖 hello / hello-ack / heartbeat 握手
  - 已接入分片 / 重组
  - 已接入最小片段 ACK / 超时重传
  - 已接入最小滑窗 / 窗口控制（windowed send pipeline）
  - 已接入 batch POST 发送
  - 已接入最小 batch envelope / long-poll receive
  - 已接入重复包抑制（retransmit dedup）
  - 已覆盖基础 frame exchange
  - 暂未接入 mux / relay / `simplex+dns://` / `simplex+oss://`

---

### WSS / TLS

`wss://` 通过 URL query 指定 TLS 参数：

- 服务端监听：
  - `tls-cert=/absolute/path/to/cert.pem`
  - `tls-key=/absolute/path/to/key.pem`
  - `tls-client-ca=/absolute/path/to/client-ca.pem`
- 客户端连接：
  - `tls-ca=/absolute/path/to/ca.pem`：附加自定义 CA
  - `tls-client-cert=/absolute/path/to/client-cert.pem`
  - `tls-client-key=/absolute/path/to/client-key.pem`
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

### HTTP Proxy -> raw

出口节点：

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -r raw:// \
  -a http-exit
```

入口节点：

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -l http://127.0.0.1:8080 \
  -r raw:// \
  -a http-entry
```

### Shadowsocks(minimal) -> raw

出口节点：

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -r raw:// \
  -a ss-exit
```

入口节点：

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -l "ss://127.0.0.1:8388?method=none" \
  -r raw:// \
  -a ss-entry
```

说明：
- 当前为 **最小版 Shadowsocks service**
- 仅支持 TCP 请求头解析与转发
- 当前仅支持 `method=none`，用于先打通 service/runtime 主链路

### 固定端口转发

```bash
cargo run --bin fusion -- \
  -r "port://127.0.0.1:8080->example.com:80" \
  -a port-node
```

### Unix Socket 两节点

终端 A：

```bash
cargo run --bin fusion -- \
  -s unix:///tmp/fusion-a.sock \
  -a unix-a
```

终端 B：

```bash
cargo run --bin fusion -- \
  -c unix:///tmp/fusion-a.sock \
  -a unix-b
```

### Memory transport

`memory://NAME` 当前用于：

- 同进程测试
- 嵌入式/库模式下的内存 transport

当前**不作为跨独立 CLI 进程的持久监听器**使用。

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

## Phase 5（当前已落地的第一版）

- `-x, --proxy-chain <URL>`
  - 当前支持：
    - `socks5://HOST:PORT`
    - `http://HOST:PORT`（CONNECT）
    - `ss://HOST:PORT?method=none`（最小 TCP 版）
- `-f, --front-proxy <URL>`
  - 作为代理链第一跳
- `--conn-policy <fallback|random|round-robin>`
  - 当前已用于多上游 endpoint 的 task / direct / socks5 / http 入口连接择路
- `--up-connect <URL>`
  - 指定上行优先连接池
- `--down-connect <URL>`
  - 指定下行 / relay 优先连接池

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

- `wss://`：当前已支持显式证书/私钥配置与基础 mTLS 参数面，但 TLS 能力仍是精简版，暂未覆盖更完整的证书矩阵与自动协商场景
- `src/tunnel/tls.rs`：当前为实际落地模块，不是空壳
- `-k`：当前为预共享密钥的帧级加密，尚未扩展为设计文档中的完整 wrapper pipeline / 多算法可插拔体系
- `src/crypto/wrapper.rs`：当前已补上多 stage wrapper 基础能力，包含：
  - shared-key AEAD
  - compression
  - padding
  - multi-stage pipeline roundtrip 测试
  - 当前已补上基础 CLI/config 配置面：
    - `--wrap-compress`
    - `--wrap-padding <BYTES>`
  - 但当前仍以基础实现/测试覆盖为主，尚未形成协商式/自动兼容式 wrapper 交付面
- Phase 5 当前是**可交付的第一版实现**：
  - 代理链目前已落地 TCP 基础链路与 HTTP CONNECT / SOCKS5 前置代理
  - 代理链现已支持认证型 HTTP CONNECT / SOCKS5 前置代理：
    - `http://HOST:PORT?username=USER&password=PASS`
    - `socks5://HOST:PORT?username=USER&password=PASS`
  - `conn-policy` 已用于 task / direct / socks5 / http 本地入口的上游择路
  - socks5/http 本地入口当前按“每个 client 按策略选择上游，并在上游失败时切换”
  - socks5/http 本地入口现已加入 **长生命周期上游 mux 连接池**，可复用既有上游 peer
  - 当复用的上游 peer 在 stream 打开阶段失败时，会从连接池剔除并尝试切到下一个上游
  - 上游池在复用前会检查 `SessionHub` / `AgentRegistry` 中的 peer 状态，避免继续复用已关闭连接
  - 上游池现已带周期性清理任务，会定期回收 hub/registry 中已经失活的 peer
  - 更深层的 route/session 感知型热切换仍可继续增强
- `simplex+http://` 当前已不止于基础帧交换：
  - 已可承载 direct task 请求链路
  - mux / relay / `simplex+dns://` / `simplex+oss://` 仍待继续扩展
- `simplex+dns://` / `simplex+oss://` 与 WASM 仍未完成
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
