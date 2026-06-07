# Simplex Transport 交付说明

本文档描述 Fusion 中 Simplex 系列 transport 的当前交付边界，对应 development plan Phase D。

## 1. 能力矩阵

| 能力 | `simplex+http://` | `simplex+dns://` | `simplex+oss://` |
|------|-------------------|------------------|------------------|
| Direct session (hello/heartbeat) | **supported** | **supported** | **supported** |
| Direct task | **supported** | **supported** | **supported** |
| Mux (stream demux) | **supported** | **supported** | **supported** |
| Raw remote service (mux) | **supported** | **supported** | **supported** |
| Relay (stream bridge) | **supported** | **supported** | **supported** |
| Runtime inbound/outbound | **supported** | **supported** | **supported** |
| SOCKS5/HTTP 本地入口 over Simplex | **experimental** | **experimental** | **experimental** |
| Port forward over Simplex | **not implemented** | **not implemented** | **not implemented** |

说明：

- **supported**：已有 runtime 接入与 `cargo test --lib` 回归覆盖。
- **experimental**：底层 mux/relay 可用，但本地 socks5/http 入口尚未专门做 Simplex upstream 集成测试；可通过 `-c simplex+…` + raw/task 链路验证。
- **not implemented**：尚无专门交付与测试。

与 TCP/WS mux 的行为差异：

- Simplex 基于 HTTP 轮询 / UDP DNS / 本地 OSS 目录的 store-and-forward，延迟高于 TCP/WS。
- Stream 帧在 receiver 打开前会缓存在 mux 层（与 TCP mux 一致）。
- `StreamClose` 可跨 relay 传播；错误主要通过 stream close reason 或 task result 体现，无独立 mux-level error frame。

## 2. 最小可运行命令

### 2.1 `simplex+http://` task

```bash
cargo run --bin fusion -- \
  -s simplex+http://0.0.0.0:39090/task \
  -a simplex-http-server

cargo run --bin fusion -- \
  -c simplex+http://127.0.0.1:39090/task \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

### 2.2 `simplex+http://` raw (mux)

```bash
cargo run --bin fusion -- \
  -s simplex+http://0.0.0.0:39091/raw \
  -a simplex-http-raw

cargo run --bin fusion -- \
  -c simplex+http://127.0.0.1:39091/raw \
  -r raw://dynamic
```

### 2.3 `simplex+dns://` / `dns://` task

`dns://` 为 `simplex+dns://` 的等价别名，下列示例可互换 scheme：

```bash
cargo run --bin fusion -- \
  -s dns://0.0.0.0:5353/task.local \
  -a simplex-dns-server

cargo run --bin fusion -- \
  -c dns://127.0.0.1:5353/task.local \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

（亦可使用 `simplex+dns://` URL，行为相同。）

### 2.4 `simplex+oss://` task

```bash
ROOT=/tmp/fusion-simplex-oss
cargo run --bin fusion -- \
  -s "simplex+oss://mesh-server/channel?root=$ROOT" \
  -a simplex-oss-server

cargo run --bin fusion -- \
  -c "simplex+oss://mesh-client/channel?root=$ROOT" \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

### 2.5 Relay 组网（3 跳示例）

各节点配置 `-s` 监听与 `-c` 指向上游 Simplex endpoint，并暴露 `raw://` 或 relay 所需 service。Route 通过 `RouteUpdate` 广播，与 TCP/WS relay 共用 registry 机制。

## 3. `simplex+dns://` 交付边界

**传输模型**

- 基于 UDP DNS 报文承载 Simplex envelope（TXT 或自定义 record 路径，见 `src/tunnel/simplex_dns.rs`）。
- 单 session 对应一个 `(host, path)` 绑定地址。

**可靠性**

- 使用 SR-ARQ：分片、ACK、超时重传、滑窗（`src/tunnel/simplex.rs`）。
- UDP 不保证有序到达；重组层处理乱序与重复（dedup）。

**Payload / 分片**

- 大 frame 自动分片；单 fragment 大小受 DNS UDP payload 限制（通常 ≤ 512–1232 字节有效载荷，取决于路径 MTU）。
- 不适合持续大流量或低延迟场景。

**Poll / ACK / Retry**

- 客户端周期性 poll；服务端 batch 返回 pending 帧。
- 未 ACK 的分片按窗口与超时重传。

**适用场景**

- 受限网络下的小包、低频 task / 控制面。
- 实验性 mux / relay 验证。

**不适用**

- 大文件传输、实时双向流、高 QPS 代理。

## 4. `simplex+oss://` 交付边界

**传输模型**

- 基于本地或共享目录的文件系统 mailbox（`root` query 参数指定根目录）。
- 每个 endpoint 的 `host/path` 映射到目录下的 inbox/outbox 文件。

**可靠性**

- 同样使用 SR-ARQ 分片与 ACK；依赖文件 rename / poll 实现近似 store-and-forward。
- 多进程并发写同一 mesh 时需使用独立 `root` 或不同 channel 名。

**Payload / 分片**

- 适合中等大小 task 结果与 control frame；极大 payload 会放大 I/O 与重组成本。

**Poll / ACK / Retry**

- 通过扫描 outbox 目录 batch 读取；ACK 写入对端可见位置。
- 重传由 ARQ 窗口驱动。

**适用场景**

- 无直连端口的离线/半离线桥接、CI 夹缝环境 smoke test。
- 本地多 hop relay 集成测试（见 `runtime_tests`）。

**不适用**

- 生产级跨公网传输（无内置加密与访问控制，需外层 wrapper/TLS 或 OS 权限隔离）。
- 低延迟交互式 shell（poll 间隔带来明显延迟）。

## 5. 回归测试索引

| 场景 | 测试位置 |
|------|----------|
| HTTP mux 多 stream | `simplex_http_mux::tests` |
| HTTP mux 缓冲 / shared key | `simplex_http_mux::tests` |
| DNS/OSS mux 多 stream / 缓冲 | `simplex_*_mux::tests` |
| HTTP/DNS/OSS 3 跳 relay | `runtime_tests::simplex_*_relay_stream_bridge_roundtrip_through_three_hops` |
| HTTP/DNS/OSS 5 跳 relay | `runtime_tests::simplex_*_relay_stream_bridge_roundtrip_through_five_hops` |
| Direct task | `runtime_tests::outbound_task_over_simplex_*` |
| Runtime session 注册 | `runtime_tests::simplex_*_inbound_runtime_registers_direct_session` |

运行：

```bash
cargo test --lib simplex
```
