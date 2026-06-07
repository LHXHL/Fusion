# HTTP 传输隧道

本文档描述 Phase G 新增的常规 HTTP 隧道，以及与 Simplex、UDP 的边界。

## 1. `http://` 长轮询隧道（G1）

### 语义

- **上行**：客户端 `POST` JSON envelope（与 `simplex+http://` 相同 SR-ARQ 分包格式）
- **下行**：服务端 `GET` 长轮询返回 JSON envelope（204 表示暂无数据）

### 与 `simplex+http://` 的关系

| 项目 | `http://` | `simplex+http://` |
|------|-----------|-------------------|
| 协议栈 | 相同（复用 `src/tunnel/simplex_http.rs`） | 原始 scheme |
| mux / relay | 支持（与 simplex+http 相同 runtime 路径） | 支持 |
| 用途 | 常规 rem 风格 URL | 历史/显式 Simplex 命名 |

### 示例

```bash
# 监听
cargo run --bin fusion -- -s http://0.0.0.0:39090/task -a http-server

# 连接 + task
cargo run --bin fusion -- \
  -c http://127.0.0.1:39090/task \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

### 与本地 HTTP 代理的区别

- `local_serves = ["http://127.0.0.1:8080"]`：**正向 HTTP 代理入口**（应用层）
- `connects = ["http://127.0.0.1:39090/tunnel"]`：**传输隧道**（tunnel 层）

二者 scheme 相同但配置字段不同，不会冲突。

---

## 2. `streamhttp://` SSE + POST（G2）

### 语义

- **上行**：`POST` JSON envelope（整帧 transport payload，无 SR-ARQ 分片）
- **下行**：`GET` + `Accept: text/event-stream`，SSE `data:` 行承载 JSON envelope

### 适用场景

- CDN / 反向代理对 **SSE 下行** 更友好
- 单跳 **direct task** 已验证；mux / relay **尚未接入**

### 示例

```bash
cargo run --bin fusion -- -s streamhttp://0.0.0.0:39100/task -a streamhttp-server

cargo run --bin fusion -- \
  -c streamhttp://127.0.0.1:39100/task \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

### 当前限制

- 无 SR-ARQ / 分片；大 payload 需后续扩展
- 入站 listener 当前为单会话 accept 模型（与 mux 化 simplex 不同）
- 不支持 relay / mux（后续 Phase 可扩展）

---

## 3. URL 方向前缀（G4）

与 `--up-connect` / `--down-connect` 等价，可在 `-c` 或 `connects` 中使用：

```bash
cargo run --bin fusion -- \
  -c up-tcp://127.0.0.1:34996 \
  -c down-ws://127.0.0.1:38080/tunnel
```

等价于：

```bash
cargo run --bin fusion -- \
  --up-connect tcp://127.0.0.1:34996 \
  --down-connect ws://127.0.0.1:38080/tunnel
```

前缀规则：

| 前缀 | 落入池 |
|------|--------|
| （无） | `connects` |
| `up-` | `up_connects` |
| `down-` | `down_connects` |

---

## 4. UDP 与 KCP 边界（G3）

| 能力 | 状态 |
|------|------|
| `udp://` datagram 会话 | **已交付**（direct session，无应用层复用） |
| KCP 可靠层 | **未实现** |
| rem 级 UDP 可靠语义 | 需独立 KCP 层立项，不与现有 `udp://` 混称 |

现有 `udp://` 适用于轻量 datagram 握手与 sandbox transport（`icmp://` / `wg://` 复用语义），**不能**替代 TCP 级可靠 mux。

---

## 5. 回归测试

```bash
cargo test --lib outbound_task_over_http
cargo test --lib outbound_task_over_streamhttp
cargo test --lib streamhttp_task_roundtrip
cargo test --lib merge_connect_endpoints
cargo test --lib classify_http_long_poll
```

完整索引见 [`test-matrix.md`](test-matrix.md)。
