# Trojan 本地入口

H3 选型：**实现 Trojan**；`externalc2://` 仍为 Cobalt Strike 专用管道协议，不在本阶段交付（见文末）。

实现入口：

- 协议解析：[`src/serve/trojan.rs`](../src/serve/trojan.rs)
- 运行时挂载：[`src/app/runtime_trojan.rs`](../src/app/runtime_trojan.rs)

---

## 1. URL 与配置

```text
trojan://127.0.0.1:443?password=secret
trojan://0.0.0.0:443?password=secret&tls-cert=/path/cert.pem&tls-key=/path/key.pem
```

| 参数 | 必填 | 说明 |
|------|------|------|
| `password` | 是 | 客户端发送 `hex(SHA224(password))`（56 字符小写 hex） |
| `tls-cert` / `tls-key` | 否 | 同时配置时在本地入口启用 TLS（与 WSS 相同 PEM 语义） |
| `tls-client-ca` | 否 | 启用 listener mTLS |

典型 Fusion 组合（经 mux 转发到远端 `raw://`）：

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:35000 \
  -l trojan://127.0.0.1:443?password=secret \
  -r raw:// \
  -a entry-node
```

---

## 2. 协议边界（与官方 Trojan 对齐部分）

握手后首包格式：

```text
[56 byte hex SHA224(password)][CRLF][CMD=0x01 TCP][ATYP+ADDR+PORT][CRLF][payload...]
```

| 能力 | 状态 |
|------|------|
| TCP CONNECT (`CMD=1`) | ✅ |
| UDP ASSOCIATE (`CMD=3`) | ❌ 未实现 |
| 明文 TCP 入口（无 TLS） | ✅ 测试/内网 |
| TLS 入口（`tls-cert`/`tls-key`） | ✅ |
| 与 Fusion mux + 远端 `raw://` | ✅ |
| WS 上游 `-c wss://...` | ✅ `TrojanWs` 模式 |

密码校验失败返回 `PermissionDenied`，不会继续解析目标地址。

---

## 3. 与 socks5 / Shadowsocks 的关系

运行时模式：`OutboundRuntimeMode::TrojanTcp` / `TrojanWs`（需 `-l trojan://...` + `-r raw://` 或 `port://...`）。

优先级与 ss/http 相同：在 `decide_outbound_runtime_mode` 中位于 Shadowsocks 之后、Relay 之前。

---

## 4. ExternalC2 为何未实现

`externalc2://` 面向 CS Beacon 的命名管道 / SMB 中继，与 Fusion 当前「本地 TCP 代理入口 + mux 隧道」模型差异较大。若后续有明确宿主集成需求，应单独立项（帧格式、任务通道、与 `task` 子系统边界）。

---

## 5. 回归测试

| 场景 | 测试 |
|------|------|
| URL / 密码 hash / 请求帧 | `serve::trojan::tests::*` |
| mux 到远端 raw 往返 | `app::runtime_tests::tcp_trojan_over_raw_roundtrip` |

---

## 6. 相关文档

- 能力基线：[`baseline-current-capabilities.md`](baseline-current-capabilities.md)
- SOCKS/RAW 组合：[`socks5-and-raw.md`](socks5-and-raw.md)
- TLS PEM 参数：[`tls-transport.md`](tls-transport.md)
