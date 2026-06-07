# SOCKS5 / RAW / PORT 示例

## RAW 语义说明

### `raw://HOST:PORT`
表示固定出口目标。

例如：

```bash
-r raw://example.com:80
```

含义：
- 当前节点暴露一个 raw TCP 出口
- 目标地址固定为 `example.com:80`
- 上游即使发来其它目标，也会被该固定目标覆盖

### `raw://`
表示动态出口目标。

例如：

```bash
-r raw://
```

含义：
- 当前节点暴露一个 raw TCP 出口
- 不预先固定目标地址
- 真正连接到哪里，由上游发起 `StreamOpen` 或 SOCKS5 CONNECT 时携带的目标地址决定

典型场景：
- 本地入口是 `socks5://127.0.0.1:1080`
- 远端出口是 `raw://`
- 用户访问任意 `host:port`
- 则远端节点会按该请求动态建立对应的 TCP 连接

## 单跳：本地 socks5 -> 远端 raw

### 出口节点

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -r raw:// \
  -a exit-node
```

### 入口节点

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -l socks5://127.0.0.1:1080 \
  -r raw:// \
  -a entry-node
```

## Simplex HTTP：本地 socks5 -> 远端 raw

入口节点也可以通过 `simplex+http://` mux 承载 socks5 本地入口：

```bash
cargo run --bin fusion -- \
  -c simplex+http://127.0.0.1:39090/socks \
  -l socks5://127.0.0.1:1080 \
  -r raw:// \
  -a entry-node
```

边界：
- 当前已验证 socks5 over `simplex+http://` 到远端 `raw://`
- http proxy over Simplex 仍待后续扩展

## Simplex HTTP：port forward -> 远端固定目标

仅配置 `port://` 远端服务并通过 `simplex+http://` 出站时，本地监听端口经 mux 隧道转发到固定 target（不再在本机直接 TCP 连接 target）：

```bash
cargo run --bin fusion -- \
  -c simplex+http://127.0.0.1:39090/port \
  -r "port://127.0.0.1:9000->127.0.0.1:22" \
  -a leaf-node
```

对端需以 mux/raw 处理 `StreamOpen`（与 socks5 出口相同路径）。

## Shadowsocks

当前支持两种本地入口：

```bash
# 明文请求头（测试/内网）
-l ss://127.0.0.1:8388?method=none

# Fusion AEAD 请求帧
-l "ss://127.0.0.1:8388?method=aes-256-gcm-siv&password=secret"
```

说明：
- `aes-256-gcm-siv` 保护首个 Shadowsocks 地址请求帧
- 当前不是完整 Shadowsocks UDP associate，也不承诺与所有 Shadowsocks 客户端逐字节兼容

## Trojan

```bash
# 明文 TCP 入口（测试/内网）
-l trojan://127.0.0.1:443?password=secret

# 生产建议加 TLS
-l "trojan://127.0.0.1:443?password=secret&tls-cert=/path/cert.pem&tls-key=/path/key.pem"
```

与 socks5/ss 相同，需配对 `-r raw://` 与 `-c tcp://`（或 `wss://`）上游。详见 [`trojan-transport.md`](trojan-transport.md)。

## 多跳：经 relay 到远端 raw

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:35000 \
  -l socks5://127.0.0.1:1080 \
  -r raw:// \
  --remote-peer <EXIT_AGENT_ID> \
  -a entry-node
```

## 固定端口转发

```bash
cargo run --bin fusion -- \
  -r "port://127.0.0.1:8080->example.com:80" \
  -a port-node
```

说明：
- `port://...->...` 当前表示当前 Agent 启动一个固定 TCP 监听并转发到固定目标
- 同时该服务也会出现在 `services list` / route announce 中

## 查看状态

```bash
cargo run --bin fusion -- status streams
cargo run --bin fusion -- services list
```
