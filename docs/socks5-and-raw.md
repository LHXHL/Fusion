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
