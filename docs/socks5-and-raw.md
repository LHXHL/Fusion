# SOCKS5 / RAW / PORT 示例

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
