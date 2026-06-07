# Quick Start

本文档只描述当前 `fusion` 单体入口的真实使用方式。

## 1. 构建

```bash
cargo build --bin fusion
```

## 2. 查看帮助

```bash
cargo run --bin fusion -- --help
```

## 3. 起一个监听节点

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -a node-a
```

## 4. 起一个连接节点

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -a node-b
```

## 5. HTTP Proxy 动态出口

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
  -l http://127.0.0.1:8080 \
  -r raw:// \
  -a entry-node
```

## 6. UDP 直连

监听端：

```bash
cargo run --bin fusion -- \
  -s udp://0.0.0.0:39040 \
  -a udp-a
```

连接端：

```bash
cargo run --bin fusion -- \
  -c udp://127.0.0.1:39040 \
  -a udp-b
```

## 7. Unix Socket 直连

监听端：

```bash
cargo run --bin fusion -- \
  -s unix:///tmp/fusion.sock \
  -a unix-a
```

连接端：

```bash
cargo run --bin fusion -- \
  -c unix:///tmp/fusion.sock \
  -a unix-b
```

## 8. Memory transport

`memory://NAME` 当前用于：
- 同进程测试
- 嵌入式/库模式

不建议把它当作两个独立 CLI 进程之间的 transport。

## 9. icmp / wg（sandbox datagram transport）

```bash
cargo run --bin fusion -- -s icmp://127.0.0.1:39050 -a icmp-a
cargo run --bin fusion -- -c icmp://127.0.0.1:39050 -a icmp-b

cargo run --bin fusion -- -s wg://127.0.0.1:39060 -a wg-a
cargo run --bin fusion -- -c wg://127.0.0.1:39060 -a wg-b
```

说明：
- 当前为 sandbox 兼容 datagram transport
- 不等同于真实内核 ICMP / WireGuard 协议栈

## 10. 查看状态

```bash
cargo run --bin fusion -- status
cargo run --bin fusion -- status --json
cargo run --bin fusion -- peers list
cargo run --bin fusion -- routes list
cargo run --bin fusion -- services list
```

## 11. 构建库与 C ABI

```bash
cargo build --lib
```

产物默认包含：
- `rlib`
- `cdylib`
- `staticlib`

头文件：
- [`include/fusion.h`](../include/fusion.h)

## 12. Phase 5 第一版：代理链与连接池

```bash
cargo run --bin fusion -- \
  --up-connect tcp://127.0.0.1:34996 \
  --down-connect ws://127.0.0.1:38080/tunnel \
  --conn-policy round-robin \
  -x socks5://127.0.0.1:1080 \
  -f http://127.0.0.1:8080
```

说明：
- `-f` 会作为代理链第一跳
- `-x` 可追加后续跳点
- 当前 `conn-policy` 主要作用于多上游 endpoint 的 task / direct 路径
- `-c up-tcp://...` / `-c down-ws://...` 与 `--up-connect` / `--down-connect` 等价（见 [docs/http-transport.md](http-transport.md)）

## 13.1 HTTP / streamhttp 隧道

```bash
# http:// 长轮询（与 simplex+http 同协议栈）
cargo run --bin fusion -- -s http://0.0.0.0:39090/task -a http-server
cargo run --bin fusion -- -c http://127.0.0.1:39090/task --task-peer <PEER_ID> task shell "whoami"

# streamhttp:// SSE 下行 + POST 上行
cargo run --bin fusion -- -s streamhttp://0.0.0.0:39100/task -a sse-server
cargo run --bin fusion -- -c streamhttp://127.0.0.1:39100/task --task-peer <PEER_ID> task shell "whoami"
```

详见 [docs/http-transport.md](http-transport.md)。

## 14. 使用配置文件

```bash
cp fusion.toml.example fusion.toml
cargo run --bin fusion -- --config ./fusion.toml
```

注意：
- 命令行参数优先
- `fusion.toml` 只做默认值来源
- 状态快照写到 `data-dir`

## 14. Wrapper Pipeline 基础配置

```bash
cargo run --bin fusion -- \
  -s tcp://127.0.0.1:34996 \
  -k fusion-secret \
  --wrap-compress \
  --wrap-padding 32
```

说明：
- `--wrap-compress`：启用 transport payload 压缩
- `--wrap-padding <BYTES>`：增加固定额外 padding
- 当前要求通信双方配置一致

## 15. 本地代理认证

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -l "socks5://127.0.0.1:1080?username=demo&password=secret" \
  -l "http://127.0.0.1:8080?username=demo&password=secret" \
  -r raw://
```

说明：
- `socks5://...?username=...&password=...`：启用 SOCKS5 用户名密码认证
- `http://...?username=...&password=...`：启用 HTTP Basic 代理认证
- 未配置 `username/password` 时，行为保持为无认证入口

## 16. simplex+http task

```bash
cargo run --bin fusion -- \
  -s simplex+http://0.0.0.0:39090/task \
  -a simplex-server

cargo run --bin fusion -- \
  -c simplex+http://127.0.0.1:39090/task \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

说明：
- `simplex+http://` 已支持 direct task、mux、raw service 与 relay
- 详见 [docs/simplex-transport.md](simplex-transport.md)

## 17. simplex+dns / simplex+oss task

```bash
# DNS
cargo run --bin fusion -- \
  -s simplex+dns://0.0.0.0:5353/task.local \
  -a simplex-dns-server

cargo run --bin fusion -- \
  -c simplex+dns://127.0.0.1:5353/task.local \
  --task-peer <PEER_ID> \
  task shell "whoami"

# OSS（需共享 root 目录）
ROOT=/tmp/fusion-simplex-oss
cargo run --bin fusion -- \
  -s "simplex+oss://mesh-server/channel?root=$ROOT" \
  -a simplex-oss-server

cargo run --bin fusion -- \
  -c "simplex+oss://mesh-client/channel?root=$ROOT" \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

说明：
- 两类 transport 均已接入 runtime、mux 与 relay
- `dns://` 为 `simplex+dns://` 等价别名（可将 DNS 示例中的 scheme 互换）
- 延迟与 payload 限制见 [docs/simplex-transport.md](simplex-transport.md)

## 18. h2:// HTTP/2 mux 隧道

```bash
cargo run --bin fusion -- \
  -s h2://0.0.0.0:39200/tunnel \
  -a h2-server

cargo run --bin fusion -- \
  -c h2://127.0.0.1:39200/tunnel \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

TLS 示例（`h2s://`）：

```bash
cargo run --bin fusion -- \
  -s "h2s://0.0.0.0:443/tunnel?tls-cert=/path/cert.pem&tls-key=/path/key.pem" \
  -a h2s-server
```

说明：
- 支持 direct task、mux、raw 入站与 relay
- 详见 [docs/h2-transport.md](h2-transport.md)
