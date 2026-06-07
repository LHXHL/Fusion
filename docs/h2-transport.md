# HTTP/2 传输隧道（Phase K）

## URL

| Scheme | 说明 |
|--------|------|
| `h2://HOST:PORT/PATH` | Cleartext HTTP/2（h2c prior knowledge，内网/测试） |
| `h2s://HOST:PORT/PATH` | TLS + ALPN `h2` |

PATH 默认 `/tunnel`。TLS query 与 `wss://` 相同（`tls-cert` / `tls-key` / `tls-ca` / `tls-insecure=1` 等）。

## 能力

| 能力 | 状态 |
|------|------|
| direct session / task | ✅ |
| mux（StreamOpen/Data/Close） | ✅ 单 h2 控制 stream 承载 Fusion 帧 |
| raw 入站 | ✅ |
| relay | ✅ |
| `h2s://` TLS | ✅ |

## 示例

```bash
cargo run --bin fusion -- -s h2://0.0.0.0:39200/tunnel -a h2-server
cargo run --bin fusion -- -c h2://127.0.0.1:39200/tunnel --task-peer <ID> task shell "whoami"
```

## 与 `http://` / `streamhttp://` 区别

- `http://`：HTTP/1.1 长轮询，mux/relay 可用，延迟较高
- `streamhttp://`：SSE + POST，仅 direct task
- `h2://`：单连接 HTTP/2，mux/relay，适合 CDN/反向代理后的长连接

## 实现形态（K3）

- **控制面**（`Content-Type: application/fusion-h2-mux`）：Hello / Heartbeat / RouteUpdate / StreamOpen / StreamClose
- **数据面**（`Content-Type: application/fusion-h2-data` + `X-Fusion-Stream-Id`）：`StreamData` 优先走独立 h2 stream
- **兼容回退**：client 侧 data stream 未就绪时 `StreamData` 暂走 control stream；**server 侧 outbound `StreamData` 始终走 control**（与 relay 回程路径一致）

并发多 stream 见单元测试 `h2_mux_uses_separate_data_streams_for_concurrent_ids`。

> h2 relay 单跳 / 3/5/10 跳集成测试：`app::runtime_tests::h2_relay_stream_bridge_roundtrip*`

实现：`src/tunnel/h2_mux.rs`

## DNS 常规隧道

`dns://` 为 `simplex+dns://` 别名，见 [`simplex-transport.md`](simplex-transport.md)。
