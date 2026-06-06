# Fusion 测试矩阵（按项目计划阶段映射）

## Phase 0

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| 基线文档存在 | `docs/baseline-current-capabilities.md` | 已覆盖 |
| smoke 脚本存在 | `scripts/regression/manual-smoke.sh` | 已覆盖 |

## Phase 1

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| `fusion --help` 可用 | `cargo run --bin fusion -- --help` | 已覆盖 |
| URL 解析可用 | `src/utils/url.rs` 单元测试 | 已覆盖 |
| `-k` 预共享密钥帧封装 | `src/crypto/transport.rs` 测试 | 已覆盖 |
| smoke 回归入口 | `scripts/regression/manual-smoke.sh` | 已覆盖 |

## Phase 2

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| crypto 抽离 | `src/crypto/aead.rs` / `src/crypto/kex.rs` | 已覆盖 |
| identity 抽离 | `src/agent/identity.rs` 测试 | 已覆盖 |
| task capability 抽离 | `src/task/*` + `src/task/dispatcher.rs` 测试 | 已覆盖 |
| runtime task artifact 抽离 | `src/app/runtime_task.rs` 测试 | 已覆盖 |

## Phase 3

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| hello / heartbeat / task echo | `src/tunnel/tcp.rs`、`src/tunnel/ws.rs`、`src/task/dispatcher.rs` 测试 | 已覆盖 |
| keyed hello / frame exchange | `tcp_peer_can_exchange_frames_with_shared_key` / `ws_peer_can_exchange_frames_with_shared_key` | 已覆盖 |
| frame codec roundtrip | `src/protocol/codec.rs` 测试 | 已覆盖 |

## Phase 4

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| PeerSession / SessionHub | `src/session/peer.rs` / `src/session/hub.rs` | 已覆盖 |
| reconnect 退避策略 | `src/session/reconnect.rs` 测试 | 已覆盖 |
| runtime status/control 抽离 | `src/app/runtime_status.rs` | 已覆盖 |
| runtime relay/announce 抽离 | `src/app/runtime_relay.rs` + relay runtime tests | 已覆盖 |
| listener/connect 模式分流抽离 | `src/app/runtime_mode.rs` 测试 | 已覆盖 |

## Phase 5

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| TCP tunnel | `src/tunnel/tcp.rs` / `src/tunnel/tcp_mux.rs` 测试 | 已覆盖 |
| WS tunnel | `src/tunnel/ws.rs` / `src/tunnel/ws_mux.rs` 测试 | 已覆盖 |
| WSS tunnel | `wss_mux_handshake_and_stream_roundtrip_with_insecure_client` + `src/tunnel/tls.rs` 测试 | 已覆盖 |
| UDP tunnel | `src/tunnel/udp.rs` + smoke 中的 UDP 互连 | 已覆盖 |
| Unix tunnel | `src/tunnel/unix.rs` 测试 | 已覆盖 |
| Memory tunnel | `src/tunnel/memory.rs` 测试 | 已覆盖 |
| icmp / wg sandbox tunnel | `src/tunnel/dialer.rs` / `src/tunnel/listener.rs` / `src/app/runtime_mode.rs` 测试 | 已覆盖 |
| HTTP CONNECT proxy chain | `src/tunnel/proxy.rs` 测试 | 已覆盖 |
| keyed mux | `mux_peer_routes_frames_by_stream_id_with_shared_key` | 已覆盖 |
| listener / dialer 抽象 | `src/tunnel/listener.rs` / `src/tunnel/dialer.rs` | 已覆盖 |
| Conn policy / up-down pool parsing | `src/app/cli.rs` + `src/app/conn_hub.rs` 测试 | 已覆盖 |
| 服务级上游择路 | `src/app/runtime_socks5.rs` / `src/app/runtime_http.rs` 编排 + smoke 主链路 | 已覆盖 |
| 上游 mux 连接复用 | `src/app/upstream_pool.rs` 测试 | 已覆盖 |
| 复用连接失效剔除 | `src/app/runtime_socks5.rs` / `src/app/runtime_http.rs` failover 编排 | 已覆盖 |
| 复用前活性校验 | `src/app/upstream_pool.rs` stale peer 测试 | 已覆盖 |
| 周期性后台清理 | `src/app/upstream_pool.rs` prune stale 测试 + runtime 维护任务 | 已覆盖 |

## Phase 6

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| socks5 -> raw | `tcp_socks5_over_relay_roundtrip` / `ws_socks5_over_relay_roundtrip` | 已覆盖 |
| http proxy -> raw | `tcp_http_proxy_over_relay_roundtrip` / `ws_http_connect_over_relay_roundtrip` + smoke | 已覆盖 |
| `port://...->...` 解析与转发 | `src/serve/portfwd.rs` 测试 | 已覆盖 |
| WSS mTLS 握手与流转发 | `wss_mux_handshake_and_stream_roundtrip_with_mutual_tls` | 已覆盖 |

## Phase 7

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| task shell/screenshot/file | `src/task/dispatcher.rs` + runtime task 路径 | 已覆盖 |

## Phase 8

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| relay 基础转发 | `tcp_relay_stream_bridge_roundtrip` / `ws_relay_stream_bridge_roundtrip` | 已覆盖 |
| route registry | `src/agent/registry.rs` / `src/session/router.rs` 测试 | 已覆盖 |

## Phase 9

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| peers/routes/services/task 控制面 | `fusion` CLI 子命令 | 已覆盖 |
| 本地状态视图 | `runtime-status.json` + `status` 命令 | 已覆盖 |

## Phase 10

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| 单一二进制 | `src/bin/fusion.rs` | 已覆盖 |
| C ABI 基础导出 | `src/ffi.rs` 测试 + `cargo build --lib` | 已覆盖 |
| simplex / SR-ARQ 基础层 | `src/tunnel/simplex.rs` + `src/tunnel/simplex_http.rs`（握手 / 分片 / ACK重传 / 窗口发送 / batch send-receive / dedup / frame exchange）测试 | 已覆盖 |
| simplex+http direct task | `outbound_task_over_simplex_http_endpoint_succeeds` | 已覆盖 |
| 认证代理链 | `connects_via_authenticated_http_and_socks5_proxy_chain` | 已覆盖 |
| task 多端点 failover | `outbound_task_fails_over_to_second_tcp_endpoint` | 已覆盖 |
| wrapper pipeline 多 stage 基础层 | `src/crypto/wrapper.rs` / `src/crypto/transport.rs`（compression / padding / AEAD 组合）测试 | 已覆盖 |
| 文档面向单体 Agent | README + docs | 已覆盖 |
