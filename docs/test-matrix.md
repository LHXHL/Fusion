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
| runtime status/control 抽离 | `src/app/runtime_status.rs` + `runtime_status_snapshot_roundtrip` | 已覆盖 |
| runtime relay/announce 抽离 | `src/app/runtime_relay.rs` + relay runtime tests | 已覆盖 |
| listener/connect 模式分流抽离 | `src/app/runtime_mode.rs` 测试 | 已覆盖 |

## Phase 5

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| TCP tunnel | `src/tunnel/tcp.rs` / `src/tunnel/tcp_mux.rs` 测试 | 已覆盖 |
| WS tunnel | `src/tunnel/ws.rs` / `src/tunnel/ws_mux.rs` 测试 | 已覆盖 |
| WSS tunnel | `wss_mux_handshake_and_stream_roundtrip_with_insecure_client` + `src/tunnel/tls.rs` 测试 | 已覆盖 |
| keyed mux | `mux_peer_routes_frames_by_stream_id_with_shared_key` | 已覆盖 |
| listener / dialer 抽象 | `src/tunnel/listener.rs` / `src/tunnel/dialer.rs` | 已覆盖 |

## Phase 6

| 验收点 | 当前证据 | 状态 |
|---|---|---|
| socks5 -> raw | `tcp_socks5_over_relay_roundtrip` / `ws_socks5_over_relay_roundtrip` | 已覆盖 |
| `port://...->...` 解析与转发 | `src/serve/portfwd.rs` 测试 | 已覆盖 |

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
| 旧三端源码删除 | `src/fusion_server` / `src/fusion_client` / `src/fusion_implant` 不存在 | 已覆盖 |
| 文档面向单体 Agent | README + docs | 已覆盖 |
