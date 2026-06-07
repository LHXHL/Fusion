# Fusion 回归测试矩阵

本文档将主要能力与 `cargo test --lib` 入口对应，便于按领域回归。

运行全部库测试：

```bash
cargo test --lib
```

按模块过滤示例：

```bash
cargo test --lib app::runtime_tests::
cargo test --lib app::upstream_pool::
cargo test --lib agent::registry::
```

---

## 1. CLI 与配置

| 能力 | 测试 |
|------|------|
| 参数解析 / status 子命令 | `app::cli::tests::parse_*` |
| Phase 5 connect / wrapper 选项 | `app::cli::tests::parse_phase5_connect_options`, `parse_wrapper_options` |
| up-/down- URL 前缀 | `app::config::tests::merge_connect_endpoints`, `parse_tunnel_endpoint_strips_up_and_down_prefixes` |
| runtime bootstrap | `app::runtime_bootstrap::tests::*` |
| conn hub / conn policy | `app::conn_hub::tests::*` |

## 2. Route 与 Relay

| 能力 | 测试 |
|------|------|
| route 生命周期 / 选优 / 收敛 | `agent::registry::tests::*` |
| route 校验 / path 保留 | `app::runtime_relay::tests::*` |
| TCP 3/5/10 跳 relay | `app::runtime_tests::tcp_relay_stream_bridge_roundtrip_through_*` |
| WS relay | `app::runtime_tests::ws_relay_stream_bridge_roundtrip` |
| **h2 relay 3/5/10 跳** | `app::runtime_tests::h2_relay_stream_bridge_roundtrip*` |
| **dns relay 3-hop** | `app::runtime_tests::dns_relay_stream_bridge_roundtrip_through_three_hops` |
| Simplex HTTP/DNS/OSS 3/5/10 跳 relay | `app::runtime_tests::simplex_*_relay_stream_bridge_roundtrip_through_*` |
| socks5/http over relay | `app::runtime_tests::tcp_socks5_over_relay_roundtrip`, `tcp_http_proxy_over_relay_roundtrip`, `ws_*_over_relay_roundtrip` |
| socks5 over Simplex HTTP | `app::runtime_tests::simplex_http_socks5_over_raw_roundtrip` |
| port forward over Simplex HTTP | `app::runtime_tests::simplex_http_port_forward_over_raw_roundtrip` |
| trojan over mux → raw | `app::runtime_tests::tcp_trojan_over_raw_roundtrip` |
| Trojan 协议解析 | `serve::trojan::tests::*` |

## 3. Upstream 连接池与 Failover

| 能力 | 测试 |
|------|------|
| mux peer 复用 | `app::upstream_pool::tests::tcp_upstream_pool_reuses_existing_mux_peer` |
| stale peer 剔除 | `app::upstream_pool::tests::tcp_upstream_pool_drops_stale_peer_before_reuse` |
| 后台 prune | `app::upstream_pool::tests::tcp_upstream_pool_prune_stale_removes_closed_entries` |
| endpoint failover | `app::upstream_pool::tests::tcp_upstream_pool_failover_skips_unreachable_endpoint` |
| round-robin 择路 | `app::upstream_pool::tests::tcp_upstream_pool_round_robin_policy_acquires_successfully` |
| task 多上游 failover | `app::runtime_tests::outbound_task_fails_over_to_second_tcp_endpoint` |

## 4. Simplex Transport

| 能力 | 测试 |
|------|------|
| HTTP direct task | `app::runtime_tests::outbound_task_over_simplex_http_endpoint_succeeds` |
| **`http://` 长轮询 task** | `app::runtime_tests::outbound_task_over_http_endpoint_succeeds` |
| **`streamhttp://` SSE task** | `app::runtime_tests::outbound_task_over_streamhttp_endpoint_succeeds`, `tunnel::streamhttp::tests::streamhttp_task_roundtrip` |
| DNS direct task | `app::runtime_tests::outbound_task_over_simplex_dns_endpoint_succeeds`, `outbound_task_over_dns_endpoint_succeeds` |
| OSS direct task | `app::runtime_tests::outbound_task_over_simplex_oss_endpoint_succeeds` |
| HTTP inbound session | `app::runtime_tests::simplex_http_inbound_runtime_registers_direct_session` |
| OSS inbound session | `app::runtime_tests::simplex_oss_inbound_runtime_registers_direct_session` |
| SR-ARQ / mux 单元 | `tunnel::simplex::tests::*`, `tunnel::simplex_http_mux::tests::*`, `tunnel::simplex_dns_mux::tests::*`, `tunnel::simplex_oss_mux::tests::*` |

边界说明见 [`simplex-transport.md`](simplex-transport.md)。`http://` / `streamhttp://` 见 [`http-transport.md`](http-transport.md)。

## 4.1 HTTP/2 与 DNS 别名（Phase K）

| 能力 | 测试 |
|------|------|
| `dns://` URL 解析 | `app::config::tests::parse_dns_tunnel_url` |
| `dns://` direct task | `app::runtime_tests::outbound_task_over_dns_endpoint_succeeds` |
| `h2://` URL 解析 | `app::config::tests::parse_h2_tunnel_url` |
| `h2://` direct task | `app::runtime_tests::outbound_task_over_h2_endpoint_succeeds` |
| h2 mux 单元 | `tunnel::h2_mux::tests::*`（含 `h2_mux_uses_separate_data_streams_for_concurrent_ids`） |
| h2 relay 单跳 / 3/5/10 跳 | `app::runtime_tests::h2_relay_stream_bridge_roundtrip*` |
| h2s TLS / mTLS | `tunnel::h2_mux::tests::h2s_handshake_*`, `tunnel::tls::tests::h2s_*` |
| dns relay 3-hop（`dns://`） | `app::runtime_tests::dns_relay_stream_bridge_roundtrip_through_three_hops` |
| fusion-logic URL | `cargo test -p fusion-logic url::tests::parse_dns_tunnel_url`, `parse_h2_tunnel_urls` |

详见 [`h2-transport.md`](h2-transport.md)。

## 5. Tunnel 与会话

| 能力 | 测试 |
|------|------|
| TCP / WS / WSS mux | `tunnel::tcp_mux::tests::*`, `tunnel::ws_mux::tests::*` |
| TLS 参数面 | `tunnel::tls::tests::*` |
| UDP / Unix / Memory | `tunnel::udp::tests::*`, `tunnel::unix::tests::*`, `tunnel::memory::tests::*` |
| listener 绑定 | `tunnel::listener::tests::*` |
| UDP inbound runtime | `app::runtime_tests::udp_inbound_runtime_registers_direct_session` |

## 6. 加密与 Wrapper

| 能力 | 测试 |
|------|------|
| AEAD / KEX | `crypto::aead::tests::*`, `crypto::kex::tests::*` |
| transport frame | `crypto::transport::tests::*` |
| wrapper pipeline | `crypto::wrapper::tests::*` |

## 7. Serve 层

| 能力 | 测试 |
|------|------|
| SOCKS5 解析 / 认证 | `serve::socks5::tests::*` |
| HTTP proxy 解析 | `serve::http::tests::*` |
| Shadowsocks `none` / AEAD 请求帧 | `serve::shadowsocks::tests::*` |
| raw / port forward | `serve::raw::tests::*`, `serve::portfwd::tests::*` |

## 8. Task

| 能力 | 测试 |
|------|------|
| artifact 落盘 | `app::runtime_task::tests::*` |
| task save 路径 | `app::runtime_tests::task_artifact_save_path_override_is_used` |

## 9. 可观测性与 Status

| 能力 | 测试 |
|------|------|
| status 快照写入 / 过滤 | `app::runtime_tests::status_snapshot_is_written_and_filtered` |
| config + recent errors 渲染 | `app::runtime_status::tests::render_status_lines_includes_config_and_recent_errors` |
| TLS 摘要（无 PEM 路径） | `tunnel::tls::tests::summarize_tls_usage_counts_wss_flags_without_paths` |
| port forward / simplex 模式 | `app::runtime_mode::tests::direct_port_forward_skipped_when_simplex_connect_configured` |
| route 事件写入 recent errors | `agent::registry::tests::registry_records_route_switch_in_recent_errors` |
| wrapper capability 标签 | `agent::identity::tests::capability_labels_include_shared_key_wrapper` |

`upstream_pools` 字段由运行时 socks5/http/shadowsocks 上游池在 acquire/invalidate/prune 时写入；可通过 `status --json` 或 `runtime-status.json` 验证。

## 10. 平台化（手动 / 示例）

| 能力 | 验证方式 |
|------|----------|
| C ABI v2 | 构建 `cargo build --lib`；运行 `examples/c_host` |
| Logic API v1 | `cargo test -p fusion-logic`；`ffi::tests::ffi_logic_api_validate_and_filter_work` |
| WASM W1 | `cargo build -p fusion-logic --target wasm32-unknown-unknown --features wasm` |
| Python ctypes | 运行 `examples/python_host/demo.py` |
| 嵌入 API | 见 [`embedding.md`](embedding.md)、[`abi-stability.md`](abi-stability.md) |

库测试不覆盖完整 FFI 生命周期；集成验证依赖上述示例。

---

## 相关文档

- 能力基线：[`baseline-current-capabilities.md`](baseline-current-capabilities.md)
- 开发规划：[`development-plan.md`](development-plan.md)
- TLS 边界：[`tls-transport.md`](tls-transport.md)
- Trojan 边界：[`trojan-transport.md`](trojan-transport.md)
