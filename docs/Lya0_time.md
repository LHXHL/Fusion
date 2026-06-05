# Lya0 Progress Checkpoint

本轮推进记录：

- 已完成：HTTP Proxy service 主链接入
  - 已把 `http://HOST:PORT` 本地代理入口接入 `service/runtime` 主链
  - 已新增并接入：`/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_http.rs`
  - 已在 orchestrator / runtime mode / service definition 中接好分流
- 已完成：HTTP Proxy service 定义与解析补强
  - 本地 service definition 支持 `http://HOST:PORT`
  - remote egress 仍复用 `raw://` / `port://`
  - `CONNECT host:port`
  - `CONNECT [IPv6]:port`
  - absolute-form -> origin-form 改写
  - hop-by-hop header 清理
  - 二进制 body 前缀保留
- 已完成：HTTP Proxy / socks5 本地桥接逻辑第一轮抽共用
  - 新增：`/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_bridge.rs`
  - 抽出共用能力：
    - `StreamData` frame 构造
    - `StreamClose` frame 构造
    - 读取远端 `StreamData` 并写回本地 client
    - close-ack 校验
  - `runtime_http.rs` 与 `runtime_socks5.rs` 已改为复用该桥接辅助逻辑
  - 这一步减少了 HTTP / socks5 两套本地代理入口中的重复数据面代码
- 已完成：HTTP Proxy 关键 E2E 回归补齐
  - `ws_http_connect_over_relay_roundtrip` 已通过，证明 HTTP CONNECT 可经 WS relay 走通
  - `tcp_http_proxy_over_relay_roundtrip` 已通过，证明普通 HTTP request/response 也可经 TCP relay 走通
- 已完成：UDP tunnel 最小骨架第一轮
  - 新增：`/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/udp.rs`
  - 已实现：
    - UDP datagram frame 收发
    - UDP 版 hello / hello-ack / heartbeat / heartbeat-ack
    - `ActiveUdpPeer`
    - `accept_peer` / `connect_peer`
    - `run_inbound_session_once` / `run_outbound_session_once`
  - 已在模块导出中接入：`/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/mod.rs`
  - 已补 URL 解析覆盖：`udp://HOST:PORT`
- 已完成：`udp://` 进入 listener / dialer / runtime mode 的最小主链
  - `bind_endpoint` 已支持 UDP listener
  - `classify_endpoint` 已支持 UDP dial target
  - `InboundRuntimeMode` 已新增 `DirectUdp`
  - `runtime_orchestrator` 已支持：
    - inbound UDP direct session 注册到 `SessionHub` / `AgentRegistry`
    - outbound UDP direct session 注册到 `SessionHub` / `AgentRegistry`
  - 当前策略仍是最小 direct path，尚未接 mux / relay / service / task
- 已完成：UDP mux / stream routing 最小骨架第一轮
  - 新增：`/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/udp_mux.rs`
  - 已实现：
    - `MuxUdpPeer`
    - `open_stream_receiver`
    - `read_control_frame`
    - `read_stream_open_frame`
    - `read_stream_open`
    - 按 `stream_id` 分发 datagram frame
    - receiver 未注册前的 pending buffer
    - UDP mux 版 `accept_mux_peer` / `connect_mux_peer`
  - 当前仍只完成 mux 骨架本身，尚未接入 runtime relay / task 主链
- 已完成：基础回归补充
  - `src/serve/http.rs` parser tests 已补齐
  - `runtime_bridge.rs` 新增基础单测
  - `udp.rs` 新增并通过：
    - `udp_session_hello_heartbeat_roundtrip`
    - `udp_peer_can_exchange_stream_control_frames_after_handshake`
  - `listener.rs` / `dialer.rs` 已补 UDP 单测
  - `runtime_tests.rs` 新增并通过：
    - `udp_inbound_runtime_registers_direct_session`
  - `udp_mux.rs` 新增并通过：
    - `udp_mux_routes_frames_by_stream_id`
    - `udp_mux_buffers_stream_frames_until_receiver_is_opened`
    - `udp_mux_can_forward_control_frames`
    - `udp_mux_reads_stream_open`

本轮涉及文件：

- `/Users/qi4l/lang/Rust/Fusion-master/src/serve/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/serve/http.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/serve/service.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_bridge.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_http.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_mode.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_orchestrator.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_socks5.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime_tests.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/udp.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/udp_mux.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/listener.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/dialer.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/utils/url.rs`

最后一次验证结果：

- `cargo test` 通过
- 当前结果：`108 passed / 0 failed`

当前边界说明：

- HTTP Proxy 已完成主链接入、parser 收口与关键 E2E 覆盖
- HTTP / socks5 本地代理入口的重复桥接逻辑已开始收敛
- UDP 当前已经具备：
  - 会话握手与控制帧收发
  - listener / dialer / runtime 最小 direct 主链
  - inbound / outbound direct 会话登记到 `SessionHub` / `AgentRegistry`
  - mux / stream routing 的最小骨架
- 但 UDP 仍然还没有：
  - runtime relay peer 主链接入
  - task / service 挂载
  - 可靠传输、分片、重传

下一步建议优先：

1. 把 UDP mux 接进 runtime relay / control-plane 主链
2. 先让 UDP 承接 direct announce / route snapshot / control broadcast
3. 再把 UDP 往 task / service 方向逐步接入
