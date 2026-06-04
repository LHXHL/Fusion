# Fusion 计划与当前实现对齐说明

本文档用于解释：项目计划中的目录结构，哪些已经实现，哪些在本轮收尾中明确调整。

## 已补齐的结构项

- `src/protocol/stream.rs`
- `src/agent/state.rs`
- `src/tunnel/listener.rs`
- `src/tunnel/dialer.rs`
- `src/crypto/wrapper.rs`
- `src/app/runtime_orchestrator.rs`
- `src/app/runtime_peer.rs`
- `src/app/runtime_service.rs`
- `src/app/runtime_socks5.rs`

## 明确保留为“后续可选项”，本轮不补空壳

- `src/tunnel/tls.rs`
- `src/utils/fs.rs`

原因：
- 当前功能路径并不依赖这些模块
- 本轮目标是收尾，不制造空壳模块
- 对于 `wss://`，通过文档明确能力边界，避免误导

## 当前结论

本轮以“真实能力可交付 + 计划结构尽量一致”为原则：
- 有实际职责的模块补齐
- 无实际功能支撑的模块，不做空壳补位

## 结构收口进展

- `src/app/runtime_orchestrator.rs` 现已承接 listener startup / dialer startup / summary 编排逻辑
- `src/app/runtime_task.rs` 现已同时承接 task artifact 与主动 task 请求路径
- `src/app/runtime_peer.rs` 现已承接 mux task server / relay peer 主循环
- `src/app/runtime_service.rs` 现已承接 port forward listener 与 inbound raw service 挂载路径
- `src/app/runtime_socks5.rs` 现已承接 socks5 本地入口与远端 raw stream 挂载路径
- `src/app/runtime_tests.rs` 现已承接 runtime 相关回归测试，避免入口文件继续承载实现与测试双重职责
- `src/app/runtime.rs` 仍是主入口，但已不再直接持有上述两类细节实现
- `src/protocol/stream.rs` 与 `src/agent/registry.rs` 已开始共用同一套 stream lifecycle 状态类型

## Phase 4 已推进项

- `src/crypto/wrapper.rs` 已新增统一 transport wrapper 管道抽象
- `src/crypto/transport.rs` 现已通过 wrapper pipeline 处理 transport payload，而不是把 shared-key 加密逻辑写死在 transport 编解码函数内部
- 当前 wrapper pipeline 已先落地两种状态：
  - passthrough
  - shared-key AEAD wrapper
- 这一步先把“挂载点”做出来，后续 compression / padding / TLS 补强可在同一抽象层继续叠加

## Phase 3 已推进项

- `src/agent/registry.rs` 现已保留完整 route path，而不只是 next-hop / hop-count
- `src/app/runtime_relay.rs` 在处理 route update 时，现已拒绝明显异常的 path：
  - source peer 与 path 首跳不一致
  - path 中包含本地 agent
  - path 自身出现回环
  - path 未以 origin agent 收尾
- route snapshot 现已复用完整已知 path，对新连入 peer 的多跳同步更完整
- 同一 destination 的冲突路由，当前已有稳定选优规则
- `src/app/runtime_tests.rs` 已新增 3 跳 / 5 跳 TCP mux relay 回归
- `src/agent/registry.rs` 已新增中间 next-hop 断开后的 route 回收与重新学习回归
