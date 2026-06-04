# Fusion 计划与当前实现对齐说明

本文档用于解释：项目计划中的目录结构，哪些已经实现，哪些在本轮收尾中明确调整。

## 已补齐的结构项

- `src/protocol/stream.rs`
- `src/agent/state.rs`
- `src/tunnel/listener.rs`
- `src/tunnel/dialer.rs`

## 明确保留为“后续可选项”，本轮不补空壳

- `src/tunnel/tls.rs`
- `src/crypto/wrapper.rs`
- `src/utils/fs.rs`

原因：
- 当前功能路径并不依赖这些模块
- 本轮目标是收尾，不制造空壳模块
- 对于 `wss://`，通过文档明确能力边界，避免误导

## 当前结论

本轮以“真实能力可交付 + 计划结构尽量一致”为原则：
- 有实际职责的模块补齐
- 无实际功能支撑的模块，不做空壳补位
