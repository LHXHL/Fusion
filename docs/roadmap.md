# Fusion Roadmap

> 本文档用于说明 Fusion 当前所处阶段、与目标设计之间的差距，以及后续建议的分阶段开发路线。
>
> 适用范围：
> - 当前仓库：`/Users/qi4l/lang/Rust/Fusion-master`
> - 当前主程序：`fusion`

---

## 1. 当前定位

Fusion 当前已经完成了从“三端模型”向“单体 Agent 模型”的主体迁移。

目前它已经不是传统意义上的：
- server
- client
- implant

而是一个：

- **单二进制**
- **参数驱动角色**
- **节点对等**
- **可 listen / connect / relay / expose service / run task**

的统一 Agent 运行时。

当前最准确的判断是：

> **Fusion 已具备单体 Agent MVP 的基础骨架，接下来工作的重点不再是重做架构，而是收口、补强、扩展。**

---

## 2. 当前已实现能力

以下内容基于当前仓库实际代码、README 和基线文档整理，表示“已存在且可作为当前能力边界参考”的部分。

### 2.1 运行形态

- 单二进制：`fusion`
- 支持：
  - `-s, --listen`
  - `-c, --connect`
  - `-l, --local-serve`
  - `-r, --remote-serve`
- 节点可同时承担：
  - listener
  - dialer
  - relay
  - service ingress
  - service egress
  - task executor

### 2.2 Tunnel

当前已接入或已验证的 tunnel：

- `tcp://`
- `ws://`
- `wss://`（当前仅部分接入，见“已知缺口”）

### 2.3 Service

当前已实现：

- `socks5://HOST:PORT`
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN_HOST:LISTEN_PORT->TARGET_HOST:TARGET_PORT`

### 2.4 Task

当前已实现：

- `shell`
- `screenshot`
- `upload`
- `download`

### 2.5 协议与运行态

当前已存在：

- `Hello`
- `HelloAck`
- `Heartbeat`
- `AgentAnnounce`
- `RouteUpdate`
- `TaskRequest`
- `TaskResult`
- `StreamOpen`
- `StreamData`
- `StreamClose`

并具备以下基础能力：

- peer 握手
- 基础 route 记录与 next-hop 决策
- relay 转发
- reconnect
- runtime status snapshot

---

## 3. 当前已知缺口

当前缺口分为两类：

1. **MVP 收口缺口**
2. **目标设计扩展缺口**

### 3.1 MVP 收口缺口

这些是当前最应该优先解决的问题。

#### A. `wss://` 尚未完全收口

当前状态：

- URL 解析支持
- CLI 接受
- runtime 分支接受
- 但尚未形成完整的 TLS listener / cert / verify 配置闭环

这意味着：

> 当前 `wss://` 仍应视为“保留入口但未完全承诺成熟能力”。

#### B. 加密链路尚未完整贯穿

当前已有：

- `crypto/aead.rs`
- `crypto/kex.rs`

但仍缺少：

- 统一 wrapper 抽象
- 配置到数据面的加密封装闭环
- 更清晰的协商与错误传播机制

#### C. 路由能力仍是基础版

当前有：

- route announce
- route registry
- next-hop 决策
- relay

但仍缺少：

- route 冲突处理
- loop 防护
- 更稳健的 route 失效和收敛机制
- 多跳稳定性验证

#### D. runtime 仍偏集中

当前 `runtime.rs` 已承担大量职责：

- 启动 listener
- 启动 dialer
- 握手
- relay
- stream 转发
- task 调度
- service 挂载

继续扩展前，建议进一步拆分。

### 3.2 目标设计扩展缺口

这些能力在设计文档中存在，但当前尚未实现或未进入稳定阶段：

#### Tunnel 扩展

- `udp://`
- `http://`
- `h2://`
- `streamhttp://`
- `dns://`
- `icmp://`
- `wg://`
- `unix://`
- `memory://`

#### Simplex / 可靠传输

- `simplex+dns://`
- `simplex+oss://`
- `simplex+http://`
- SR-ARQ
- 分片 / 重传 / 滑窗

#### Wrapper / 流量处理

- 统一 wrapper pipeline
- TLS 完整收口
- 多加密算法接入
- 压缩
- padding

#### Serve 扩展

- HTTP Proxy
- Shadowsocks
- Trojan
- ExternalC2

#### 高级网络能力

- 代理链：`-x`
- 前置代理：`-f`
- 上下行分离：`up-` / `down-`
- ConnHub 负载均衡

#### 平台与分发

- C ABI 导出
- 多语言嵌入
- WASM
- 更完整的跨平台构建矩阵

---

## 4. 路线图原则

后续开发建议遵循以下原则：

### 4.1 先收口，再扩展

先把已有 MVP 做扎实，再继续补复杂协议族和非常规信道。

### 4.2 先稳定主链路，再增加新类型

优先保证以下主链路稳定：

- TCP / WS / WSS
- socks5 -> raw
- port forward
- relay
- task shell / upload / download / screenshot

### 4.3 先拆职责，再堆能力

在 `runtime`、`tunnel`、`serve`、`session` 职责边界不清晰之前，不建议快速叠加太多新协议。

### 4.4 文档与代码同步收口

任何阶段完成后，都应同步更新：

- `README.md`
- `docs/baseline-current-capabilities.md`
- `docs/protocol.md`
- `docs/test-matrix.md`

---

## 5. 分阶段开发计划

---

## Phase 0：能力冻结与边界澄清

### 目标

冻结当前真实能力边界，避免“文档目标”和“当前实现”继续混淆。

### 重点工作

- 统一当前仓库的能力说明
- 明确 `wss://` 当前状态
- 明确哪些能力是“已支持”，哪些是“设计目标”
- 建立路线图文档和当前/目标对照文档

### 建议产出

- `docs/roadmap.md`
- `docs/architecture-current-vs-target.md`

### 验收标准

- 新成员阅读文档后，能明确区分：
  - 当前真实能力
  - 已知限制
  - 后续扩展方向

---

## Phase 1：MVP 收口与稳定化

### 目标

把当前 Fusion 从“可运行”提升到“稳定可交付”。

### 重点工作

#### 1. 完整收口 `wss://`

- 明确 TLS listener / client 能力边界
- 明确证书来源、校验策略和配置项
- 增加回归测试

#### 2. 梳理 `-k` 与加密数据面

- 明确 `key` 在握手、会话、stream 数据中的作用范围
- 形成统一 wrapper 或加密挂载点
- 完善失败时的错误传播

#### 3. 统一错误处理与可观测性

- 统一关键路径错误输出
- 提升 runtime snapshot 可读性
- 改进 relay / route / stream 的状态观测

#### 4. 补强主链路回归

至少稳定覆盖：

- TCP 单跳
- WS 单跳
- WSS 单跳
- relay
- socks5 -> raw
- port forward
- reconnect
- task 基本链路

### 建议涉及模块

- `src/tunnel/ws.rs`
- `src/tunnel/listener.rs`
- `src/tunnel/dialer.rs`
- `src/crypto/aead.rs`
- `src/crypto/kex.rs`
- `src/app/runtime.rs`

### 验收标准

- 当前 README 中承诺的能力都可稳定复现
- `cargo test` 通过
- smoke 脚本通过
- WSS 能力边界明确，不再含混

---

## Phase 2：运行时拆分与结构收口

### 目标

降低 `runtime.rs` 耦合度，为后续扩展铺路。

### 重点工作

#### 1. 拆分 runtime 职责

建议按职责拆分为：

- bootstrap
- listener startup
- dialer startup
- relay flow
- service mount
- task dispatch
- status snapshot

#### 2. 明确 stream 生命周期

- 打开
- 激活
- 转发
- 关闭
- 失败

统一状态机和错误处理路径。

#### 3. 明确 service 挂载模型

- 本地入口服务如何接入 stream
- 远端出口服务如何解析和执行业务
- socks5 / raw / port 的统一抽象边界

### 建议涉及模块

- `src/app/runtime.rs`
- `src/session/*`
- `src/serve/*`
- `src/protocol/stream.rs`（如决定进一步抽象）

### 验收标准

- runtime 不再是单点“大总管”
- 增加新 tunnel / new service 时，不需要大面积改 runtime

---

## Phase 3：路由增强与多跳稳定化

### 目标

把当前“基础 relay”升级为“可控的多跳组网能力”。

### 重点工作

#### 1. 增强路由传播与失效机制

- route TTL
- route 刷新
- stale route 清理
- peer 断开时 route 回收

#### 2. 增加 loop 防护

- 防止 route 回环
- 防止 relay 自旋转发

#### 3. 增加 route 冲突处理

- 多条路由如何选优
- 同一目的地的 next-hop 冲突如何处理

#### 4. 补多跳回归

至少验证：

- 3 跳链路
- 5 跳链路
- 中间节点断开后的恢复
- 指定 destination agent 的稳定转发

### 建议涉及模块

- `src/agent/registry.rs`
- `src/session/router.rs`
- `src/protocol/route.rs`
- `src/app/runtime.rs`

### 验收标准

- 3~5 跳场景可稳定运行
- route 收敛和 route 失效行为可解释、可观测、可回归

---

## Phase 4：基础扩展能力

### 目标

在主链路稳定和结构清晰后，开始补“最值得先做”的扩展能力。

### 建议优先顺序

1. wrapper 抽象
2. HTTP Proxy service
3. UDP tunnel

### 重点工作

#### 1. Wrapper Pipeline

建立统一挂载点，支持后续逐步接入：

- encryption
- compression
- padding
- TLS 补强

#### 2. HTTP Proxy service

补一个高复用、低门槛的 service 类型。

#### 3. UDP tunnel

为后续不可靠信道、增强场景和 Simplex 能力打基础。

### 建议涉及模块

- `src/crypto/`（可新增 wrapper 抽象）
- `src/serve/service.rs`
- `src/serve/http.rs`（建议新增）
- `src/tunnel/udp.rs`（建议新增）

### 验收标准

- 新增 service / tunnel 的开发成本明显下降
- wrapper 能作为后续功能稳定挂载点

---

## Phase 5：高级网络能力

### 目标

补设计文档中较强但复杂度更高的网络特性。

### 重点工作

#### 1. 代理链

- `-x` 出站代理链
- `-f` 前置代理
- 支持多层混合代理

#### 2. ConnHub 负载均衡

- `random`
- `fallback`
- `round-robin`

#### 3. 上下行分离

- `up-`
- `down-`
- 异构 tunnel 组合

#### 4. 更强的 failover

- 多连接故障切换
- route 与 session 状态联动

### 验收标准

- 多连接、多出口、多代理链场景可稳定工作
- 负载均衡和故障切换行为可验证

---

## Phase 6：非常规信道与平台化输出

### 目标

进入设计文档中的第二阶段能力建设。

### 重点工作

#### 1. Simplex 与 SR-ARQ

- `simplex+http://`
- `simplex+dns://`
- `simplex+oss://`
- 分片 / 重传 / 滑窗 / 窗口控制

#### 2. 特殊 Tunnel

- `wg://`
- `icmp://`
- `memory://`
- `unix://`

#### 3. 平台化输出

- C ABI
- 动态库 / 静态库导出
- 跨语言嵌入
- WASM / 受限环境构建

### 验收标准

- 形成第二阶段产品能力，而不只是主链路代理工具

---

## 6. 建议优先级排序

如果以“最短路径把项目推进到可持续迭代状态”为目标，建议优先级如下：

### 第一优先级

1. `wss://` 收口
2. `-k` / crypto 数据面贯通
3. runtime 拆分
4. 多跳 route 稳定化

### 第二优先级

5. wrapper 抽象
6. HTTP Proxy
7. UDP tunnel

### 第三优先级

8. 代理链
9. 负载均衡
10. 上下行分离

### 第四优先级

11. Simplex / SR-ARQ
12. C ABI / 多平台输出

---

## 7. 里程碑定义

### M1：稳定 MVP

满足：

- TCP / WS / WSS 能力边界明确
- socks5 / raw / port forward 稳定
- relay 稳定
- task 稳定
- 主链路有回归

### M2：可维护骨架

满足：

- runtime 拆分完成
- service / tunnel 扩展边界清晰
- stream / route 生命周期更稳定

### M3：可组网 Agent

满足：

- 多跳 route 稳定
- route 收敛/失效明确
- 3~5 跳场景可稳定复现

### M4：可扩展平台

满足：

- wrapper 已建立
- 新增 service / tunnel 成本可控
- 高级网络能力可持续接入

---

## 8. 非目标说明

以下内容不建议在当前阶段抢跑：

- 同时并行实现多个复杂 tunnel
- 在 runtime 未拆分前堆大量新 service
- 在 route 仍基础版时实现复杂负载均衡
- 在 wrapper 未抽象前堆多种加密/压缩/padding
- 在主链路未稳定前推进 C ABI / WASM / Simplex

---

## 9. 文档联动要求

每完成一个阶段，至少同步检查以下文件：

- `/Users/qi4l/lang/Rust/Fusion-master/README.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/baseline-current-capabilities.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/protocol.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/test-matrix.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/quick-start.md`

原则：

> 文档用于描述“当前真实可交付能力”，不是提前替未来功能占位。

---

## 10. 总结

Fusion 当前已经完成了最困难的一步：

> 从旧的三角色模型，迁移到统一的单体 Agent 运行时模型。

接下来的重点不是推翻重来，而是：

- 把已有 MVP 收口
- 把 runtime 结构理顺
- 把 route 和多跳做稳
- 再逐步补 tunnel / service / wrapper / 高级网络能力

建议整体执行顺序保持为：

> **先稳定主链路 → 再拆结构 → 再补路由 → 再扩展能力 → 最后做非常规信道与平台化**

