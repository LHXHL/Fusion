# Fusion Roadmap

本文档说明 Fusion 当前实际进度、距离目标设计仍缺什么，以及接下来建议继续补强的方向。

原则：
- 只描述当前仓库真实能力
- 已完成和未完成明确分开
- 路线图用于指导后续开发，不再重复已经落地的工作

---

## 1. 当前判断

Fusion 已经不再处于“单体 Agent MVP 刚搭起来”的阶段。

结合当前仓库代码、README、基线文档和测试矩阵，可以更准确地把它定义为：

> **Fusion 已完成单体 Agent 主链路收口，并进入“补强稳定性、补高级能力、补文档一致性”的阶段。**

这意味着后续重点不再是“先把骨架做出来”，而是：
- 收口真实欠缺能力
- 减少文档和实现之间的偏差
- 继续补复杂链路的稳定性与交付面

---

## 2. 已完成的路线图项目

以下能力已经在当前仓库落地，不应再继续归类为“主要缺口”。

### 2.1 单体 Agent 主模型

已完成：
- 单二进制 `fusion`
- 参数驱动角色
- listener / dialer / relay / service / task 统一运行时
- `status` / `peers` / `routes` / `services` 控制面

### 2.2 主链路 Tunnel

已完成并已有测试或文档收口：
- `tcp://`
- `ws://`
- `wss://`
- `udp://`
- `unix://`
- `memory://`
- `icmp://`
- `wg://`

### 2.3 Service

已完成：
- `socks5://`
- `http://`
- `ss://...?...method=none`
- `raw://HOST:PORT`
- `raw://`
- `port://LISTEN->TARGET`

### 2.4 Task

已完成：
- `shell`
- `screenshot`
- `upload`
- `download`

### 2.5 加密与 Wrapper 基础层

已完成：
- `-k` 预共享密钥帧级加密
- `src/crypto/wrapper.rs` 统一 wrapper pipeline
- compression stage
- padding stage
- AEAD stage
- `--wrap-compress`
- `--wrap-padding <BYTES>`

### 2.6 Runtime 结构拆分

已完成第一轮拆分：
- `runtime_status.rs`
- `runtime_task.rs`
- `runtime_peer.rs`
- `runtime_service.rs`
- `runtime_orchestrator.rs`
- `runtime_socks5.rs`
- `runtime_http.rs`
- `runtime_relay.rs`
- `runtime_mode.rs`
- `runtime_bootstrap.rs`

### 2.7 Route 与多跳第一轮收口

已完成：
- route announce / route snapshot
- next-hop 决策
- route TTL
- stale route 清理
- peer 断开后的 route 回收
- route path 全量保留
- loop/path 校验
- 3 跳 / 5 跳 relay 回归

### 2.8 高级网络能力的第一版

已完成第一版：
- `-x` proxy chain
- `-f` front proxy
- `--conn-policy fallback|random|round-robin`
- `--up-connect`
- `--down-connect`
- 上游连接池复用与清理

### 2.9 非常规信道 / 平台化基础层

已完成第一版：
- `simplex+http://` 最小 direct session
- SR-ARQ 基础构件
- batch send / long-poll receive
- 窗口发送、ACK、重传、去重
- C ABI 基础导出
- `rlib` / `cdylib` / `staticlib`

---

## 3. 现在真正还差什么

当前仍然缺的，不是“有没有入口”，而是“能力是否完整、是否稳定、是否容易交付”。

### 3.1 文档同步仍然不彻底

当前问题：
- `docs/roadmap.md` 之前仍把不少已完成能力写成缺口
- 部分文档仍残留旧机器绝对路径
- 示例配置文件没有覆盖当前已经实现的关键配置面

影响：
- 新读者会误判项目成熟度
- 后续开发容易重复补已经完成的能力

### 3.2 Wrapper 仍缺自动协商与交付约束

当前已具备基础 pipeline，但还缺：
- 双端能力协商
- wrapper 不一致时更清晰的错误提示
- 更完整的配置矩阵说明
- 更明确的兼容性约束

当前判断：
- 基础层已完成
- 交付面仍属手工对齐模式

### 3.3 TLS/WSS 仍偏显式 PEM 配置

当前已支持：
- 服务端 cert/key
- client CA
- client cert/key
- `tls-insecure=1`
- 基础 mTLS 测试

仍缺：
- 更完整的证书来源矩阵
- 更细的校验策略表达
- 证书热更新
- 更明确的生产级边界描述

### 3.4 Route 冲突处理还可以继续补强

当前已具备：
- loop/path 校验
- TTL/回收
- 多跳回归

仍建议补：
- 同 hop-count 候选路由的更稳定选优策略
- 路由切换时更强可观测性
- 中间节点抖动时的收敛行为验证
- 更系统的多出口/多路径回归

### 3.5 错误模型仍不统一

当前错误主要散落在：
- stderr
- `TaskResult.ok=false`
- stream close
- runtime status snapshot

仍缺：
- 统一错误码
- 更稳定的 operator-facing 错误文案
- transport / route / task / service 错误归一化

### 3.6 `simplex+http://` 仍是基础版

当前已接入：
- hello / hello-ack / heartbeat
- 分片 / 重组
- ACK / 重传
- 窗口发送

仍缺：
- mux
- relay
- service/task 主链路复用
- `simplex+dns://`
- `simplex+oss://`

### 3.7 平台化输出仍是基础导出

当前已具备：
- C ABI
- 基础头文件
- 多种 crate-type

仍缺：
- 更完整 ABI 面
- 示例宿主集成
- 多语言嵌入 demo
- WASM 交付面

---

## 4. 建议新的阶段划分

后续路线图建议按“补强真实缺口”推进，而不是重复旧 phase。

### Phase A：文档与交付面收口

目标：
- 让 README、baseline、roadmap、quick-start、config example 完全对齐

重点：
- 清理旧绝对路径
- 更新 roadmap 为真实现状
- 补齐配置示例
- 明确哪些能力是“已支持但精简版”

验收：
- 新成员只看文档就能正确理解当前能力边界

### Phase B：稳定性与可观测性补强

目标：
- 让已有主链路更容易排障、更稳定回归

重点：
- route 冲突处理增强
- route 收敛日志增强
- wrapper/TLS 失败错误收口
- runtime status 信息继续增强

验收：
- 多跳、故障切换、wrapper 配置不一致等场景更容易定位

### Phase C：高级网络能力第二轮补强

目标：
- 把当前已经有第一版入口的能力继续做实

重点：
- proxy chain 场景回归扩充
- 上下行分离与连接池策略补强
- 多路径 failover 行为验证

验收：
- 多入口、多上游、多代理链组合行为可重复验证

### Phase D：Simplex 扩展

目标：
- 把 `simplex+http://` 从基础 direct session 推向可复用 transport

重点：
- mux
- relay
- service/task 链路接入
- `simplex+dns://` / `simplex+oss://`

验收：
- Simplex 系列不再只是实验性基础层

### Phase E：平台化输出补强

目标：
- 把库模式从“能导出”推进到“可嵌入”

重点：
- 扩展 ABI
- 提供宿主示例
- 补跨语言/受限环境文档

验收：
- 外部程序可稳定嵌入 Fusion 核心能力

---

## 5. 建议优先级

按“投入产出比”和“对当前项目价值”排序，建议优先级如下：

### 第一优先级

1. 文档和实现完全对齐
2. route 冲突处理与可观测性补强
3. wrapper/TLS 错误收口
4. 多路径 failover 回归补强

### 第二优先级

5. `simplex+http://` 接入 mux / relay
6. 扩充高级网络能力测试矩阵
7. 扩展配置与嵌入示例

### 第三优先级

8. `simplex+dns://`
9. `simplex+oss://`
10. 更完整 C ABI / 多语言嵌入

---

## 6. 当前里程碑判断

结合当前实现，建议这样判断：

- M1 稳定 MVP：已完成
- M2 可维护骨架：已完成第一轮
- M3 可组网 Agent：已完成第一轮，还需补 route 冲突与更复杂 failover
- M4 可扩展平台：已进入建设期，但尚未完成

---

## 7. 文档联动要求

后续每次补功能时，至少同步检查：

- `README.md`
- `docs/baseline-current-capabilities.md`
- `docs/protocol.md`
- `docs/test-matrix.md`
- `docs/quick-start.md`
- `fusion.toml.example`

原则：

> 文档描述的是“当前真实可交付能力”，不是未来设想，也不是过时阶段状态。

---

## 8. 总结

Fusion 现在距离旧版路线图最大的差距，不是功能数量，而是：

- 路线图本身落后于实现
- 某些能力已经有了，但交付面还不够完整
- 真正该补的是稳定性、可观测性、回归覆盖和平台化落地

因此接下来最合理的推进顺序是：

> **先对齐文档与配置面，再补稳定性和冲突处理，再继续推进 Simplex 与平台化。**
