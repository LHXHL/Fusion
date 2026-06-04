# Fusion：当前架构与目标架构对照

> 本文档用于回答两个问题：
>
> 1. Fusion **现在实际是什么**
> 2. Fusion **目标要演进到什么程度**

---

## 1. 当前架构

当前仓库已经完成了核心架构转向：

- 只保留一个二进制：`fusion`
- 不再区分 `server / client / implant`
- 节点通过参数组合决定职责：
  - `-s`：监听 tunnel
  - `-c`：连接上游 tunnel
  - `-l`：本地入口服务
  - `-r`：远端出口服务
  - `task`：发起或执行任务

当前代码大致分为：

- `src/app/`：CLI / config / runtime
- `src/agent/`：身份、能力、注册表、运行态聚合
- `src/tunnel/`：TCP / WS / WSS 连接与多路复用
- `src/session/`：握手、心跳、路由决策、重连
- `src/protocol/`：统一帧与消息定义
- `src/serve/`：socks5 / raw / port forward
- `src/task/`：shell / screenshot / upload / download
- `src/crypto/`：kex / aead

---

## 2. 当前真实能力边界

### 2.1 已具备

- 单体 Agent 运行时
- TCP tunnel
- WS tunnel
- WSS tunnel
  - 支持服务端显式证书/私钥配置
  - 支持客户端自定义 CA 或 `tls-insecure`
- socks5 入口
- raw 出口
- port forward
- relay
- 基础 route announce / next-hop 转发
- task 执行能力

### 2.2 仍属于基础版

- route 收敛
- 多跳稳定性
- 错误码体系
- wrapper 抽象
- 加密链路与数据面统一封装
- runtime 模块进一步拆分

---

## 3. 目标架构

目标架构不是回到“三端系统”，而是在当前单体 Agent 基础上继续扩展：

### 3.1 目标运行形态

```bash
fusion [global-options] [-s tunnel]... [-c tunnel]... [-l serve]... [-r serve]...
```

每个节点都应可以灵活承担：

- ingress
- egress
- relay
- task executor
- route forwarder

### 3.2 目标能力方向

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

#### Serve 扩展

- HTTP Proxy
- Shadowsocks
- Trojan
- ExternalC2

#### 数据面增强

- wrapper pipeline
- compression
- padding
- 更多加密算法

#### 组网增强

- route 收敛增强
- 多跳稳定化
- 代理链
- 多连接负载均衡
- 上下行分离

#### 平台化

- C ABI
- 静态/动态库导出
- 跨语言嵌入
- WASM

---

## 4. 当前与目标的核心差距

### 4.1 当前已完成的“骨架转换”

这部分已经基本完成：

- 三端角色模型 → 单体 Agent 模型
- 多套协议边界 → 统一 frame/message
- 固定 server/client 职责 → 参数驱动职责

### 4.2 仍需补齐的“成熟度”

当前主要差距不是概念，而是成熟度：

- WSS 已可用，但 TLS 配置矩阵仍较精简
- route 能工作，但还缺少复杂组网所需的防护和择优
- crypto 有基础模块，但未形成统一 wrapper pipeline
- runtime 仍偏集中，后续扩展前需要继续拆分

### 4.3 仍需补齐的“能力面”

这些是后续阶段的增量，不属于当前 MVP 已承诺范围：

- UDP / HTTP2 / DNS / ICMP / WG
- Simplex / SR-ARQ
- Shadowsocks / Trojan / ExternalC2
- 代理链 / 负载均衡 / duplex
- C ABI / WASM

---

## 5. 结论

当前 Fusion 的正确理解方式是：

> **单体 Agent MVP 已成型，当前重点是把已有主链路收口做稳，再逐步扩展成更完整的对等代理与隧道平台。**

因此后续开发不应再围绕“是否回到 server/client/implant”展开，
而应围绕以下顺序推进：

1. 收口当前主链路
2. 拆清 runtime / session / tunnel / serve 边界
3. 做稳 route / relay / 多跳
4. 再扩展 tunnel / serve / wrapper / 平台化能力

