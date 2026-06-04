# Fusion 单体 Agent 架构改造项目计划

> 目标：将当前 `/Users/qi4l/lang/Rust/Fusion` 从“服务端 / 客户端 / 植入端”三端模型，重构为类似 `rem` 的**单二进制、对等 Agent、参数驱动角色**架构。
>
> 核心结果：最终只保留一个主程序，例如 `fusion`，不再需要 `fusion-server` / `fusion-client` / `fusion-implant` 三个入口。任意节点都可以通过参数同时承担监听、连接、中继、入口服务、出口服务等角色。

---

## 1. 项目目标

### 1.1 总目标

将现有 Fusion 改造成如下形态：

- **单一可执行文件**：例如 `fusion`
- **无固定 server/client/implant 角色**
- **每个节点都是 Agent**
- **通过参数组合决定行为**：
  - `-s`：监听 tunnel
  - `-c`：连接上游 tunnel
  - `-l`：本端开放服务
  - `-r`：对端开放服务
- **节点可同时监听和连接**，支持 relay
- **节点之间通过统一协议通信**，不再区分“控制台协议”和“植入回连协议”
- **保留现有可复用能力**：
  - 注册/身份识别逻辑
  - 加密协商逻辑
  - shell / screenshot / upload / download 等执行能力
  - listener / 连接管理经验

### 1.2 本次改造不追求一步到位的内容

以下内容可以列为后续阶段，不要求首版全部完成：

- DNS / ICMP / WireGuard / HTTP2 / StreamHTTP 等所有 rem 级传输
- 多跳自动路由和拓扑广播的完整实现
- C 动态库导出
- 所有加密插件与 wrapper 插件
- 完整代理协议家族（trojan / ss / externalc2 等）

### 1.3 首版必须达到的目标

首版建议聚焦一个**可运行的单体 Agent MVP**：

- 单二进制
- 支持 `tcp://` 和 `ws://` / `wss://` tunnel
- 支持 `-s` / `-c` 自由组合
- 支持 `-l socks5://...`
- 支持 `-r raw://...` 或 `-r port://...`
- 支持基础重连
- 支持节点 ID / 名称 / 基本 metadata
- 支持节点间统一消息帧
- 支持至少单跳 + relay 两种形态

---

## 2. 当前项目现状

当前项目源码路径：`/Users/qi4l/lang/Rust/Fusion`

### 2.1 当前结构

当前已经融合为一个 Cargo 工程，但逻辑上仍是三套系统并存：

- 服务端：`/Users/qi4l/lang/Rust/Fusion/src/fusion_server`
- 客户端：`/Users/qi4l/lang/Rust/Fusion/src/fusion_client`
- 植入端：`/Users/qi4l/lang/Rust/Fusion/src/fusion_implant`

当前入口：

- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion-server.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion-client.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion-implant.rs`

### 2.2 当前实际能力

#### 服务端
- WebSocket 控制面：`/Users/qi4l/lang/Rust/Fusion/src/fusion_server/server/server.rs`
- listener 管理
- agent / implant / task / db / certs
- 使用 SQLite 保存状态

#### 客户端
- 交互式 CLI
- 通过 WebSocket 连接服务端
- 发送 listener / task / implant 等控制命令
- 当前本质是“控制台”

#### 植入端
- HTTP/HTTPS 方式注册和轮询任务
- 与服务端进行加密通信
- 支持 shell / screenshot / upload / download / whoami 等
- 当前本质是“执行体”

### 2.3 当前架构根本问题

当前问题不是“代码分 3 个目录”，而是**协议和职责本身就是三套**：

1. **控制面和数据面割裂**
   - client ↔ server：WebSocket 文本命令
   - implant ↔ server：HTTP 轮询 + AES/X25519 消息

2. **角色固定**
   - server 天生居中
   - client 只能控制
   - implant 只能执行

3. **无法天然 relay / 多跳**
   - 当前植入不会转发其它节点流量
   - 当前节点不是平等对等体

4. **能力绑定在角色，而不是绑定在 Agent 节点**
   - socks5/listener 在 server/client 体系里
   - shell/screenshot 在 implant 体系里
   - 无法参数组合自由拼装

因此，本次改造的关键不是“合并 main.rs”，而是：

> **用统一 Agent 协议 + 统一连接模型 + 统一服务抽象，替换当前三套角色边界。**

---

## 3. 目标架构设计

## 3.1 最终形态

建议最终程序形态：

```bash
fusion [global-options] [-s tunnel]... [-c tunnel]... [-l serve]... [-r serve]...
```

示例：

```bash
fusion -s tcp://0.0.0.0:34996
fusion -c tcp://1.2.3.4:34996 -l socks5://127.0.0.1:1080
fusion -c tcp://upstream:34996 -s tcp://0.0.0.0:35000
fusion -c ws://relay:8080/tunnel -l socks5://127.0.0.1:1080 -r raw://target:80
```

## 3.2 逻辑分层

建议新架构分成 6 层：

1. **CLI / Config 层**
   - 参数解析
   - URL 解析
   - 运行模式构建

2. **Runtime 层**
   - 启动所有 listener/dialer
   - 管理 Agent 生命周期
   - 管理任务调度和服务挂载

3. **Tunnel 层**
   - 抽象连接建立与收发
   - TCP / WS / WSS 统一接口

4. **Session / Peer 层**
   - 节点握手
   - 身份识别
   - key exchange
   - 心跳、重连、peer 状态

5. **Protocol 层**
   - Route / OpenStream / Data / Close / Announce / Task 等统一帧

6. **Serve / Capability 层**
   - socks5 / raw / port forward / shell / file / screenshot 等能力

## 3.3 关键设计原则

### 原则 A：所有节点都是 Agent
每个进程启动后都拥有：
- 节点 ID
- 节点标签/名称
- 能力集合
- 连接集合
- 服务集合

### 原则 B：监听和连接都只是“连接来源”
- `-s`：产生入站 peer
- `-c`：产生出站 peer
- 后续逻辑只面对统一 `PeerSession`

### 原则 C：服务和能力要抽象成插件式接口
- 本端服务（如 socks5）只关心如何把流量封装成 stream
- 对端能力（如 raw connect / shell / file）只关心如何处理 stream 或 request

### 原则 D：不要再保留“server 专有数据库 / client 专有命令 / implant 专有任务轮询协议”
统一改成：
- 节点状态由 Runtime 管理
- 命令由 Protocol Message 驱动
- 数据流由 Multiplexed Stream 驱动

---

## 4. 建议的新目录结构

建议在当前工程上逐步迁移到：

```text
/Users/qi4l/lang/Rust/Fusion/src
├── bin/
│   └── fusion.rs
├── app/
│   ├── mod.rs
│   ├── cli.rs
│   ├── config.rs
│   └── runtime.rs
├── agent/
│   ├── mod.rs
│   ├── identity.rs
│   ├── registry.rs
│   ├── capabilities.rs
│   └── state.rs
├── tunnel/
│   ├── mod.rs
│   ├── listener.rs
│   ├── dialer.rs
│   ├── tcp.rs
│   ├── ws.rs
│   ├── tls.rs
│   └── stream.rs
├── session/
│   ├── mod.rs
│   ├── handshake.rs
│   ├── peer.rs
│   ├── reconnect.rs
│   ├── heartbeat.rs
│   └── hub.rs
├── protocol/
│   ├── mod.rs
│   ├── frame.rs
│   ├── codec.rs
│   ├── message.rs
│   ├── route.rs
│   └── stream.rs
├── serve/
│   ├── mod.rs
│   ├── socks5.rs
│   ├── raw.rs
│   ├── portfwd.rs
│   └── service.rs
├── task/
│   ├── mod.rs
│   ├── shell.rs
│   ├── file.rs
│   ├── screenshot.rs
│   └── dispatcher.rs
├── crypto/
│   ├── mod.rs
│   ├── kex.rs
│   ├── aead.rs
│   └── wrapper.rs
├── store/
│   ├── mod.rs
│   ├── memory.rs
│   └── sqlite.rs
└── utils/
    ├── mod.rs
    ├── url.rs
    ├── random.rs
    └── fs.rs
```

注意：
- `fusion_server` / `fusion_client` / `fusion_implant` 最终应作为**迁移源**，不是长期保留结构。
- 首先新增上述新目录，再逐步迁移旧代码，最终删掉旧模块。

---

## 5. 改造策略

建议采用：

> **双轨迁移 + 分阶段替换 + 每阶段可编译可回归**

不要一次性推倒重来，而是：

1. 保持当前三入口可用
2. 新增单体入口 `fusion`
3. 先把公共能力抽出来
4. 再把协议统一
5. 再把服务端/客户端/植入端逐步退役
6. 最后删除旧入口

这样做的好处：
- 每个阶段都可运行
- 方便定位回归
- 风险可控
- 可以先完成 MVP，再补高级特性

---

## 6. 分阶段项目计划

# Phase 0：冻结基线与建立回归样本

## 目标
在动架构之前，固定当前行为基线，避免改造后“看起来跑了，其实功能丢了”。

## 步骤

### 0.1 建立基线说明文件
新建：
- `/Users/qi4l/lang/Rust/Fusion/docs/baseline-current-capabilities.md`

内容记录：
- 当前支持的 listener 类型
- 当前支持的 implant 任务
- 当前配置项
- 当前消息格式
- 当前 DB 表结构

### 0.2 录制最小回归场景
至少固化 5 个场景：

1. server 启动
2. client 连接 server
3. implant 注册 server
4. shell 命令执行返回
5. 文件下载/上传

建议保存：
- 启动命令
- 输入参数
- 关键日志
- 预期结果

### 0.3 建立手动回归脚本
新建：
- `/Users/qi4l/lang/Rust/Fusion/scripts/regression/manual-smoke.sh`

内容至少包括：
- 编译
- 启动节点
- 发起测试命令
- 检查输出文件/日志

### 0.4 为关键模块补单元测试入口
优先补：
- 加密编码解码
- 文件上传下载编码逻辑
- URL 解析逻辑
- 配置解析逻辑

## 交付物
- baseline 文档
- smoke 脚本
- 至少 8~15 个基础单元测试

## 验收标准
- 当前旧系统可以稳定通过 baseline smoke

---

# Phase 1：建立新单体入口与参数系统

## 目标
新增统一入口 `fusion`，但先不改变底层逻辑，只完成新 CLI 框架。

## 步骤

### 1.1 新建主入口
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion.rs`

职责：
- 解析统一参数
- 调用 `app::runtime::run()`

### 1.2 新建 CLI 模块
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/app/cli.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/app/config.rs`

实现：
- `-s/--listen <URL>` 可重复
- `-c/--connect <URL>` 可重复
- `-l/--local-serve <URL>` 可重复
- `-r/--remote-serve <URL>` 可重复
- `-a/--agent-name <NAME>`
- `-k/--key <SECRET>`
- `--retry`
- `--retry-interval`
- `--retry-max-interval`
- `--data-dir`
- `--log-level`

### 1.3 定义统一配置结构
在 `/Users/qi4l/lang/Rust/Fusion/src/app/config.rs` 中建立：
- `AppConfig`
- `TunnelEndpoint`
- `ServeEndpoint`
- `RetryPolicy`
- `AgentIdentityConfig`

### 1.4 URL 解析器
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/utils/url.rs`

负责：
- 解析 `tcp://` / `ws://` / `wss://`
- 解析 `socks5://` / `raw://` / `port://`
- 解析 query 参数（如 TLS / wrapper / retry）

## 交付物
- `fusion` 新入口可编译
- `fusion --help` 能完整展示新参数

## 验收标准
- `cargo run --bin fusion -- --help` 成功
- URL 参数能正确解析为内部配置结构

---

# Phase 2：抽离公共基础模块

## 目标
把当前三套代码中重复或通用的部分先抽成公共库，减少后续重构阻力。

## 步骤

### 2.1 抽离 crypto
从以下位置迁移公共逻辑：
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_server/server/crypto`
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_implant/crypto`

新位置：
- `/Users/qi4l/lang/Rust/Fusion/src/crypto`

统一内容：
- X25519 key exchange
- AEAD encrypt/decrypt
- nonce 编解码
- 公私钥生成
- wrapper 接口预留

### 2.2 抽离 random / fs / common utils
来源：
- `fusion_server/utils`
- `fusion_client/utils`
- `fusion_implant/utils`

目标：
- `/Users/qi4l/lang/Rust/Fusion/src/utils`

### 2.3 抽离 identity
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/agent/identity.rs`

统一处理：
- uuid / hostname / os / arch
- 自定义 agent name
- platform-specific metadata

### 2.4 抽离 task capability
从 implant 平台代码中提取：
- shell
- screenshot
- upload/download 基础逻辑

先抽成：
- `/Users/qi4l/lang/Rust/Fusion/src/task`

注意：
- 先保留 windows/linux/macos 分平台实现
- 只调整调用路径，不急着统一协议

## 交付物
- crypto/utils/task/identity 成为公共模块

## 验收标准
- 旧三个入口仍能编译
- 新入口也能链接公共模块

---

# Phase 3：设计统一协议帧

## 目标
替换当前“WebSocket 文本命令协议”和“HTTP 轮询任务协议”为统一内部消息协议。

## 协议设计要求
统一消息至少支持：
- 握手
- 心跳
- 节点公告
- 开流
- 数据
- 关流
- 任务请求
- 任务响应
- 服务注册
- 路由更新

## 步骤

### 3.1 定义 frame 结构
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/frame.rs`

建议字段：
- version
- msg_type
- session_id
- stream_id
- src_agent
- dst_agent
- flags
- payload

### 3.2 定义 message enum
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/message.rs`

枚举建议：
- `Hello`
- `HelloAck`
- `Heartbeat`
- `AgentAnnounce`
- `OpenStream`
- `StreamData`
- `CloseStream`
- `TaskRequest`
- `TaskResult`
- `RouteAnnounce`
- `ServiceExpose`

### 3.3 定义 codec
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/codec.rs`

要求：
- 支持 length-prefixed 帧
- 支持 JSON / CBOR / MessagePack 其一
- 预留后续升级字段

建议首版：
- 控制帧用 JSON/serde
- 数据帧走二进制包

### 3.4 定义 stream 抽象
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/stream.rs`

实现：
- 多路复用 stream id
- 一条 tunnel 上承载多个 service/task/data stream

## 交付物
- protocol 模块
- 协议文档草案

## 验收标准
- 两个本地内存 peer 间能完成 hello + heartbeat + task echo

---

# Phase 4：建立统一 Session / Peer 模型

## 目标
把所有连接抽象成统一 `PeerSession`，无论它来自监听还是主动连接。

## 步骤

### 4.1 新建 session 核心结构
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/session/peer.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/session/hub.rs`

关键结构：
- `PeerSession`
- `PeerId`
- `SessionState`
- `SessionHub`

### 4.2 实现握手
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/session/handshake.rs`

握手内容建议：
- 协议版本
- agent id / name
- public key
- capability bitmap
- transport metadata

### 4.3 实现心跳与失活检测
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/session/heartbeat.rs`

需要：
- 定时心跳
- 超时断开
- 状态更新

### 4.4 实现自动重连
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/session/reconnect.rs`

要求：
- 指数退避
- 上限时间
- 可配置最大次数
- 保留 endpoint 配置重新拨号

## 交付物
- SessionHub 可管理多 peer
- 出站连接掉线可自动重连

## 验收标准
- `-c tcp://...` 断开后能够自动恢复
- `-s` 入站和 `-c` 出站在上层看来都是统一 peer

---

# Phase 5：建立 Tunnel 抽象

## 目标
把 TCP / WS / WSS 都包装为统一 Tunnel Transport。

## 步骤

### 5.1 定义 trait
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/stream.rs`

定义：
- `TunnelListener`
- `TunnelDialer`
- `TunnelStream`

### 5.2 实现 TCP
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/tcp.rs`

支持：
- listen
- dial
- stream split

### 5.3 实现 WS/WSS
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/ws.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/tls.rs`

要求：
- 可复用当前 WebSocket 依赖
- WSS 支持证书配置

### 5.4 封装入站/出站启动器
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/listener.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/dialer.rs`

实现：
- 多 `-s`
- 多 `-c`
- 与 `SessionHub` 对接

## 交付物
- Tunnel 抽象完成

## 验收标准
- `fusion -s tcp://...`
- `fusion -c tcp://...`
- `fusion -s ws://...`
- `fusion -c ws://...`
都能建立统一 session

---

# Phase 6：实现 Serve 抽象

## 目标
把 `-l` / `-r` 变成统一服务定义，而不是当前 server/client 的命令概念。

## 步骤

### 6.1 定义服务 trait
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/serve/service.rs`

定义：
- `LocalService`
- `RemoteServiceRequest`
- `ServiceFactory`

### 6.2 实现 socks5 入口
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/serve/socks5.rs`

职责：
- 本地监听 socks5
- 每个连接转换为 `OpenStream`
- 把目标地址发给远端

### 6.3 实现 raw / port forward 出口
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/serve/raw.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/serve/portfwd.rs`

职责：
- 接收远端 `OpenStream`
- 在本机或目标地址建立 TCP 连接
- 双向转发字节流

### 6.4 定义 `-l` 和 `-r` 语义
建议首版：
- `-l socks5://127.0.0.1:1080`：在本地开入口
- `-r raw://host:port`：对端为每条流建立出口
- `-r port://127.0.0.1:8080->host:port`：对端做固定转发

## 交付物
- 本地 socks5 到远端 raw connect 可跑通

## 验收标准
- 类似下面命令可工作：

```bash
fusion -s tcp://0.0.0.0:34996
fusion -c tcp://127.0.0.1:34996 -l socks5://127.0.0.1:1080 -r raw://
```

---

# Phase 7：把植入能力并入 Agent Capability

## 目标
不再存在“专门的 implant 进程”，而是每个 Agent 可选择暴露执行能力。

## 步骤

### 7.1 定义 capability registry
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/agent/capabilities.rs`

内容：
- shell
- screenshot
- file-upload
- file-download
- whoami
- platform-specific extensions

### 7.2 定义任务调度器
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/task/dispatcher.rs`

职责：
- 接收 `TaskRequest`
- 根据 capability 分发到具体实现
- 返回 `TaskResult`

### 7.3 迁移旧 implant 能力
来源：
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_implant/core/run_linux.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_implant/core/run_windows.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_implant/core/tasks/*`

迁移为：
- `task/shell.rs`
- `task/file.rs`
- `task/screenshot.rs`

### 7.4 定义 CLI 任务子命令
建议在 `fusion` 中加入：
- `fusion task --peer <id> shell "whoami"`
- `fusion task --peer <id> screenshot`
- `fusion task --peer <id> download /tmp/a`

注意：
- 这时 CLI 不再是“连接 server 的客户端”
- 而是一个本地 Agent 的管理视图

## 交付物
- 任意 Agent 都可执行任务或响应任务

## 验收标准
- 两个 `fusion` 进程互连后，可对其中一方发 task 并获得结果

---

# Phase 8：实现 Relay 与多跳基础能力

## 目标
让一个 Agent 同时 `-c` 上游并 `-s` 下游，成为 relay。

## 步骤

### 8.1 定义 route table
新建：
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/route.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/agent/registry.rs`

内容：
- peer 直连信息
- 下一跳映射
- service 暴露点

### 8.2 实现 agent announce
节点连接建立后广播：
- 自己的 Agent ID
- 邻接关系
- 暴露服务
- capability 列表

### 8.3 实现 route forward
当收到目标不是本节点的消息时：
- 查 route table
- 转发到下一跳 peer

### 8.4 先做简化版
首版路由策略建议：
- 仅支持树状/链状拓扑
- 不做复杂最短路
- 使用“首次可达路径”

## 交付物
- relay 基础转发

## 验收标准
- 三节点链路打通：

```bash
fusion -s tcp://0.0.0.0:34996
fusion -c tcp://console:34996 -s tcp://0.0.0.0:35000
fusion -c tcp://relay:35000 -l socks5://127.0.0.1:1080 -r raw://
```

---

# Phase 9：迁移控制面，退役旧 client/server 模型

## 目标
把当前“client 管 server”的控制面，改成“本地 Agent 管网络中 peer”。

## 步骤

### 9.1 新建本地控制命令
在 `fusion` 中增加子命令：
- `peers list`
- `peers info`
- `services list`
- `routes list`
- `task ...`

### 9.2 替换旧 client 的命令模型
废弃依赖：
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_client/client/*`

保留有价值部分：
- 参数提示体验
- readline 历史记录
- 命令解析组织方式

### 9.3 退役旧 server 中心模型
逐步废弃：
- `listener` 持久化概念
- “服务端数据库唯一真相源”
- WebSocket 文本命令入口

替代为：
- runtime 内部状态
- 可选 sqlite 持久化 peer/service/route 快照

## 交付物
- 旧 client / server 功能被新 CLI 管理面替代

## 验收标准
- 不再需要运行 `fusion-server` 和 `fusion-client`

---

# Phase 10：收口、删除旧模块、稳定化

## 目标
删掉旧三端逻辑，只保留单体架构。

## 步骤

### 10.1 删除旧入口
删除：
- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion-server.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion-client.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion-implant.rs`

### 10.2 删除旧模块
逐步删除：
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_server`
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_client`
- `/Users/qi4l/lang/Rust/Fusion/src/fusion_implant`

### 10.3 清理配置
把旧：
- `/Users/qi4l/lang/Rust/Fusion/config.json`

改造成：
- 命令行参数优先
- 可选 `fusion.toml`
- 数据目录下的 runtime state

### 10.4 清理依赖
移除只为旧架构存在的依赖和兼容代码。

### 10.5 文档重写
重写：
- README
- quick start
- relay 示例
- socks5 示例
- task 示例

## 交付物
- 单一二进制可运行
- 旧三端彻底退役

## 验收标准
- `cargo build --bin fusion` 成功
- 文档只描述单体 Agent 模型

---

## 7. 里程碑建议

### Milestone A：单体入口成型
完成 Phase 1~2

输出：
- `fusion` 入口
- 统一配置
- 公共模块

### Milestone B：统一协议 + 单跳连通
完成 Phase 3~5

输出：
- PeerSession
- TCP/WS tunnel
- hello/heartbeat/reconnect

### Milestone C：socks5/portfwd MVP
完成 Phase 6

输出：
- `-l` / `-r` 可用
- 可做基础代理/转发

### Milestone D：Agent 执行能力并入
完成 Phase 7

输出：
- shell / screenshot / file 统一 task 能力

### Milestone E：Relay 与多跳基础
完成 Phase 8~9

输出：
- relay
- 基础 route
- 新控制面

### Milestone F：旧架构下线
完成 Phase 10

输出：
- 彻底单体化

---

## 8. 每阶段建议的 Git 提交粒度

建议每个阶段至少拆成以下粒度：

1. `feat(cli): add unified fusion entry and config parser`
2. `refactor(crypto): move shared crypto into common module`
3. `feat(protocol): add common frame and message codec`
4. `feat(session): implement peer session and heartbeat`
5. `feat(tunnel): support tcp and websocket transports`
6. `feat(serve): add local socks5 and remote raw service`
7. `feat(task): migrate shell screenshot and file capabilities`
8. `feat(route): add relay and route forwarding`
9. `refactor(control-plane): replace old client/server command path`
10. `chore(cleanup): remove legacy three-role modules`

这样回滚和 bisect 更容易。

---

## 9. 风险清单

### 风险 1：一次性协议替换过大
**应对：** 先做 memory/tcp 上的最小 protocol MVP，再迁移其它 transport。

### 风险 2：旧能力迁移时平台差异爆炸
**应对：** task 能力先按 linux/windows/macos 分文件保留，不要首轮统一到一份实现。

### 风险 3：既要“代理网络”又要“任务执行”导致模型过杂
**应对：** 明确区分两类通道：
- `stream-based traffic`：用于代理/转发
- `rpc/task-based request`：用于任务能力

### 风险 4：过早做多跳导致调试困难
**应对：** 先做单跳，再做 relay，再做 route announce。

### 风险 5：数据库拖累架构
**应对：** 首版尽量以内存态为主，SQLite 仅做可选快照，不要让它成为中心依赖。

---

## 10. 测试计划

## 10.1 单元测试
优先覆盖：
- URL 解析
- frame encode/decode
- message serde
- key exchange
- encrypt/decrypt
- reconnect backoff
- route table lookup

## 10.2 集成测试
建议新建：
- `/Users/qi4l/lang/Rust/Fusion/tests/`

至少包含：
1. `tcp_peer_handshake.rs`
2. `ws_peer_handshake.rs`
3. `task_shell_roundtrip.rs`
4. `socks5_to_raw_stream.rs`
5. `relay_chain_roundtrip.rs`

## 10.3 手工验收场景

### 场景 A：单跳 socks5
```bash
fusion -s tcp://0.0.0.0:34996
fusion -c tcp://127.0.0.1:34996 -l socks5://127.0.0.1:1080 -r raw://
```

### 场景 B：relay
```bash
fusion -s tcp://0.0.0.0:34996
fusion -c tcp://127.0.0.1:34996 -s tcp://0.0.0.0:35000
fusion -c tcp://127.0.0.1:35000 -l socks5://127.0.0.1:1080 -r raw://
```

### 场景 C：远程任务执行
```bash
fusion -s tcp://0.0.0.0:34996
fusion -c tcp://127.0.0.1:34996
fusion task --peer <peer-id> shell "whoami"
```

---

## 11. 推荐实施顺序（最务实版本）

如果你想尽快做出能跑的东西，建议按下面顺序推进：

1. 新建 `fusion` 入口
2. 做统一参数解析
3. 抽公共 crypto / utils / task
4. 做统一 frame/message
5. 做 TCP peer session
6. 做 heartbeat + reconnect
7. 做 `-l socks5` + `-r raw`
8. 做 relay
9. 把 shell/file/screenshot 并入 capability
10. 最后再替换旧 client/server 管理面

这条路线的优点是：
- 最快产出 MVP
- 最快验证“rem 风格架构”是否成立
- 避免先陷入复杂交互式 CLI 和数据库设计

---

## 12. 首版 MVP 定义（建议）

如果需要控制范围，建议把 MVP 定义为：

### 必做
- 单二进制 `fusion`
- `-s tcp://`
- `-c tcp://`
- `-l socks5://`
- `-r raw://`
- 统一 peer handshake
- AEAD 加密通信
- 基础重连
- relay
- shell task

### 选做
- `ws://` / `wss://`
- screenshot
- upload/download
- route announce

### 暂缓
- DNS / ICMP / WG
- 多算法 wrapper
- 完整多跳智能路由
- 库导出

---

## 13. 本计划对应的第一批实际落地文件建议

建议你下一轮真正开始编码时，第一批先创建这些文件：

- `/Users/qi4l/lang/Rust/Fusion/src/bin/fusion.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/app/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/app/cli.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/app/config.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/app/runtime.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/frame.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/message.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/protocol/codec.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/session/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/session/peer.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/tunnel/tcp.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/serve/mod.rs`
- `/Users/qi4l/lang/Rust/Fusion/src/serve/service.rs`

第一批目标不要太大：

> `fusion -s tcp://...` 与 `fusion -c tcp://...` 能建立加密 session，并发送一个 hello/heartbeat。

只要这一步打通，后面都能往上叠。

---

## 14. 结论

这次改造的本质不是“把 3 个 bin 合并”，而是：

- 从**中心化 C2 三角色模型**
- 迁移到**单体 Agent、统一协议、参数驱动角色、支持 relay 的对等网络模型**

最关键的三个抓手是：

1. **统一入口与参数系统**
2. **统一 peer/session/protocol**
3. **统一 service/capability 抽象**

只要这三件事做对，Fusion 就能逐步长成你文档里那个 `rem` 风格的东西。
