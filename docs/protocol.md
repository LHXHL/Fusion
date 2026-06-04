# Fusion 统一 Agent 协议说明（当前实现）

本文档描述当前仓库中已经落地的统一消息协议。

## 1. 编码方式

当前使用：
- `serde_json`
- 单帧 JSON 编码/解码

对应实现：
- `/Users/qi4l/lang/Rust/Fusion-master/src/protocol/codec.rs`

## 2. Frame 结构

定义位置：
- `/Users/qi4l/lang/Rust/Fusion-master/src/protocol/frame.rs`

### Header 字段
- `version`：协议版本，当前固定为 `1`
- `msg_type`：消息类型
- `session_id`：保留字段，当前未系统化使用
- `stream_id`：多路复用流 ID
- `src_agent`：源 Agent ID
- `dst_agent`：目标 Agent ID

### Body
- `message`：消息体枚举

## 3. MessageType 列表

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

## 4. 握手流程

### 4.1 建链后顺序
1. 发 `Hello`
2. 收 `HelloAck`
3. 发 `Heartbeat`
4. 收 `Heartbeat`

### 4.2 语义
- `Hello`：声明本端 agent_id / agent_name / capabilities / protocol_version
- `HelloAck`：接受握手并回传 peer_id
- `Heartbeat`：保持会话存活

## 5. 控制消息

### 5.1 `AgentAnnounce`
用于广播：
- 节点 ID
- 节点名称
- capability 列表
- 暴露服务列表

### 5.2 `RouteUpdate`
用于传播：
- 目标节点
- 下一跳
- hops
- service/capability 元数据

## 6. Task 消息

### 6.1 `TaskRequest`
字段：
- `task_id`
- `action`
- `args`
- `data_hex`

### 6.2 `TaskResult`
字段：
- `task_id`
- `ok`
- `output`
- `data_hex`

### 6.3 当前支持的 action
- `Shell`
- `Screenshot`
- `FileUpload`
- `FileDownload`

## 7. Stream 消息

### 7.1 `StreamOpen`
字段：
- `service`
- `target_host`
- `target_port`

当前 `service` 主要用于：
- `raw`

### 7.2 `StreamData`
字段：
- `data_hex`

说明：
- 当前字节流数据做 hex 编码后承载在 JSON 中

### 7.3 `StreamClose`
字段：
- `reason`

## 8. 路由约定

### 8.1 本地处理
如果 `dst_agent` 为空或等于本节点，则本地消费。

### 8.2 转发处理
如果 `dst_agent` 指向其它节点：
- 查询 route table
- 找到 `next_hop`
- 原样转发 frame

### 8.3 无路由
若无可达下一跳：
- 当前实现直接丢弃
- 记录 `relay.drop.no_route`

## 9. 错误传播

当前实现中多数错误通过以下方式暴露：
- 本地 stderr 日志
- `TaskResult.ok=false`
- stream close / runtime status snapshot

当前尚未实现统一错误码表。

## 10. 与目录结构对应关系

- Frame / Message：`src/protocol/*`
- Stream 生命周期补充抽象：`src/protocol/stream.rs`
- Session：`src/session/*`
- Relay / Route：`src/agent/registry.rs` + `src/session/router.rs`
