# Fusion 单体 Agent 架构改造项目收尾 TODO

> 基于当前仓库实际状态，对 `Fusion-单体Agent架构改造项目计划.md` 的未闭环项进行按优先级整理。
>
> 当前判断：
> - 已基本完成单体 Agent 主体改造
> - 可编译、可测试、可运行
> - 主要缺口集中在：基线/回归资产、部分计划文件未落地、个别功能未完全补齐、文档与结构一致性收口

---

## P0（最高优先级）——先补“可交付性”和“可回归性”缺口

这些项目不会显著改变架构，但会直接影响项目是否能稳定验收、交接、回归和继续演进。

### 1. 补齐 Phase 0 基线文档
**目标**：补上计划中明确要求的基线说明文件。

**新增文件**：
- `/Users/qi4l/lang/Rust/Fusion-master/docs/baseline-current-capabilities.md`

**至少应包含**：
- 当前支持的 tunnel 类型
  - `tcp://`
  - `ws://`
  - `wss://`
- 当前支持的 service 类型
  - `socks5://`
  - `raw://`
- 当前支持的 task 类型
  - shell
  - screenshot
  - upload
  - download
- 当前 CLI 参数清单
- 当前控制子命令
  - `status`
  - `peers`
  - `routes`
  - `services`
  - `task`
- 当前状态持久化文件
  - 例如 `data-dir/runtime-status.json`
- 当前协议消息类型
  - Hello / HelloAck / Heartbeat / AgentAnnounce / RouteUpdate / TaskRequest / TaskResult / StreamOpen / StreamData / StreamClose
- 已知限制
  - 暂未实现 `port://`
  - 暂未按计划拆出 `tls.rs` / `listener.rs` / `dialer.rs`
  - 暂无独立 protocol 文档草案

**验收标准**：
- 新成员只看该文件即可快速理解当前“真实能力边界”

---

### 2. 补齐手动 smoke 回归脚本
**目标**：建立最小手工回归入口。

**新增文件**：
- `/Users/qi4l/lang/Rust/Fusion-master/scripts/regression/manual-smoke.sh`

**脚本建议覆盖的最小场景**：
1. `cargo build --bin fusion`
2. `fusion --help`
3. 单节点监听启动
4. 双节点 TCP 互连
5. 双节点 WS 互连
6. `task shell` 基本回归
7. `status / peers / routes / services` 输出回归

**建议特性**：
- 使用临时目录隔离 `data-dir`
- 自动清理后台进程
- 明确打印 PASS/FAIL
- 失败时输出关键日志路径

**验收标准**：
- 新机器上执行一次脚本，即可快速知道主链路是否正常

---

### 3. 把现有测试能力映射回计划验收项
**目标**：解决“测试存在，但和计划阶段验收没有直接映射”的问题。

**建议新增文件**：
- `/Users/qi4l/lang/Rust/Fusion-master/docs/test-matrix.md`

**内容建议**：
| 计划阶段 | 验收点 | 当前测试/命令 | 状态 |
|---|---|---|---|
| Phase 1 | `fusion --help` | `cargo run --bin fusion -- --help` | 已覆盖 |
| Phase 3 | hello/heartbeat/task echo | 单元测试 / runtime 测试 | 已覆盖 |
| Phase 6 | socks5 → raw | runtime relay/socks5 roundtrip | 已覆盖 |
| Phase 8 | relay 三节点链路 | runtime relay tests | 已覆盖 |

**验收标准**：
- 能从计划阶段直接追踪到测试或验证命令

---

## P1（高优先级）——补齐实际功能缺口

这些项目属于“计划里写了、当前还不完整”的内容，优先级仅次于回归资产。

### 4. 实现 `port://` 远端服务
**现状**：
- 当前 `socks5://` + `raw://` 已可用
- 计划中的 `port://127.0.0.1:8080->host:port` 尚未落地

**建议新增文件**：
- `/Users/qi4l/lang/Rust/Fusion-master/src/serve/portfwd.rs`

**建议实现能力**：
- 解析 `port://listen_host:listen_port->target_host:target_port`
- 对端固定建立 TCP 转发
- 支持被 `ServiceDefinition` 识别和挂载
- 在 `services list` 中显示

**同时需要修改**：
- `/Users/qi4l/lang/Rust/Fusion-master/src/serve/service.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/cli.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/utils/url.rs`
- `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime.rs`

**验收标准**：
- `-r port://127.0.0.1:8080->host:port` 可解析、可显示、可转发

---

### 5. 完整梳理 `wss://` 语义与实现边界
**现状**：
- URL 解析支持 `wss://`
- runtime 路径中也接受 `ws|wss`
- 但计划中的 `src/tunnel/tls.rs` 并不存在
- 证书配置能力没有独立模块化收口

**目标**：
明确“当前的 WSS 是真实完成，还是仅接口保留”。

**建议动作**：
- 检查 `tokio-tungstenite` 当前用法是否真正覆盖 TLS 连接场景
- 若已支持，则补文档和测试
- 若未完整支持，则：
  - 新增 `/Users/qi4l/lang/Rust/Fusion-master/src/tunnel/tls.rs`
  - 明确证书来源、校验策略、配置项

**至少补齐以下之一**：
1. **实现级收口**：真的补 `tls.rs` 和 WSS 配置
2. **文档级收口**：明确声明“当前仅保留 URL 入口，不承诺完整 TLS 配置能力”

**验收标准**：
- 团队成员不再对 `wss://` 当前成熟度产生歧义

---

### 6. 补协议文档草案
**现状**：
- 协议实现已经有 `frame.rs` / `message.rs` / `codec.rs`
- 但缺少独立协议说明文档

**建议新增文件**：
- `/Users/qi4l/lang/Rust/Fusion-master/docs/protocol.md`

**建议内容**：
- Frame 结构
- Header 字段含义
- MessageType 列表
- 各消息方向
- `src_agent` / `dst_agent` / `stream_id` 语义
- relay 转发时的约定
- task 请求/响应约定
- 状态码/错误传播约定

**验收标准**：
- 不看实现代码也能理解线协议

---

## P2（中优先级）——结构收口，让代码更符合原计划设计

这些项目主要提升架构整洁度与维护性，不是当前可用性的第一阻塞项。

### 7. 拆出 tunnel 启动器抽象：`listener.rs` / `dialer.rs`
**现状**：
- 监听/连接逻辑大量集成在 `/Users/qi4l/lang/Rust/Fusion-master/src/app/runtime.rs`
- 计划中的：
  - `src/tunnel/listener.rs`
  - `src/tunnel/dialer.rs`
  尚未落地

**目标**：
把 runtime 中的“根据 endpoint 启动 listener / dialer”的逻辑下沉到 tunnel 层。

**建议改造方向**：
- `listener.rs` 负责：
  - 多 `-s` 启动
  - TCP/WS 分派
  - 返回统一入站 session 事件
- `dialer.rs` 负责：
  - 多 `-c` 启动
  - retry/reconnect
  - 返回统一出站 session 事件

**收益**：
- `runtime.rs` 体积下降
- tunnel 与 runtime 的职责边界更清晰

---

### 8. 评估并决定是否补 `protocol/stream.rs`
**现状**：
- 多路复用 stream 能力实际上已经存在
- 但计划中的 `/src/protocol/stream.rs` 不存在
- 目前 stream 相关语义散布在：
  - `protocol/message.rs`
  - `session/stream.rs`
  - `tunnel/*_mux.rs`

**建议选择其一**：
1. **补文件**：新增 `protocol/stream.rs`，统一抽象 stream 生命周期与事件
2. **修正文档**：明确说明 stream 设计已下沉到 session/tunnel 层，不再单列该文件

**验收标准**：
- 设计文档与代码结构一致

---

### 9. 评估并决定是否补 `agent/state.rs`
**现状**：
- 当前状态管理主要落在：
  - `SessionHub`
  - `AgentRegistry`
  - runtime snapshot
- 计划中的 `/src/agent/state.rs` 不存在

**建议选择其一**：
1. 新增 `agent/state.rs`，封装节点运行态聚合结构
2. 明确废弃这个计划文件，把状态职责正式归并到 `registry + session + runtime snapshot`

**验收标准**：
- 不再存在“计划中要有，但仓库里没有”的悬空设计项

---

### 10. 评估并决定是否补 `crypto/wrapper.rs` 与 `utils/fs.rs`
**现状**：
- 当前项目并不依赖这两个文件才能运行
- 但它们仍属于计划中列出的结构

**建议**：
- 如果短期不会接入 wrapper 层或额外 FS 抽象，建议直接在文档中声明“本期取消”
- 不建议为了凑目录空壳式新增无意义模块

**验收标准**：
- 计划结构与实际工程结构一致，不留伪需求

---

## P3（较低优先级）——文档与体验完善

### 11. 更新 README 中的绝对路径示例
**现状**：
- README 和 docs 中存在历史路径，如 `/Users/bytedance/Desktop/git/Fusion`
- 与当前仓库实际路径不一致

**建议修改文件**：
- `/Users/qi4l/lang/Rust/Fusion-master/README.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/quick-start.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/relay.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/socks5-and-raw.md`
- `/Users/qi4l/lang/Rust/Fusion-master/docs/task.md`

**建议**：
- 尽量统一写成相对通用形式：
  - `cd <repo>`
  - `cargo build --bin fusion`
- 避免保留开发者本机路径痕迹

---

### 12. 给 README 增加“当前未实现项”小节
**建议新增内容**：
- 未实现 `port://`
- `wss://` 完整 TLS 配置能力待确认/待补全
- 尚未提供独立 smoke 脚本
- 若有更多限制，也应集中列出

**收益**：
- 降低使用者误判成熟度的风险

---

### 13. 为控制命令补示例输出
**建议补充到 README 或 docs**：
- `status`
- `peers list`
- `routes list`
- `services list`
- `task shell`

**目标**：
让用户知道命令不仅存在，而且输出长什么样

---

## 推荐执行顺序

### 第一轮（建议本周内完成）
1. 补 `docs/baseline-current-capabilities.md`
2. 补 `scripts/regression/manual-smoke.sh`
3. 补 `docs/test-matrix.md`
4. 补 `docs/protocol.md`

### 第二轮（功能收口）
5. 实现 `port://`
6. 梳理 `wss://` 的真实支持边界并补测试/文档

### 第三轮（结构收口）
7. 拆 `listener.rs` / `dialer.rs`
8. 决定 `protocol/stream.rs` 去留
9. 决定 `agent/state.rs` 去留
10. 决定 `crypto/wrapper.rs` / `utils/fs.rs` 去留

### 第四轮（文档体验）
11. 清理 README 和 docs 中的历史路径
12. 增加未实现项说明
13. 增加命令输出示例

---

## 建议的完成定义

建议把“项目真正收尾完成”的判断标准定义为：

- `cargo build --bin fusion` 稳定成功
- `cargo test` 全通过
- 有最小 smoke 脚本
- 有 baseline 文档
- 有 protocol 文档
- README 与当前真实能力一致
- 计划中未落地的结构项，要么实现、要么正式删项说明
- `port://` 和 `wss://` 的支持边界清晰

---

## 一句话结论

当前项目已经**基本完成单体 Agent 主体改造**；
后续收尾工作的重点不是“大改架构”，而是：

> **补基线、补回归、补协议文档、补缺失功能、清理计划与实现之间的不一致。**
