# WSS / TLS 传输配置

Fusion 当前仅在 **`wss://`** WebSocket 隧道上提供 TLS。`tcp://`、`simplex+http://` 等 scheme 本身不内置 TLS；若需 HTTPS 外层加密，应在前置反向代理或独立 TLS 终止层处理。

实现入口：[`src/tunnel/tls.rs`](../src/tunnel/tls.rs)。

---

## 1. 服务端（`-l` / `listens`）

| 参数 | 必填 | 说明 |
|------|------|------|
| `tls-cert` | 是 | 服务端证书 PEM 路径 |
| `tls-key` | 是 | 服务端私钥 PEM 路径 |
| `tls-client-ca` | 否 | 客户端 CA PEM；配置后启用 **双向 TLS（mTLS）**，仅持有该 CA 签发证书的客户端可连接 |

示例：

```text
wss://0.0.0.0:8443/tunnel?tls-cert=/path/to/cert.pem&tls-key=/path/to/key.pem
wss://0.0.0.0:8443/tunnel?tls-cert=/path/to/cert.pem&tls-key=/path/to/key.pem&tls-client-ca=/path/to/client-ca.pem
```

---

## 2. 客户端（`-c` / `connects` 及 up/down 方向）

| 参数 | 说明 |
|------|------|
| （默认） | 使用 **操作系统信任库** 校验服务端证书；SNI 取自 URL host |
| `tls-ca` | 附加自定义 CA PEM（与系统 CA 叠加） |
| `tls-client-cert` + `tls-client-key` | 客户端证书身份（mTLS）；**必须成对出现** |
| `tls-insecure=1` | 跳过证书与主机名校验；**仅用于本地/测试** |

示例：

```text
wss://127.0.0.1:8443/tunnel?tls-ca=/path/to/ca.pem
wss://127.0.0.1:8443/tunnel?tls-client-cert=/path/to/client.pem&tls-client-key=/path/to/client-key.pem
wss://127.0.0.1:8443/tunnel?tls-insecure=1
```

### SNI 与主机名校验

- 连接器 SNI / 主机名校验目标为 URL 中的 **host** 部分。
- 自签证书请在 SAN 中包含实际连接 host（如 `localhost`、`127.0.0.1`），或使用 `tls-insecure=1`（测试）。
- 生产环境应使用与 DNS/访问地址一致的证书，并避免 `tls-insecure`。

---

## 3. 校验策略矩阵

| 场景 | 推荐配置 |
|------|----------|
| 内网自签 + 固定 CA | 客户端 `tls-ca`；服务端常规 cert/key |
| 公网 / 系统 CA 签发 | 客户端默认（系统 CA） |
| 双向认证 | 服务端 `tls-client-ca` + 客户端 cert/key 对 |
| 本地快速联调 | `tls-insecure=1`（勿用于生产） |

---

## 4. 运维可观测（status）

`status --json` 的 `config.tls` 字段汇总 **非敏感** TLS 使用情况（计数，不含 PEM 路径）：

| 字段 | 含义 |
|------|------|
| `wss_listen_endpoints` | 配置了 `wss://` 的 listen 数量 |
| `wss_connect_endpoints` | 配置了 `wss://` 的 connect/up/down 数量 |
| `insecure_enabled` | 启用 `tls-insecure` 的 connect 数 |
| `custom_ca_configured` | 配置了 `tls-ca` 的 connect 数 |
| `client_identity_configured` | 配置了客户端 cert/key 的 connect 数 |
| `listener_mutual_tls_enabled` | 配置了 `tls-client-ca` 的 listen 数 |

文本 status 对应行前缀：`config.tls.*`。

---

## 5. 明确不支持（I2）

| 能力 | 状态 |
|------|------|
| 证书 / 密钥 **热更新**（不重启 listener） | **不支持** — 修改 PEM 后需重启进程 |
| `tcp://` 内联 TLS | **不支持** — 使用 `wss://` 或外部 TLS 终止 |
| 自动 ACME / Let's Encrypt | **不支持** — 需外部签发后配置 PEM 路径 |
| 按 SNI 多证书动态选择 | **不支持** — 单 listener 单 cert/key 对 |

---

## 6. 相关文档

- 能力基线：[`baseline-current-capabilities.md`](baseline-current-capabilities.md)
- 配置示例：[`fusion.toml.example`](../fusion.toml.example)
- 回归矩阵：[`test-matrix.md`](test-matrix.md)
