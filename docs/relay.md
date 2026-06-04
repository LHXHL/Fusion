# Relay 示例

当前 `fusion` 支持一个节点同时：

- `-c` 连接上游
- `-s` 监听下游

这就是最小 relay 形态。

## 三节点链路

### 上游节点

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -a upstream
```

### Relay 节点

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -s tcp://0.0.0.0:35000 \
  -a relay
```

### 下游节点

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:35000 \
  -a leaf
```

## 查看路由

```bash
cargo run --bin fusion -- routes list
cargo run --bin fusion -- peers list
```
