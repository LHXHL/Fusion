# Task 示例

当前 `fusion` 已支持统一 task 入口。

## shell

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  --task-peer <PEER_ID> \
  task shell "whoami"
```

## screenshot

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  --task-peer <PEER_ID> \
  --task-save ./screen.png \
  task screenshot
```

## download

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  --task-peer <PEER_ID> \
  --task-save ./artifact.bin \
  task download /tmp/remote-file
```

## upload

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  --task-peer <PEER_ID> \
  task upload ./local.bin /tmp/remote.bin
```

## 查看 peer

```bash
cargo run --bin fusion -- peers info <PEER_ID>
```
