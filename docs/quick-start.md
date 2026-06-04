# Quick Start

本文档只描述当前 `fusion` 单体入口的真实使用方式。

## 1. 构建

```bash
cargo build --bin fusion
```

## 2. 查看帮助

```bash
cargo run --bin fusion -- --help
```

## 3. 起一个监听节点

```bash
cargo run --bin fusion -- \
  -s tcp://0.0.0.0:34996 \
  -a node-a
```

## 4. 起一个连接节点

```bash
cargo run --bin fusion -- \
  -c tcp://127.0.0.1:34996 \
  -a node-b
```

## 5. 查看状态

```bash
cargo run --bin fusion -- status
cargo run --bin fusion -- peers list
cargo run --bin fusion -- routes list
cargo run --bin fusion -- services list
```

## 6. 使用配置文件

```bash
cp fusion.toml.example fusion.toml
cargo run --bin fusion -- --config ./fusion.toml
```

注意：
- 命令行参数优先
- `fusion.toml` 只做默认值来源
- 状态快照写到 `data-dir`
