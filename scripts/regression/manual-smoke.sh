#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

TMP_ROOT="$(mktemp -d -t fusion-smoke-XXXXXX)"
PIDS=()
LOGS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "[INFO] logs kept in: $TMP_ROOT"
}
trap cleanup EXIT

pass() { echo "[PASS] $*"; }
fail() { echo "[FAIL] $*"; exit 1; }

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local tries="${3:-50}"
  for _ in $(seq 1 "$tries"); do
    if grep -q "$pattern" "$file" 2>/dev/null; then
      return 0
    fi
    sleep 0.2
  done
  return 1
}

wait_for_file() {
  local file="$1"
  local tries="${2:-50}"
  for _ in $(seq 1 "$tries"); do
    [[ -f "$file" ]] && return 0
    sleep 0.2
  done
  return 1
}

echo "[INFO] temp root: $TMP_ROOT"

cargo build --bin fusion >/dev/null
pass "cargo build --bin fusion"

cargo run --quiet --bin fusion -- --help >/dev/null
pass "fusion --help"

TCP_A_DIR="$TMP_ROOT/tcp-a"
TCP_B_DIR="$TMP_ROOT/tcp-b"
mkdir -p "$TCP_A_DIR" "$TCP_B_DIR"
TCP_A_LOG="$TMP_ROOT/tcp-a.log"
TCP_B_LOG="$TMP_ROOT/tcp-b.log"
LOGS+=("$TCP_A_LOG" "$TCP_B_LOG")

cargo run --quiet --bin fusion -- \
  --data-dir "$TCP_A_DIR" \
  -s tcp://127.0.0.1:39096 \
  -a smoke-tcp-a \
  >"$TCP_A_LOG" 2>&1 &
PIDS+=("$!")
wait_for_log "$TCP_A_LOG" 'listen.active=tcp://127.0.0.1:39096' || fail "tcp listener startup"
pass "单节点 TCP 监听启动"

cargo run --quiet --bin fusion -- \
  --data-dir "$TCP_B_DIR" \
  -c tcp://127.0.0.1:39096 \
  -s tcp://127.0.0.1:39097 \
  -a smoke-tcp-b \
  >"$TCP_B_LOG" 2>&1 &
PIDS+=("$!")
wait_for_log "$TCP_A_LOG" 'session.inbound.peer=' || fail "tcp inbound peer established"
wait_for_file "$TCP_A_DIR/runtime-status.json" || fail "tcp runtime-status snapshot"
pass "双节点 TCP 互连"

KEY_A_DIR="$TMP_ROOT/key-a"
mkdir -p "$KEY_A_DIR"
KEY_A_LOG="$TMP_ROOT/key-a.log"
LOGS+=("$KEY_A_LOG")

cargo run --quiet --bin fusion -- \
  --data-dir "$KEY_A_DIR" \
  -s tcp://127.0.0.1:39098 \
  -k smoke-shared-key \
  -a smoke-key-a \
  >"$KEY_A_LOG" 2>&1 &
PIDS+=("$!")
wait_for_log "$KEY_A_LOG" 'listen.active=tcp://127.0.0.1:39098' || fail "keyed tcp listener startup"

KEY_TASK_LOG="$TMP_ROOT/key-task.log"
LOGS+=("$KEY_TASK_LOG")
cargo run --quiet --bin fusion -- \
  -c tcp://127.0.0.1:39098 \
  -k smoke-shared-key \
  task shell 'echo fusion-key-smoke' \
  >"$KEY_TASK_LOG" 2>&1
grep -q 'task.result.ok=true' "$KEY_TASK_LOG" || fail "keyed task shell did not return ok=true"
grep -q 'fusion-key-smoke' "$KEY_TASK_LOG" || fail "keyed task shell output mismatch"
pass "共享密钥 TCP/task 链路"

cargo run --quiet --bin fusion -- --data-dir "$TCP_A_DIR" peers list >/dev/null
cargo run --quiet --bin fusion -- --data-dir "$TCP_A_DIR" routes list >/dev/null
cargo run --quiet --bin fusion -- --data-dir "$TCP_A_DIR" services list >/dev/null
cargo run --quiet --bin fusion -- --data-dir "$TCP_A_DIR" status >/dev/null
pass "status / peers / routes / services 输出"

WS_A_DIR="$TMP_ROOT/ws-a"
WS_B_DIR="$TMP_ROOT/ws-b"
mkdir -p "$WS_A_DIR" "$WS_B_DIR"
WS_A_LOG="$TMP_ROOT/ws-a.log"
WS_B_LOG="$TMP_ROOT/ws-b.log"
LOGS+=("$WS_A_LOG" "$WS_B_LOG")

cargo run --quiet --bin fusion -- \
  --data-dir "$WS_A_DIR" \
  -s ws://127.0.0.1:39196/tunnel \
  -a smoke-ws-a \
  >"$WS_A_LOG" 2>&1 &
PIDS+=("$!")
wait_for_log "$WS_A_LOG" 'listen.active=ws://127.0.0.1:39196/tunnel' || fail "ws listener startup"

cargo run --quiet --bin fusion -- \
  --data-dir "$WS_B_DIR" \
  -c ws://127.0.0.1:39196/tunnel \
  -s ws://127.0.0.1:39197/tunnel \
  -a smoke-ws-b \
  >"$WS_B_LOG" 2>&1 &
PIDS+=("$!")
wait_for_log "$WS_A_LOG" 'session.inbound.peer=' || fail "ws inbound peer established"
pass "双节点 WS 互连"

TASK_DIR="$TMP_ROOT/task"
mkdir -p "$TASK_DIR"
TASK_LOG="$TMP_ROOT/task.log"
LOGS+=("$TASK_LOG")

cargo run --quiet --bin fusion -- \
  --data-dir "$TASK_DIR" \
  -c tcp://127.0.0.1:39096 \
  task shell 'echo fusion-smoke-task' \
  >"$TASK_LOG" 2>&1

grep -q 'task.result.ok=true' "$TASK_LOG" || fail "task shell did not return ok=true"
grep -q 'fusion-smoke-task' "$TASK_LOG" || fail "task shell output mismatch"
pass "task shell 基本回归"

PORT_DIR="$TMP_ROOT/port"
mkdir -p "$PORT_DIR"
PORT_LOG="$TMP_ROOT/port.log"
ECHO_LOG="$TMP_ROOT/port-echo.log"
LOGS+=("$PORT_LOG" "$ECHO_LOG")

python3 - <<'PY' >"$ECHO_LOG" 2>&1 &
import socket, threading
srv = socket.socket()
srv.bind(('127.0.0.1', 39281))
srv.listen()
while True:
    conn, _ = srv.accept()
    data = conn.recv(4096)
    conn.sendall(data)
    conn.close()
PY
PIDS+=("$!")
sleep 0.5

cargo run --quiet --bin fusion -- \
  --data-dir "$PORT_DIR" \
  -r "port://127.0.0.1:39280->127.0.0.1:39281" \
  -a smoke-port \
  >"$PORT_LOG" 2>&1 &
PIDS+=("$!")
wait_for_log "$PORT_LOG" 'service.remote.active=port://127.0.0.1:39280->127.0.0.1:39281' || fail "port forward listener startup"
python3 - <<'PY'
import socket
s = socket.create_connection(('127.0.0.1', 39280), timeout=3)
s.sendall(b'port-smoke')
resp = s.recv(4096)
assert resp == b'port-smoke', resp
s.close()
PY
pass "port:// 固定端口转发"

echo "[PASS] manual smoke completed"
