#!/usr/bin/env bash
# 验收场景 5「崩溃恢复」一键复现（P1 维护）
#
# 步骤：启动 daemon -> 入队并模拟传输到 40% -> kill -9 daemon -> 检查任务与进度仍在
#       -> 重启 daemon，等可见性超时被 recover 回收 -> 检查可被重新认领且进度不丢
#
# 用法：tests/acceptance/run_crash_recovery.sh
#       WORK_DIR=/tmp/x VISIBILITY_MS=5000 tests/acceptance/run_crash_recovery.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK_DIR="${WORK_DIR:-$(mktemp -d /tmp/crash-recovery.XXXXXX)}"
DB="${DB:-$WORK_DIR/data/db/queue.db}"
PIDFILE="$WORK_DIR/data/logs/queue-daemon.pid"
LOG="$WORK_DIR/daemon.log"
VISIBILITY_MS="${VISIBILITY_MS:-3000}"
DRIVER=(python3 "$ROOT_DIR/tests/acceptance/crash_recovery_driver.py")

mkdir -p "$WORK_DIR" "$(dirname "$PIDFILE")"

fail() { echo "[FAIL] $*" >&2; exit 1; }
pass() { echo "[PASS] $*"; }

start_daemon() {
  ( cd "$ROOT_DIR" && python3 -m src.daemon.main --init-db --db "$DB" --pidfile "$PIDFILE" \
      --interval-ms 300 --recover-every-ms 500 --visibility-timeout-ms "$VISIBILITY_MS" \
      --log-level INFO >>"$LOG" 2>&1 & )
  for _ in $(seq 1 40); do
    [ -f "$PIDFILE" ] && return 0
    sleep 0.25
  done
  fail "daemon 启动超时，日志见 $LOG"
}

echo "== 工作目录: $WORK_DIR（保留下来便于查看证据）"
echo "== 1) 启动 queue-daemon（可见性超时 ${VISIBILITY_MS}ms）"
start_daemon
echo "   daemon pid=$(cat "$PIDFILE")"

echo "== 2) 入队任务，并由 daemon 分流；接收方上线确认；worker 认领并 ack 40%"
TASK_ID="$("${DRIVER[@]}" prepare --db "$DB" --visibility-timeout-ms "$VISIBILITY_MS" \
  --ack-chunks 2 | tee /dev/stderr | sed -n 's/^TASK_ID=//p')"
[ -n "$TASK_ID" ] || fail "拿不到 task_id"

echo "== 3) kill -9 queue-daemon（模拟崩溃，不是优雅停止）"
PIDFILE="$PIDFILE" "$ROOT_DIR/scripts/kill_daemon.sh"
sleep 0.5

echo "== 4) 崩溃后检查：任务与已 ack 进度必须仍在库中"
BEFORE="$("${DRIVER[@]}" status --db "$DB" --task-id "$TASK_ID")"
echo "$BEFORE"
echo "$BEFORE" | grep -q '"state": "transferring"' || fail "崩溃后任务状态丢失"
echo "$BEFORE" | grep -q '"acked_chunks": 2' || fail "崩溃后已 ack 的进度丢失"
pass "崩溃后任务仍在 transferring，已 ack 的 2/5 分片未丢"

echo "== 5) 重启 daemon，等可见性超时被 recover 回收"
start_daemon
sleep $(( VISIBILITY_MS / 1000 + 2 ))
grep -q "启动恢复完成" "$LOG" && echo "   daemon 启动时执行了恢复扫描"

echo "== 6) 恢复后检查：应回到 network_wait，且可被重新认领"
AFTER="$("${DRIVER[@]}" status --db "$DB" --task-id "$TASK_ID")"
echo "$AFTER"
echo "$AFTER" | grep -q '"state": "network_wait"' || fail "可见性超时后未回到 network_wait"
echo "$AFTER" | grep -q '"acked_chunks": 2' || fail "恢复后已 ack 的进度丢失"
RECLAIMED="$("${DRIVER[@]}" reclaim --db "$DB" --visibility-timeout-ms "$VISIBILITY_MS" \
  --worker-id w-after-crash | tee /dev/stderr | sed -n 's/^RECLAIMED=//p')"
[ "$RECLAIMED" = "$TASK_ID" ] || fail "重新认领失败"
pass "崩溃恢复完成：同一任务被重新认领，已 ack 的 2/5 分片保留"

echo "== 7) 收尾：停止 daemon"
PIDFILE="$PIDFILE" "$ROOT_DIR/scripts/kill_daemon.sh" || true
echo "== 场景 5 全部通过"
