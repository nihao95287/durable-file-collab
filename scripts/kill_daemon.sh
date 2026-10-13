#!/usr/bin/env bash
# 杀掉 queue-daemon，用于验收场景 5（崩溃恢复）。
#
# 用法：
#   scripts/kill_daemon.sh                 # 默认 SIGKILL，模拟进程崩溃
#   SIGNAL=-TERM scripts/kill_daemon.sh    # 优雅停止，对比两种退出路径
#
# 只杀命令行匹配 queue-daemon 的进程，避免误伤。
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="${PIDFILE:-${ROOT_DIR}/data/logs/queue-daemon.pid}"
SIGNAL="${SIGNAL:--KILL}"
PATTERN="${PATTERN:-src.daemon.main}"
killed=0

matches_daemon() {   # $1 = pid
  local pid="$1"
  [ -r "/proc/${pid}/cmdline" ] || return 0   # 非 Linux：拿不到 cmdline 就不做名称校验
  tr '\0' ' ' < "/proc/${pid}/cmdline" | grep -q -- "${PATTERN}"
}

terminate() {        # $1 = pid
  local pid="$1"
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 1
  fi
  if ! matches_daemon "${pid}"; then
    echo "[kill_daemon] pid ${pid} 与 ${PATTERN} 不匹配，跳过" >&2
    return 1
  fi
  kill "${SIGNAL}" "${pid}"
  echo "[kill_daemon] 已向 pid ${pid} 发送 ${SIGNAL}"
  return 0
}

if [ -f "${PIDFILE}" ]; then
  pid="$(cat "${PIDFILE}" 2>/dev/null || true)"
  if [ -n "${pid}" ] && terminate "${pid}"; then
    killed=1
  fi
  rm -f "${PIDFILE}"
fi

if [ "${killed}" -eq 0 ] && command -v pgrep >/dev/null 2>&1; then
  while read -r pid; do
    [ -n "${pid}" ] || continue
    if terminate "${pid}"; then
      killed=1
    fi
  done < <(pgrep -f "${PATTERN}" || true)
fi

if [ "${killed}" -eq 0 ]; then
  echo "[kill_daemon] 没有发现运行中的 queue-daemon" >&2
  exit 1
fi
