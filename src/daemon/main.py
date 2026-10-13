"""queue-daemon 进程（P1）。

职责（docs/Interfaces.md 与团队分工 3.1）：

1. 启动时执行一次 recover()，此后按 recover_every_ms 周期执行（崩溃恢复）；
2. 执行注册进来的定时钩子（P2 的延迟到期检查、P3 的租约过期回收）；
3. 把 queued 任务按接收方在线状态分流到 waiting_online / waiting_confirm；
4. 被 SIGKILL 杀掉后，重启即可从 SQLite 恢复未完成任务（验收场景 5）。

用法：

    python3 -m src.daemon.main --db data/db/queue.db --interval-ms 500
    python3 -m src.daemon.main --once          # 只跑一个调度周期，供脚本/测试使用
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable

from src.common.timeutil import now_ms
from src.storage.db import DEFAULT_DB_PATH, ROOT_DIR, ensure_data_dirs, init_database
from src.storage.queue import Queue

logger = logging.getLogger("queue.daemon")

DEFAULT_PIDFILE = ROOT_DIR / "data" / "logs" / "queue-daemon.pid"
DEFAULT_HOOK_INTERVAL_MS = 1_000


class Daemon:
    """单线程调度循环。一个 tick 内做：恢复扫描 -> 定时钩子 -> 任务分流。"""

    def __init__(
        self,
        queue: Queue,
        *,
        interval_ms: int = 500,
        recover_every_ms: int = 5_000,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if interval_ms <= 0:
            raise ValueError("interval_ms 必须为正")
        self.queue = queue
        self.interval_ms = interval_ms
        self.recover_every_ms = recover_every_ms
        self._clock = clock or now_ms
        self._last_recover_at: int | None = None
        self._stopping = False

    # ------------------------------------------------------------------ 生命周期
    def bootstrap(self) -> dict[str, Any]:
        """启动时恢复一次：杀掉 daemon 后重启，未完成任务和进度仍然可恢复。"""

        now = self._clock()
        report = self.queue.recover(now)
        self._last_recover_at = now
        logger.info("启动恢复完成: %s", report.to_dict())
        return report.to_dict()

    def request_stop(self) -> None:
        """SIGTERM / SIGINT：处理完当前 tick 后退出（SIGKILL 走崩溃恢复路径）。"""

        self._stopping = True

    # ------------------------------------------------------------------ 调度循环
    def tick(self, now: int | None = None) -> dict[str, Any]:
        """执行一个调度周期，返回本次摘要（便于测试与演示观察）。"""

        timestamp = self._clock() if now is None else now
        summary: dict[str, Any] = {"recover": None, "hooks": [], "dispatched": 0}
        if self._recover_due(timestamp):
            summary["recover"] = self.queue.recover(timestamp).to_dict()
            self._last_recover_at = timestamp
        summary["hooks"] = self.queue.hooks.run_due(timestamp)
        summary["dispatched"] = self.queue.dispatch_pending(timestamp)
        return summary

    def run_forever(self) -> int:
        self.bootstrap()
        logger.info("queue-daemon 启动: interval=%dms", self.interval_ms)
        while not self._stopping:
            started = time.monotonic()
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - 单次失败不能让守护进程退出
                logger.exception("调度周期失败，继续运行")
            elapsed_ms = int((time.monotonic() - started) * 1000)
            time.sleep(max(0.0, (self.interval_ms - elapsed_ms) / 1000))
        logger.info("queue-daemon 正常退出")
        return 0

    def _recover_due(self, now: int) -> bool:
        if self.recover_every_ms <= 0:
            return False
        if self._last_recover_at is None:
            return True
        return now - self._last_recover_at >= self.recover_every_ms


def write_pidfile(pidfile: Path) -> None:
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    pidfile.write_text(f"{os.getpid()}\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="queue-daemon", description="持久化队列守护进程（P1）")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite 数据库路径")
    parser.add_argument("--interval-ms", type=int, default=500, help="调度周期")
    parser.add_argument("--recover-every-ms", type=int, default=5_000, help="恢复扫描周期")
    parser.add_argument("--pidfile", default=str(DEFAULT_PIDFILE), help="pid 文件路径")
    parser.add_argument("--once", action="store_true", help="只执行一个调度周期后退出")
    parser.add_argument("--init-db", action="store_true", help="先建库（幂等）再启动")
    parser.add_argument("--log-level", default="INFO")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    ensure_data_dirs(ROOT_DIR)

    conn: sqlite3.Connection = (
        init_database(args.db) if args.init_db else _open_existing(args.db)
    )
    daemon = Daemon(
        Queue(conn),
        interval_ms=args.interval_ms,
        recover_every_ms=args.recover_every_ms,
    )

    if args.once:
        daemon.bootstrap()
        print(daemon.tick())
        conn.close()
        return 0

    def _handle_signal(signum: int, _frame: Any) -> None:
        logger.info("收到信号 %s，准备退出", signum)
        daemon.request_stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    pidfile = Path(args.pidfile)
    write_pidfile(pidfile)
    try:
        return daemon.run_forever()
    finally:
        pidfile.unlink(missing_ok=True)
        conn.close()


def _open_existing(db_path: str) -> sqlite3.Connection:
    """打开已有库；若还没建库就报错（提示先跑 --init-db 或 init_database()）。"""

    from src.storage.db import connect

    if not Path(db_path).exists():
        raise SystemExit(f"数据库不存在: {db_path}（先执行 --init-db 或 init_database()）")
    return connect(db_path)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
