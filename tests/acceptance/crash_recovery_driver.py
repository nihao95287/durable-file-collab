#!/usr/bin/env python3
"""验收场景 5「崩溃恢复」的驱动（P1）。

P4 的传输 worker 还没实现，所以这里由本脚本扮演 worker：入队 -> 等 daemon 分流 ->
接收方上线确认 -> 认领 -> ack 前 40% 的分片；随后由 shell 脚本 kill -9 daemon，
再重启观察恢复。

用法：

    python3 tests/acceptance/crash_recovery_driver.py prepare --db <db> --visibility-timeout-ms 3000
    python3 tests/acceptance/crash_recovery_driver.py status  --db <db> --task-id <id>
    python3 tests/acceptance/crash_recovery_driver.py reclaim --db <db> --worker-id w-after-crash
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.timeutil import now_ms  # noqa: E402
from src.storage.config import QueueConfig  # noqa: E402
from src.storage.db import connect, init_database  # noqa: E402
from src.storage.queue import Queue  # noqa: E402

MB = 1 << 20


def open_queue(db_path: str, visibility_timeout_ms: int) -> tuple:
    path = Path(db_path)
    conn = init_database(path) if not path.exists() else connect(path)
    config = QueueConfig(visibility_timeout_ms=visibility_timeout_ms)
    return conn, Queue(conn, config=config)


def task_state(conn, task_id: str) -> str:
    row = conn.execute(
        "SELECT state FROM delivery_tasks WHERE task_id = ?", (task_id,)
    ).fetchone()
    return row["state"] if row else "(任务不存在)"


def wait_for_state(conn, task_id: str, states: set, timeout: float) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = task_state(conn, task_id)
        if state in states:
            return state
        time.sleep(0.2)
    raise SystemExit(f"等待状态 {sorted(states)} 超时，当前状态 {task_state(conn, task_id)}")


def cmd_prepare(args: argparse.Namespace) -> int:
    conn, queue = open_queue(args.db, args.visibility_timeout_ms)
    task_id = queue.enqueue_task(
        "delivery",
        {
            "sender_id": "u-a",
            "receiver_id": "u-b",
            "file_name": "report.bin",
            "source_path": "data/tmp/source.bin",
            "size_bytes": args.size_mb * MB,
            "checksum": "abc123",
        },
    )
    print(f"[prepare] 入队 task_id={task_id} state={task_state(conn, task_id)}")
    state = wait_for_state(conn, task_id, {"waiting_online", "waiting_confirm"}, args.wait_seconds)
    print(f"[prepare] daemon 已分流 -> {state}")

    queue.wake_user("u-b")  # 模拟 P2 的心跳：接收方上线
    queue.transition(task_id, "confirm", ["waiting_confirm"], "P2")
    claim = queue.claim("w-1", now_ms())
    if claim is None:
        raise SystemExit("[prepare] 认领失败：任务不在可认领状态")
    print(
        f"[prepare] 认领成功 worker=w-1 attempt={claim.attempt} "
        f"可见性超时=+{args.visibility_timeout_ms}ms"
    )

    total = int(
        conn.execute(
            "SELECT total_chunks FROM delivery_tasks WHERE task_id = ?", (task_id,)
        ).fetchone()["total_chunks"]
    )
    progress = None
    for chunk_no in range(min(args.ack_chunks, total)):
        progress = queue.ack_chunk(task_id, chunk_no, f"chunk-{chunk_no}")
    print(
        f"[prepare] 已 ack {progress.acked_chunks}/{progress.total_chunks} 片 "
        f"({progress.acked_bytes}/{progress.total_bytes} 字节)"
    )
    print(f"TASK_ID={task_id}")
    conn.close()
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    conn, _ = open_queue(args.db, args.visibility_timeout_ms)
    row = conn.execute(
        "SELECT * FROM delivery_tasks WHERE task_id = ?", (args.task_id,)
    ).fetchone()
    if row is None:
        print("任务不存在")
        return 1
    claims = [
        dict(claim)
        for claim in conn.execute(
            "SELECT worker_id, attempt, visibility_deadline FROM queue_claims WHERE task_id = ?",
            (args.task_id,),
        )
    ]
    events = [
        f"{event['from_state']}--{event['event']}-->{event['to_state']}({event['actor']})"
        for event in conn.execute(
            "SELECT event, from_state, to_state, actor FROM task_events "
            "WHERE task_id = ? ORDER BY event_id",
            (args.task_id,),
        )
    ]
    snapshot = {
        "task_id": row["task_id"],
        "state": row["state"],
        "acked_chunks": row["acked_chunks"],
        "total_chunks": row["total_chunks"],
        "acked_bytes": row["acked_bytes"],
        "size_bytes": row["size_bytes"],
        "retry_count": row["retry_count"],
        "claims": claims,
        "events": events,
    }
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))
    print(f"STATE={row['state']} ACKED_CHUNKS={row['acked_chunks']}")
    conn.close()
    return 0


def cmd_reclaim(args: argparse.Namespace) -> int:
    conn, queue = open_queue(args.db, args.visibility_timeout_ms)
    claim = queue.claim(args.worker_id, now_ms())
    if claim is None:
        print("[reclaim] 没有可认领的任务（可能可见性超时还没到）")
        conn.close()
        return 1
    print(f"[reclaim] 重新认领成功 task_id={claim.task_id} attempt={claim.attempt}")
    print(f"RECLAIMED={claim.task_id}")
    conn.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="验收场景 5 驱动（P1）")
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--db", required=True)
        p.add_argument("--visibility-timeout-ms", type=int, default=3_000)

    prepare = sub.add_parser("prepare", help="入队并模拟传输到 40%")
    common(prepare)
    prepare.add_argument("--size-mb", type=int, default=5)
    prepare.add_argument("--ack-chunks", type=int, default=2)
    prepare.add_argument("--wait-seconds", type=float, default=15.0)
    prepare.set_defaults(func=cmd_prepare)

    status = sub.add_parser("status", help="打印库内快照（任务/进度/claim/事件）")
    common(status)
    status.add_argument("--task-id", required=True)
    status.set_defaults(func=cmd_status)

    reclaim = sub.add_parser("reclaim", help="恢复后重新认领")
    common(reclaim)
    reclaim.add_argument("--worker-id", default="w-after-crash")
    reclaim.set_defaults(func=cmd_reclaim)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
