"""状态变更事件（P1）：状态机流转的落库证据，也是崩溃恢复与审计的输入。"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from src.common.audit import AuditSink, audit_writer
from src.common.timeutil import now_ms

#: P1 独占的 event_type 取值（docs/Interfaces.md §3）。
#: 契约测试 C-09 会校验：写入 task_events 的 event 必须在此集合内。
EVENT_TYPES = frozenset(
    {
        "task.enqueued",
        "task.transition",
        "chunk.acked",
        "task.retry_exhausted",
    }
)


def record_event(
    conn: sqlite3.Connection,
    *,
    task_id: str,
    event: str,
    actor: str,
    from_state: str | None = None,
    to_state: str | None = None,
    detail: dict[str, Any] | None = None,
    now: int | None = None,
    audit: AuditSink | None = None,
) -> int:
    """追加一条事件；必须在调用方事务内执行（本函数不自己开事务）。

    传入 audit（P4 的审计模块）时，同一条事件会通过 audit.join_tx(conn) 写进
    调用方的事务：业务成功而审计丢失（或反之）都不可能出现（docs/契约待澄清项.md 第 7 条）。
    """

    if event not in EVENT_TYPES:
        raise ValueError(f"未定义的 event 取值: {event!r}（见 src/storage/events.py）")
    timestamp = now_ms() if now is None else now
    cursor = conn.execute(
        """
        INSERT INTO task_events (task_id, event, from_state, to_state, actor, detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            event,
            from_state,
            to_state,
            actor,
            json.dumps(detail or {}, ensure_ascii=False),
            timestamp,
        ),
    )
    writer = audit_writer(audit, conn)
    if writer is not None:
        writer.write(
            event,
            actor,
            "task",
            task_id,
            {"from_state": from_state, "to_state": to_state, **(detail or {})},
        )
    return int(cursor.lastrowid)


def list_events(conn: sqlite3.Connection, task_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM task_events WHERE task_id = ? ORDER BY event_id ASC", (task_id,)
    ).fetchall()
    return [dict(row) for row in rows]
