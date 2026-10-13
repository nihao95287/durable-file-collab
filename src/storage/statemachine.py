"""投递状态机的唯一权威实现（P1）。

状态与合法流转严格等于 docs/Interfaces.md §1.2；任何模块不得在文档之外新增状态或流转，
也不得提前自行判断合法性——所有状态变更必须经过 transition()。
"""

from __future__ import annotations

import sqlite3
from typing import Any, Iterable, Sequence

from src.common.audit import AuditSink
from src.common.errors import IllegalTransition, TaskNotFound
from src.common.timeutil import now_ms

from .events import record_event

#: 全部状态取值（docs/Interfaces.md §1.1）
STATES: tuple[str, ...] = (
    "submitted",
    "queued",
    "waiting_online",
    "waiting_confirm",
    "delayed",
    "transferring",
    "network_wait",
    "completed",
    "rejected",
    "failed",
)

TERMINAL_STATES = frozenset({"completed", "rejected", "failed"})

#: 合法流转表：(当前状态, 事件) -> 目标状态
TRANSITIONS: dict[tuple[str, str], str] = {
    ("submitted", "enqueued"): "queued",
    ("queued", "dispatch_offline"): "waiting_online",
    ("queued", "dispatch_online"): "waiting_confirm",
    ("queued", "dispatch_immediate"): "transferring",
    ("waiting_online", "receiver_online"): "waiting_confirm",
    ("waiting_confirm", "confirm"): "transferring",
    ("waiting_confirm", "reject"): "rejected",
    ("waiting_confirm", "delay"): "delayed",
    ("delayed", "delay_expired"): "waiting_confirm",
    ("transferring", "transfer_complete"): "completed",
    ("transferring", "interrupted"): "network_wait",
    ("network_wait", "recovered"): "transferring",
    ("transferring", "retry_exhausted"): "failed",
    ("network_wait", "retry_exhausted"): "failed",
}

#: 全部事件取值
EVENTS: frozenset[str] = frozenset(event for _, event in TRANSITIONS)

#: 这些目标状态需要把 next_run_at 重置为当前时间（延迟到期后立即可被调度）
_RESET_NEXT_RUN_ON: frozenset[str] = frozenset({"waiting_confirm"})


def is_terminal(state: str) -> bool:
    return state in TERMINAL_STATES


def next_state(state: str, event: str) -> str:
    """返回流转后的状态；不在流转表中抛 IllegalTransition。"""

    try:
        return TRANSITIONS[(state, event)]
    except KeyError:
        raise IllegalTransition(
            f"非法流转: {state} --{event}--> ?",
            detail={"state": state, "event": event},
        ) from None


def transition(
    conn: sqlite3.Connection,
    task_id: str,
    event: str,
    expect_states: Sequence[str] | Iterable[str],
    actor: str,
    *,
    detail: dict[str, Any] | None = None,
    now: int | None = None,
    reset_next_run_at: bool | None = None,
    audit: AuditSink | None = None,
) -> str:
    """唯一的状态变更入口，必须在调用方事务内执行。

    expect_states 采用乐观并发：当前状态不在其中即抛 IllegalTransition，
    避免「先读状态再写」的竞态。成功后写入一条 task.transition 事件。
    """

    timestamp = now_ms() if now is None else now
    expected = tuple(dict.fromkeys(expect_states))
    row = conn.execute("SELECT state FROM delivery_tasks WHERE task_id = ?", (task_id,)).fetchone()
    if row is None:
        raise TaskNotFound(f"任务不存在: {task_id}", detail={"task_id": task_id})

    current = row["state"]
    if current not in expected:
        raise IllegalTransition(
            f"任务当前状态 {current!r} 不在期望集合 {expected} 内",
            detail={"task_id": task_id, "state": current, "expect_states": list(expected)},
        )

    target = next_state(current, event)
    cursor = conn.execute(
        """
        UPDATE delivery_tasks
           SET state = ?, updated_at = ?
         WHERE task_id = ? AND state = ?
        """,
        (target, timestamp, task_id, current),
    )
    if cursor.rowcount != 1:
        # 乐观并发失败：另一个连接在我们读取之后改了状态
        raise IllegalTransition(
            f"任务状态已被并发修改: {task_id}",
            detail={"task_id": task_id, "expected_state": current},
        )

    if reset_next_run_at is None:
        reset_next_run_at = target in _RESET_NEXT_RUN_ON
    if reset_next_run_at:
        conn.execute(
            "UPDATE delivery_tasks SET next_run_at = ? WHERE task_id = ?", (timestamp, task_id)
        )

    record_event(
        conn,
        task_id=task_id,
        event="task.transition",
        from_state=current,
        to_state=target,
        actor=actor,
        detail={"event": event, **(detail or {})},
        now=timestamp,
        audit=audit,
    )
    return target
