"""QueueAPI 的唯一实现（P1 拥有，P2/P3/P4 调用）。

签名与语义严格等于 docs/Interfaces.md §2。调用方只传 kind 与 payload，
本模块不解释业务字段含义（只把 sender_id / receiver_id / size_bytes 三个路由键冗余成列，
用于调度与只读视图，已写入契约 §2.1）。

实现边界：只碰 delivery_tasks / task_chunks / queue_claims / task_events 四张表。
"""

from __future__ import annotations

import json
import math
import sqlite3
from typing import Any, Callable, Mapping

from src.common.audit import AuditSink
from src.common.errors import (
    ChunkChecksumMismatch,
    InvalidArgument,
    RetryExhausted,
    TaskNotFound,
)
from src.common.timeutil import now_ms
from src.common.types import Claim, Progress, RecoverReport

from .config import QueueConfig
from .db import auto_tx
from .events import record_event
from .hooks import HookRegistry
from .ids import new_id
from .statemachine import transition as apply_transition

#: 任务类型白名单（docs/Interfaces.md §2.7）
TASK_KINDS = frozenset({"delivery", "version_upload"})

#: claim() 可认领的状态（契约 §2.2 已按修订去掉 queued：queued 必须先经过 §1.2 的分流流转）
CLAIMABLE_STATES: tuple[str, ...] = ("transferring", "network_wait")

#: 需要接收方确认的任务类型；其余类型由 daemon 用 dispatch_immediate 直接进入传输
CONFIRM_REQUIRED_KINDS: frozenset[str] = frozenset({"delivery"})

#: query_view 支持的只读视图名（供 P2 管理端使用）
VIEW_NAMES = frozenset({"tasks", "queue", "retry_stats", "claims"})

_TASK_FILTERS = frozenset({"state", "kind", "sender_id", "receiver_id", "retry_min", "limit", "cursor"})

_TASK_COLUMNS = (
    "task_id, kind, state, sender_id, receiver_id, size_bytes, acked_bytes, "
    "total_chunks, acked_chunks, retry_count, next_run_at, last_error, created_at, updated_at"
)


class Queue:
    """持久化队列内核。一个实例绑定一个连接；多进程各持一个连接即可。"""

    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        config: QueueConfig | None = None,
        hooks: HookRegistry | None = None,
        clock: Callable[[], int] | None = None,
        audit: AuditSink | None = None,
    ) -> None:
        self.conn = conn
        self.config = config or QueueConfig()
        self.hooks = hooks if hooks is not None else HookRegistry()
        self._clock = clock or now_ms
        #: P4 的审计模块（AuditAPI）。未交付时可以是 None 或 NullAuditSink。
        self.audit = audit

    # ------------------------------------------------------------- 注入接口
    def set_online_probe(self, probe: Callable[[str], bool] | None) -> None:
        """P2 在启动时注入「接收方是否在线」的查询（users 表归 P2，P1 不跨模块读表）。"""

        self.config.receiver_online_probe = probe

    # ------------------------------------------------------------- 内部转发
    def _apply(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        event: str,
        expect_states: list[str],
        actor: str,
        **kwargs: Any,
    ) -> str:
        """带审计的流转：业务状态与审计记录落在同一个事务里。"""

        return apply_transition(
            conn, task_id, event, expect_states, actor, audit=self.audit, **kwargs
        )

    def _record(self, conn: sqlite3.Connection, **kwargs: Any) -> int:
        return record_event(conn, audit=self.audit, **kwargs)

    # ------------------------------------------------------------- 2.1 入队
    def enqueue_task(
        self,
        kind: str,
        payload: dict[str, Any],
        run_after: int | None = None,
    ) -> str:
        """单事务写入任务主表 + 分片表 + 初始事件；提交成功后才返回 task_id。"""

        if kind not in TASK_KINDS:
            raise InvalidArgument(
                f"未知任务类型: {kind!r}",
                detail={"kind": kind, "allowed": sorted(TASK_KINDS)},
            )
        if not isinstance(payload, Mapping):
            raise InvalidArgument("payload 必须是 dict（内部按 JSON 字符串存储）")
        if run_after is not None and not isinstance(run_after, int):
            raise InvalidArgument("run_after 必须是 UTC 毫秒整数")

        now = self._clock()
        payload = dict(payload)
        task_id = new_id("task")
        size_bytes = int(payload.get("size_bytes") or 0)
        chunks = self._plan_chunks(size_bytes)

        with auto_tx(self.conn) as conn:
            conn.execute(
                """
                INSERT INTO delivery_tasks (
                    task_id, kind, payload, state, sender_id, receiver_id, size_bytes,
                    acked_bytes, total_chunks, acked_chunks, retry_count, max_retries,
                    next_run_at, created_at, updated_at
                ) VALUES (?, ?, ?, 'submitted', ?, ?, ?, 0, ?, 0, 0, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    kind,
                    json.dumps(payload, ensure_ascii=False),
                    payload.get("sender_id"),
                    payload.get("receiver_id"),
                    size_bytes,
                    len(chunks),
                    self.config.retry.max_retries,
                    run_after if run_after is not None else now,
                    now,
                    now,
                ),
            )
            conn.executemany(
                """
                INSERT INTO task_chunks (task_id, chunk_no, offset_bytes, length_bytes)
                VALUES (?, ?, ?, ?)
                """,
                [(task_id, no, offset, length) for no, offset, length in chunks],
            )
            self._record(
                conn,
                task_id=task_id,
                event="task.enqueued",
                actor="P1",
                to_state="submitted",
                detail={"kind": kind, "chunks": len(chunks), "size_bytes": size_bytes},
                now=now,
            )
            self._apply(conn, task_id, "enqueued", ["submitted"], "P1", now=now)
        return task_id

    # ------------------------------------------------------------- 2.2 认领
    def claim(self, worker_id: str, now: int) -> Claim | None:
        """认领一个可执行任务；返回 None 表示当前无可执行任务。"""

        if not worker_id:
            raise InvalidArgument("worker_id 不能为空")
        placeholders = ", ".join("?" for _ in CLAIMABLE_STATES)
        with auto_tx(self.conn) as conn:
            row = conn.execute(
                f"""
                SELECT t.task_id, t.kind, t.payload, t.state
                  FROM delivery_tasks t
                 WHERE t.state IN ({placeholders})
                   AND t.next_run_at <= ?
                   AND t.retry_count <= t.max_retries
                   AND NOT EXISTS (
                       SELECT 1 FROM queue_claims c
                        WHERE c.task_id = t.task_id AND c.visibility_deadline > ?
                   )
                 ORDER BY t.next_run_at ASC, t.created_at ASC
                 LIMIT 1
                """,
                (*CLAIMABLE_STATES, now, now),
            ).fetchone()
            if row is None:
                return None

            if row["state"] == "network_wait":
                # network_wait --recovered--> transferring（契约 §1.2，由 P1 调度触发）
                self._apply(
                    conn,
                    row["task_id"],
                    "recovered",
                    ["network_wait"],
                    "P1",
                    detail={"reason": "claim"},
                    now=now,
                )

            deadline = now + self.config.visibility_timeout_ms
            conn.execute(
                """
                INSERT INTO queue_claims (task_id, worker_id, attempt, claimed_at, visibility_deadline)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    worker_id = excluded.worker_id,
                    attempt = queue_claims.attempt + 1,
                    claimed_at = excluded.claimed_at,
                    visibility_deadline = excluded.visibility_deadline
                """,
                (row["task_id"], worker_id, now, deadline),
            )
            attempt = conn.execute(
                "SELECT attempt FROM queue_claims WHERE task_id = ?", (row["task_id"],)
            ).fetchone()["attempt"]

        return Claim(
            task_id=row["task_id"],
            kind=row["kind"],
            payload=row["payload"],
            state="transferring",
            visibility_deadline=deadline,
            attempt=int(attempt),
        )

    # --------------------------------------------------------- 2.3 分片确认 / 否定
    def ack_chunk(self, task_id: str, chunk_no: int, checksum: str) -> Progress:
        """校验一致才推进进度；重复 ack 幂等返回，不重复累加。"""

        if not isinstance(chunk_no, int) or chunk_no < 0:
            raise InvalidArgument("chunk_no 必须是非负整数")
        with auto_tx(self.conn) as conn:
            task = conn.execute(
                "SELECT task_id, state FROM delivery_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise TaskNotFound(f"任务不存在: {task_id}", detail={"task_id": task_id})

            chunk = conn.execute(
                "SELECT * FROM task_chunks WHERE task_id = ? AND chunk_no = ?",
                (task_id, chunk_no),
            ).fetchone()
            if chunk is None:
                raise InvalidArgument(
                    f"分片不存在: {task_id}#{chunk_no}",
                    detail={"task_id": task_id, "chunk_no": chunk_no},
                )

            if chunk["state"] == "acked":
                if checksum and chunk["checksum"] and chunk["checksum"] != checksum:
                    raise ChunkChecksumMismatch(
                        "重复 ack 的分片校验值与首次不一致，进度不推进",
                        detail={"task_id": task_id, "chunk_no": chunk_no},
                    )
                return self._progress(conn, task_id)  # 幂等

            now = self._clock()
            conn.execute(
                """
                UPDATE task_chunks
                   SET state = 'acked', checksum = ?, acked_at = ?,
                       attempts = attempts + 1, last_error = NULL
                 WHERE task_id = ? AND chunk_no = ?
                """,
                (checksum, now, task_id, chunk_no),
            )
            self._refresh_progress(conn, task_id, now)
            self._record(
                conn,
                task_id=task_id,
                event="chunk.acked",
                actor="P4",
                to_state=task["state"],
                detail={"chunk_no": chunk_no, "length_bytes": chunk["length_bytes"]},
                now=now,
            )
            return self._progress(conn, task_id)

    def nack(self, task_id: str, chunk_no: int, reason: str, retryable: bool) -> None:
        """记录一次失败：retry_count+1、算 next_run_at；超限则置 failed 并抛 RetryExhausted。"""

        exhausted_error: RetryExhausted | None = None
        with auto_tx(self.conn) as conn:
            task = conn.execute(
                """
                SELECT task_id, state, retry_count, max_retries
                  FROM delivery_tasks WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
            if task is None:
                raise TaskNotFound(f"任务不存在: {task_id}", detail={"task_id": task_id})

            now = self._clock()
            attempt = int(task["retry_count"]) + 1
            exhausted = (not retryable) or attempt > int(task["max_retries"])
            next_run_at = now if exhausted else now + self.config.retry.backoff_ms(attempt)

            if chunk_no is not None:
                conn.execute(
                    """
                    UPDATE task_chunks
                       SET attempts = attempts + 1, last_error = ?
                     WHERE task_id = ? AND chunk_no = ?
                    """,
                    (reason, task_id, chunk_no),
                )
            conn.execute(
                """
                UPDATE delivery_tasks
                   SET retry_count = ?, last_error = ?, next_run_at = ?, updated_at = ?
                 WHERE task_id = ?
                """,
                (attempt, reason, next_run_at, now, task_id),
            )

            detail = {"chunk_no": chunk_no, "reason": reason, "retry_count": attempt}
            if exhausted:
                if task["state"] in ("transferring", "network_wait"):
                    self._apply(
                        conn,
                        task_id,
                        "retry_exhausted",
                        ["transferring", "network_wait"],
                        "P1",
                        detail=detail,
                        now=now,
                    )
                self._record(
                    conn,
                    task_id=task_id,
                    event="task.retry_exhausted",
                    actor="P1",
                    to_state="failed",
                    detail=detail,
                    now=now,
                )
                exhausted_error = RetryExhausted(
                    f"重试次数超限，任务已置为 failed: {task_id}",
                    detail={"task_id": task_id, "retry_count": attempt},
                )
            elif task["state"] == "transferring":
                # transferring --interrupted--> network_wait，backoff 到期后可被重新认领
                self._apply(
                    conn, task_id, "interrupted", ["transferring"], "P1", detail=detail, now=now
                )

        if exhausted_error is not None:
            # 事务已提交后再抛，否则会回滚掉刚写入的 failed 状态
            raise exhausted_error

    # ------------------------------------------------------------- 2.4 状态流转
    def transition(
        self,
        task_id: str,
        event: str,
        expect_states: list[str],
        actor: str,
    ) -> str:
        """唯一的状态变更入口；非法流转抛 IllegalTransition（调用方不得自行判断）。"""

        with auto_tx(self.conn) as conn:
            return self._apply(conn, task_id, event, expect_states, actor, now=self._clock())

    # --------------------------------------------------------- 2.5 心跳与唤醒
    def heartbeat_claim(self, task_id: str, worker_id: str) -> bool:
        """续期 visibility_deadline；仅持有者可续，否则返回 False。"""

        if not worker_id:
            raise InvalidArgument("worker_id 不能为空")
        now = self._clock()
        with auto_tx(self.conn) as conn:
            cursor = conn.execute(
                """
                UPDATE queue_claims
                   SET visibility_deadline = ?
                 WHERE task_id = ? AND worker_id = ?
                """,
                (now + self.config.visibility_timeout_ms, task_id, worker_id),
            )
            return cursor.rowcount == 1

    def wake_user(self, user_id: str) -> int:
        """接收方由离线转在线：waiting_online -> waiting_confirm，返回受影响任务数。"""

        if not user_id:
            raise InvalidArgument("user_id 不能为空")
        now = self._clock()
        woken = 0
        with auto_tx(self.conn) as conn:
            rows = conn.execute(
                """
                SELECT task_id FROM delivery_tasks
                 WHERE state = 'waiting_online' AND receiver_id = ?
                 ORDER BY created_at ASC
                """,
                (user_id,),
            ).fetchall()
            for row in rows:
                self._apply(
                    conn, row["task_id"], "receiver_online", ["waiting_online"], "P2", now=now
                )
                woken += 1
        return woken

    def set_delay(self, task_id: str, delay_until: int, actor: str = "P2") -> str:
        """P2 的「延迟接收」路径：waiting_confirm --delay--> delayed，并写入到期时间。

        transition() 只管状态、不写 next_run_at，所以这里补一个 P1 侧的组合入口；
        见 docs/契约待澄清项.md 第 6 条。
        """

        if not isinstance(delay_until, int):
            raise InvalidArgument("delay_until 必须是 UTC 毫秒整数")
        now = self._clock()
        with auto_tx(self.conn) as conn:
            state = self._apply(
                conn,
                task_id,
                "delay",
                ["waiting_confirm"],
                actor,
                detail={"delay_until": delay_until},
                now=now,
            )
            conn.execute(
                "UPDATE delivery_tasks SET next_run_at = ? WHERE task_id = ?",
                (delay_until, task_id),
            )
        return state

    # ------------------------------------------------------- 2.6 恢复与定时钩子
    def recover(self, now: int) -> RecoverReport:
        """清理失效 claim 与到期延迟任务；可重复执行（第二次应为全 0）。"""

        claims_expired = requeued = delay_expired = 0
        with auto_tx(self.conn) as conn:
            stale = conn.execute(
                "SELECT task_id FROM queue_claims WHERE visibility_deadline <= ?", (now,)
            ).fetchall()
            for row in stale:
                conn.execute("DELETE FROM queue_claims WHERE task_id = ?", (row["task_id"],))
                state = conn.execute(
                    "SELECT state FROM delivery_tasks WHERE task_id = ?", (row["task_id"],)
                ).fetchone()
                if state is not None and state["state"] == "transferring":
                    self._apply(
                        conn,
                        row["task_id"],
                        "interrupted",
                        ["transferring"],
                        "P1",
                        detail={"reason": "claim_visibility_timeout"},
                        now=now,
                    )
                claims_expired += 1

            orphans = conn.execute(
                """
                SELECT t.task_id FROM delivery_tasks t
                 WHERE t.state = 'transferring'
                   AND NOT EXISTS (
                       SELECT 1 FROM queue_claims c
                        WHERE c.task_id = t.task_id AND c.visibility_deadline > ?
                   )
                """,
                (now,),
            ).fetchall()
            for row in orphans:
                self._apply(
                    conn,
                    row["task_id"],
                    "interrupted",
                    ["transferring"],
                    "P1",
                    detail={"reason": "orphan_task"},
                    now=now,
                )
                requeued += 1

            delayed = conn.execute(
                "SELECT task_id FROM delivery_tasks WHERE state = 'delayed' AND next_run_at <= ?",
                (now,),
            ).fetchall()
            for row in delayed:
                self._apply(
                    conn,
                    row["task_id"],
                    "delay_expired",
                    ["delayed"],
                    "P1",
                    detail={"reason": "delay_until_reached"},
                    now=now,
                )
                delay_expired += 1

        return RecoverReport(
            requeued=requeued, claims_expired=claims_expired, delay_expired=delay_expired
        )

    def register_periodic_hook(
        self, name: str, interval_ms: int, fn: Callable[..., Any]
    ) -> None:
        """供 P2（延迟到期）与 P3（租约过期回收）挂载定时任务。

        daemon 只认识钩子注册表，不认识业务模块，避免反向依赖。
        """

        self.hooks.register(name, interval_ms, fn)

    # ------------------------------------------------- 2.6b 调度（P1 内部，daemon 调用）
    def dispatch_pending(
        self,
        now: int | None = None,
        receiver_online: Callable[[str], bool] | None = None,
    ) -> int:
        """queued 任务分流（契约 §1.2）：

        * 需要接收方确认的任务（delivery）：按在线状态走 dispatch_online / dispatch_offline；
        * 无需确认的任务（如 version_upload）：走 dispatch_immediate 直接进入 transferring。

        在线状态属于 P2 的 users 表，这里只用探针回调获取（set_online_probe 注入；
        都不提供时视为离线，之后由 P2 的心跳接口调用 wake_user 唤醒）。
        """

        timestamp = self._clock() if now is None else now
        probe = receiver_online or self.config.receiver_online_probe
        dispatched = 0
        with auto_tx(self.conn) as conn:
            rows = conn.execute(
                """
                SELECT task_id, kind, receiver_id FROM delivery_tasks
                 WHERE state = 'queued' AND next_run_at <= ?
                 ORDER BY next_run_at ASC, created_at ASC
                """,
                (timestamp,),
            ).fetchall()
            for row in rows:
                if row["kind"] in CONFIRM_REQUIRED_KINDS:
                    online = (
                        bool(probe(row["receiver_id"])) if (probe and row["receiver_id"]) else False
                    )
                    event = "dispatch_online" if online else "dispatch_offline"
                    detail = {"receiver_online": online}
                else:
                    event = "dispatch_immediate"
                    detail = {"reason": "no_confirmation_required"}
                self._apply(
                    conn,
                    row["task_id"],
                    event,
                    ["queued"],
                    "P1",
                    detail=detail,
                    now=timestamp,
                )
                dispatched += 1
        return dispatched

    # ------------------------------------------------------------- 2.6c 只读视图
    def query_view(
        self, name: str, filters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """只读视图，供 P2 管理端使用；无写入能力。"""

        if name not in VIEW_NAMES:
            raise InvalidArgument(
                f"未定义的只读视图: {name!r}", detail={"allowed": sorted(VIEW_NAMES)}
            )
        filters = dict(filters or {})
        if name == "tasks":
            return self._view_tasks(filters)
        if name == "queue":
            self._reject_unknown_filters(name, filters, frozenset())
            now = self._clock()
            rows = self.conn.execute(
                """
                SELECT t.state AS state,
                       COUNT(*) AS tasks,
                       COALESCE(SUM(t.acked_bytes), 0) AS acked_bytes,
                       COALESCE(SUM(
                           CASE WHEN c.task_id IS NOT NULL AND c.visibility_deadline > ?
                                THEN 1 ELSE 0 END
                       ), 0) AS active_claims,
                       MIN(t.next_run_at) AS earliest_next_run_at
                  FROM delivery_tasks t
                  LEFT JOIN queue_claims c ON c.task_id = t.task_id
                 GROUP BY t.state
                 ORDER BY t.state ASC
                """,
                (now,),
            ).fetchall()
            return [dict(row) for row in rows]
        if name == "retry_stats":
            self._reject_unknown_filters(name, filters, frozenset())
            rows = self.conn.execute(
                """
                SELECT retry_count, COUNT(*) AS tasks
                  FROM delivery_tasks GROUP BY retry_count ORDER BY retry_count ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]

        self._reject_unknown_filters(name, filters, frozenset())
        rows = self.conn.execute(
            """
            SELECT c.task_id, c.worker_id, c.attempt, c.claimed_at, c.visibility_deadline,
                   t.kind, t.state
              FROM queue_claims c JOIN delivery_tasks t ON t.task_id = c.task_id
             WHERE c.visibility_deadline > ?
             ORDER BY c.visibility_deadline ASC
            """,
            (self._clock(),),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---------------------------------------------------------------- 内部工具
    def _plan_chunks(self, size_bytes: int) -> list[tuple[int, int, int]]:
        """分片划分规则属于 P1 的契约：固定大小切分；大小未知时按单分片处理。"""

        chunk_size = max(1, int(self.config.chunk_size_bytes))
        if size_bytes <= 0:
            return [(0, 0, 0)]
        count = math.ceil(size_bytes / chunk_size)
        return [
            (no, no * chunk_size, min(chunk_size, size_bytes - no * chunk_size))
            for no in range(count)
        ]

    def _refresh_progress(self, conn: sqlite3.Connection, task_id: str, now: int) -> None:
        conn.execute(
            """
            UPDATE delivery_tasks
               SET acked_chunks = (
                       SELECT COUNT(*) FROM task_chunks
                        WHERE task_id = ? AND state = 'acked'
                   ),
                   acked_bytes = (
                       SELECT COALESCE(SUM(length_bytes), 0) FROM task_chunks
                        WHERE task_id = ? AND state = 'acked'
                   ),
                   updated_at = ?
             WHERE task_id = ?
            """,
            (task_id, task_id, now, task_id),
        )

    def _progress(self, conn: sqlite3.Connection, task_id: str) -> Progress:
        row = conn.execute(
            """
            SELECT acked_bytes, size_bytes, acked_chunks, total_chunks
              FROM delivery_tasks WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        if row is None:
            raise TaskNotFound(f"任务不存在: {task_id}", detail={"task_id": task_id})
        return Progress(
            acked_bytes=int(row["acked_bytes"]),
            total_bytes=int(row["size_bytes"]),
            acked_chunks=int(row["acked_chunks"]),
            total_chunks=int(row["total_chunks"]),
        )

    def _view_tasks(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        self._reject_unknown_filters("tasks", filters, _TASK_FILTERS)
        where: list[str] = []
        params: list[Any] = []
        for column in ("state", "kind", "sender_id", "receiver_id"):
            if filters.get(column) is not None:
                where.append(f"{column} = ?")
                params.append(filters[column])
        if filters.get("retry_min") is not None:
            where.append("retry_count >= ?")
            params.append(int(filters["retry_min"]))

        sql = f"SELECT {_TASK_COLUMNS} FROM delivery_tasks"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC, task_id DESC LIMIT ? OFFSET ?"
        limit = int(filters.get("limit") or 50)
        params.extend((max(1, min(limit, 500)), max(0, int(filters.get("cursor") or 0))))
        return [dict(row) for row in self.conn.execute(sql, params)]

    @staticmethod
    def _reject_unknown_filters(
        name: str, filters: dict[str, Any], allowed: frozenset[str]
    ) -> None:
        unknown = set(filters) - set(allowed)
        if unknown:
            raise InvalidArgument(
                f"{name} 视图不支持的过滤条件: {sorted(unknown)}",
                detail={"view": name, "allowed": sorted(allowed)},
            )
