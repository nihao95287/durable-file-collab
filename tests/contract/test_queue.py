"""契约测试（P1 维护）：C-01 ~ C-05、C-09、C-10 以及投递主流程。

运行方式（无第三方依赖，pytest 也能直接收集）：

    python3 -m unittest discover -s tests/contract -v

每个用例都在临时目录里建一个全新的库，互不影响。
"""

from __future__ import annotations

import inspect
import json
import multiprocessing
import re
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.errors import (  # noqa: E402
    ChunkChecksumMismatch,
    IllegalTransition,
    InvalidArgument,
    RetryExhausted,
    TaskNotFound,
)
from src.storage import statemachine  # noqa: E402
from src.storage.config import QueueConfig, RetryPolicy  # noqa: E402
from src.storage.db import (  # noqa: E402
    apply_migrations,
    connect,
    ensure_data_dirs,
    init_database,
    retry_on_busy,
)
from src.storage.events import EVENT_TYPES, list_events  # noqa: E402
from src.storage.hooks import HookRegistry  # noqa: E402
from src.storage.queue import TASK_KINDS, VIEW_NAMES, Queue  # noqa: E402

MB = 1 << 20


class FakeClock:
    """可控时钟：让可见性超时、退避这些时间相关行为可以被确定性地测试。"""

    def __init__(self, start: int = 1_700_000_000_000) -> None:
        self.value = start

    def __call__(self) -> int:
        return self.value

    def advance(self, ms: int) -> int:
        self.value += ms
        return self.value


def delivery_payload(size_bytes: int = 2 * MB, receiver: str = "u-b") -> dict:
    """P2 提交的 delivery 任务负载（docs/Interfaces.md §2.7）。"""

    return {
        "sender_id": "u-a",
        "receiver_id": receiver,
        "file_name": "report.bin",
        "source_path": "data/tmp/source.bin",
        "size_bytes": size_bytes,
        "checksum": "deadbeef",
    }


class QueueContractCase(unittest.TestCase):
    """公共脚手架：一个临时库 + 一个假时钟 + 一个 Queue 实例。"""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self._tmpdir.name)
        self.clock = FakeClock()
        self.db_path = self.root / "queue.db"
        self.conn = init_database(self.db_path)
        self.queue = Queue(self.conn, clock=self.clock)

    def tearDown(self) -> None:
        self.conn.close()
        self._tmpdir.cleanup()

    # ------------------------------------------------------------- 测试用工具
    def enqueue_delivery(self, size_bytes: int = 2 * MB, receiver: str = "u-b") -> str:
        return self.queue.enqueue_task("delivery", delivery_payload(size_bytes, receiver))

    def state_of(self, task_id: str) -> str:
        row = self.conn.execute(
            "SELECT state FROM delivery_tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        self.assertIsNotNone(row, f"任务不存在: {task_id}")
        return row["state"]

    def move_to_transferring(self, task_id: str, receiver: str = "u-b") -> str:
        """queued -> waiting_online -> waiting_confirm -> transferring（离线投递主路径）。"""

        self.queue.dispatch_pending(self.clock())
        self.queue.wake_user(receiver)
        return self.queue.transition(task_id, "confirm", ["waiting_confirm"], "P2")

    def chunk_count(self, task_id: str) -> int:
        return int(
            self.conn.execute(
                "SELECT COUNT(*) AS n FROM task_chunks WHERE task_id = ?", (task_id,)
            ).fetchone()["n"]
        )


class TestEnqueue(QueueContractCase):
    def test_enqueue_writes_task_chunks_and_events_in_one_transaction(self) -> None:
        task_id = self.enqueue_delivery(size_bytes=2 * MB + 1)

        self.assertEqual(self.state_of(task_id), "queued")
        self.assertEqual(self.chunk_count(task_id), 3)  # ceil(2MiB+1 / 1MiB)
        events = list_events(self.conn, task_id)
        self.assertEqual([e["event"] for e in events], ["task.enqueued", "task.transition"])
        self.assertEqual(events[1]["from_state"], "submitted")
        self.assertEqual(events[1]["to_state"], "queued")

    def test_enqueue_keeps_payload_verbatim(self) -> None:
        payload = delivery_payload()
        payload["extra"] = {"自定义": "字段"}
        task_id = self.queue.enqueue_task("delivery", payload)
        stored = json.loads(
            self.conn.execute(
                "SELECT payload FROM delivery_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()["payload"]
        )
        self.assertEqual(stored, payload)

    def test_enqueue_rejects_unknown_kind_and_bad_payload(self) -> None:
        with self.assertRaises(InvalidArgument):
            self.queue.enqueue_task("not-a-kind", {})
        with self.assertRaises(InvalidArgument):
            self.queue.enqueue_task("delivery", "not-a-dict")  # type: ignore[arg-type]
        with self.assertRaises(InvalidArgument):
            self.queue.enqueue_task("delivery", {}, run_after="soon")  # type: ignore[arg-type]


class TestSignatures(QueueContractCase):
    """C-01：QueueAPI 全部函数签名、参数顺序与返回结构符合契约第 2 节。"""

    EXPECTED = {
        "enqueue_task": ["kind", "payload", "run_after"],
        "claim": ["worker_id", "now"],
        "ack_chunk": ["task_id", "chunk_no", "checksum"],
        "nack": ["task_id", "chunk_no", "reason", "retryable"],
        "transition": ["task_id", "event", "expect_states", "actor"],
        "heartbeat_claim": ["task_id", "worker_id"],
        "wake_user": ["user_id"],
        "register_periodic_hook": ["name", "interval_ms", "fn"],
        "recover": ["now"],
        "query_view": ["name", "filters"],
    }

    def test_c01_signatures(self) -> None:
        for name, params in self.EXPECTED.items():
            signature = inspect.signature(getattr(Queue, name))
            actual = [
                p.name for p in signature.parameters.values() if p.name != "self"
            ]
            self.assertEqual(actual, params, f"{name} 的参数顺序与契约不符")

    def test_c01_optional_defaults(self) -> None:
        enqueue = inspect.signature(Queue.enqueue_task).parameters
        self.assertIsNone(enqueue["run_after"].default)
        self.assertIsNone(inspect.signature(Queue.query_view).parameters["filters"].default)


class TestStateMachine(QueueContractCase):
    """C-02：非法流转抛 IllegalTransition；状态机表与契约一致。"""

    def test_c02_illegal_transition_raises_and_keeps_state(self) -> None:
        task_id = self.enqueue_delivery()
        with self.assertRaises(IllegalTransition):
            self.queue.transition(task_id, "confirm", ["waiting_confirm"], "P2")
        self.assertEqual(self.state_of(task_id), "queued")

    def test_c02_unknown_event_raises(self) -> None:
        task_id = self.enqueue_delivery()
        with self.assertRaises(IllegalTransition):
            self.queue.transition(task_id, "explode", ["queued"], "P2")

    def test_c02_missing_task_raises_task_not_found(self) -> None:
        with self.assertRaises(TaskNotFound):
            self.queue.transition("t-missing", "enqueued", ["submitted"], "P1")

    def test_c02_terminal_states_are_not_reclaimable(self) -> None:
        self.assertTrue(statemachine.is_terminal("completed"))
        self.assertTrue(statemachine.is_terminal("rejected"))
        self.assertTrue(statemachine.is_terminal("failed"))
        self.assertFalse(statemachine.is_terminal("queued"))

    def test_c09_statemachine_tables_are_self_consistent(self) -> None:
        """C-09 的一半：状态与事件都在白名单内，目标状态合法。"""

        for (state, event), target in statemachine.TRANSITIONS.items():
            self.assertIn(state, statemachine.STATES)
            self.assertIn(event, statemachine.EVENTS)
            self.assertIn(target, statemachine.STATES)
        self.assertNotIn("transferring", statemachine.TERMINAL_STATES)

    def test_c09_event_types_are_within_contract(self) -> None:
        contract = {
            "task.enqueued",
            "task.transition",
            "chunk.acked",
            "task.retry_exhausted",
        }
        self.assertEqual(set(EVENT_TYPES), contract)
        self.assertEqual(set(TASK_KINDS), {"delivery", "version_upload"})
        self.assertEqual(set(VIEW_NAMES), {"tasks", "queue", "retry_stats", "claims"})


class TestClaim(QueueContractCase):
    """C-03：并发 claim 同一任务时只有一个成功。"""

    def test_c03_second_claim_returns_none_while_claim_held(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)

        first = self.queue.claim("w-1", self.clock())
        self.assertIsNotNone(first)
        self.assertEqual(first.task_id, task_id)
        self.assertEqual(first.state, "transferring")
        self.assertEqual(json.loads(first.payload)["receiver_id"], "u-b")
        self.assertEqual(first.visibility_deadline, self.clock() + 30_000)
        self.assertIsNone(self.queue.claim("w-2", self.clock()))

    def test_c03_concurrent_claim_only_one_wins(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        results: list[object] = []
        lock = threading.Lock()

        def contender(worker_id: str) -> None:
            conn = connect(self.db_path)
            try:
                queue = Queue(conn, clock=self.clock)
                claim = retry_on_busy(lambda: queue.claim(worker_id, self.clock()))
            finally:
                conn.close()
            with lock:
                results.append(claim)

        threads = [threading.Thread(target=contender, args=(f"w-{i}",)) for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        winners = [claim for claim in results if claim is not None]
        self.assertEqual(len(winners), 1, f"并发 claim 必须互斥，实际结果: {results}")

    def test_visibility_timeout_blocks_reclaim_until_expired(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        self.assertIsNone(self.queue.claim("w-2", self.clock()))
        self.clock.advance(29_999)  # 还差 1ms 到期：claim 仍然失效
        self.assertIsNone(self.queue.claim("w-2", self.clock()))
        self.clock.advance(1)  # 到点：可见性超时，recover 清理后重新可认领
        self.queue.recover(self.clock())
        reclaimed = self.queue.claim("w-2", self.clock())
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed.task_id, task_id)

    def test_heartbeat_claim_extends_only_for_holder(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        self.assertTrue(self.queue.heartbeat_claim(task_id, "w-1"))
        self.assertFalse(self.queue.heartbeat_claim(task_id, "w-2"))
        deadline = self.conn.execute(
            "SELECT visibility_deadline FROM queue_claims WHERE task_id = ?", (task_id,)
        ).fetchone()["visibility_deadline"]
        self.assertEqual(deadline, self.clock() + 30_000)


class TestAckChunk(QueueContractCase):
    """C-04：重复 ack_chunk 不重复累加进度。"""

    def test_c04_repeated_ack_is_idempotent(self) -> None:
        task_id = self.enqueue_delivery(size_bytes=2 * MB)
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        first = self.queue.ack_chunk(task_id, 0, "chunk-0")
        self.assertEqual(first.acked_chunks, 1)
        self.assertEqual(first.acked_bytes, MB)

        again = self.queue.ack_chunk(task_id, 0, "chunk-0")
        self.assertEqual(again.to_dict(), first.to_dict())
        acked_events = [
            e for e in list_events(self.conn, task_id) if e["event"] == "chunk.acked"
        ]
        self.assertEqual(len(acked_events), 1, "重复 ack 不应再写事件")

    def test_ack_with_conflicting_checksum_does_not_advance(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())
        self.queue.ack_chunk(task_id, 0, "chunk-0")

        with self.assertRaises(ChunkChecksumMismatch):
            self.queue.ack_chunk(task_id, 0, "different")
        progress = self.queue.ack_chunk(task_id, 0, "chunk-0")
        self.assertEqual(progress.acked_chunks, 1)

    def test_ack_unknown_chunk_or_task_raises(self) -> None:
        task_id = self.enqueue_delivery()
        with self.assertRaises(InvalidArgument):
            self.queue.ack_chunk(task_id, 99, "x")
        with self.assertRaises(TaskNotFound):
            self.queue.ack_chunk("t-missing", 0, "x")

    def test_chunk_size_is_configurable(self) -> None:
        queue = Queue(
            self.conn, config=QueueConfig(chunk_size_bytes=1024), clock=self.clock
        )
        task_id = queue.enqueue_task("delivery", delivery_payload(size_bytes=2500))
        self.assertEqual(self.chunk_count(task_id), 3)
        lengths = [
            row["length_bytes"]
            for row in self.conn.execute(
                "SELECT length_bytes FROM task_chunks WHERE task_id = ? ORDER BY chunk_no",
                (task_id,),
            ).fetchall()
        ]
        self.assertEqual(lengths, [1024, 1024, 452])


class TestRecover(QueueContractCase):
    """C-05：recover 连续执行两次结果一致。"""

    def test_c05_recover_expires_claim_and_is_idempotent(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())
        self.clock.advance(30_001)

        report = self.queue.recover(self.clock())
        self.assertEqual(report.claims_expired, 1)
        self.assertEqual(report.requeued, 0)
        self.assertEqual(self.state_of(task_id), "network_wait")

        second = self.queue.recover(self.clock())
        self.assertEqual(
            second.to_dict(), {"requeued": 0, "claims_expired": 0, "delay_expired": 0}
        )
        self.assertEqual(self.state_of(task_id), "network_wait")
        self.assertIsNotNone(self.queue.claim("w-2", self.clock()))

    def test_c05_recover_requeues_orphan_transferring_task(self) -> None:
        """claim 记录被外部删除（模拟 worker 崩溃后手工清理）时，任务仍能被恢复。"""

        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())
        self.conn.execute("DELETE FROM queue_claims WHERE task_id = ?", (task_id,))

        report = self.queue.recover(self.clock())
        self.assertEqual(report.requeued, 1)
        self.assertEqual(self.state_of(task_id), "network_wait")
        self.assertEqual(
            self.queue.recover(self.clock()).to_dict(),
            {"requeued": 0, "claims_expired": 0, "delay_expired": 0},
        )

    def test_c05_recover_wakes_delayed_task_when_due(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)  # 现在处于 transferring，这里只借路径
        waiting = self.queue.transition(task_id, "interrupted", ["transferring"], "P4")
        self.assertEqual(waiting, "network_wait")

        other = self.enqueue_delivery()
        self.queue.dispatch_pending(self.clock())
        self.queue.wake_user("u-b")
        delay_until = self.clock() + 5_000
        self.queue.set_delay(other, delay_until, "P2")
        self.assertEqual(self.state_of(other), "delayed")

        self.assertEqual(self.queue.recover(self.clock()).delay_expired, 0)
        self.clock.advance(5_000)
        report = self.queue.recover(self.clock())
        self.assertEqual(report.delay_expired, 1)
        self.assertEqual(self.state_of(other), "waiting_confirm")


class TestNack(QueueContractCase):
    def test_nack_retryable_moves_to_network_wait_with_backoff(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        self.queue.nack(task_id, 0, "network reset", True)
        row = self.conn.execute(
            "SELECT state, retry_count, next_run_at FROM delivery_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        self.assertEqual(row["state"], "network_wait")
        self.assertEqual(row["retry_count"], 1)
        self.assertEqual(row["next_run_at"], self.clock() + 1_000)  # 指数退避首档

    def test_nack_exhausted_marks_failed_and_raises(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        with self.assertRaises(RetryExhausted):
            for _ in range(6):
                self.queue.nack(task_id, 0, "boom", True)
        self.assertEqual(self.state_of(task_id), "failed")
        self.assertIn(
            "task.retry_exhausted", [e["event"] for e in list_events(self.conn, task_id)]
        )

    def test_nack_non_retryable_fails_immediately(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())
        with self.assertRaises(RetryExhausted):
            self.queue.nack(task_id, 0, "fatal", False)
        self.assertEqual(self.state_of(task_id), "failed")


class TestDeliveryFlow(QueueContractCase):
    """投递主流程：离线投递 -> 上线唤醒 -> 确认 -> 传输 -> 完成。"""

    def test_offline_delivery_reaches_completed(self) -> None:
        task_id = self.enqueue_delivery(size_bytes=2 * MB)
        self.assertEqual(self.state_of(task_id), "queued")

        self.assertEqual(self.queue.dispatch_pending(self.clock()), 1)
        self.assertEqual(self.state_of(task_id), "waiting_online")

        self.assertEqual(self.queue.wake_user("u-b"), 1)
        self.assertEqual(self.state_of(task_id), "waiting_confirm")
        self.assertEqual(self.queue.wake_user("u-b"), 0)  # 幂等：没有可唤醒的任务

        self.queue.transition(task_id, "confirm", ["waiting_confirm"], "P2")
        claim = self.queue.claim("w-1", self.clock())
        self.assertIsNotNone(claim)

        progress = None
        for chunk_no in range(self.chunk_count(task_id)):
            progress = self.queue.ack_chunk(task_id, chunk_no, f"chunk-{chunk_no}")
        self.assertIsNotNone(progress)
        self.assertEqual(progress.acked_bytes, progress.total_bytes)
        self.assertEqual(progress.acked_chunks, progress.total_chunks)

        final = self.queue.transition(task_id, "transfer_complete", ["transferring"], "P4")
        self.assertEqual(final, "completed")
        self.assertEqual(
            [e["event"] for e in list_events(self.conn, task_id)].count("chunk.acked"), 2
        )

    def test_reject_path_is_terminal(self) -> None:
        task_id = self.enqueue_delivery()
        self.queue.dispatch_pending(self.clock())
        self.queue.wake_user("u-b")
        self.queue.transition(task_id, "reject", ["waiting_confirm"], "P2")
        self.assertEqual(self.state_of(task_id), "rejected")
        self.assertIsNone(self.queue.claim("w-1", self.clock()))

    def test_delay_path_can_be_confirmed_after_expiry(self) -> None:
        task_id = self.enqueue_delivery()
        self.queue.dispatch_pending(self.clock())
        self.queue.wake_user("u-b")
        delay_until = self.clock() + 1_000
        self.queue.set_delay(task_id, delay_until, "P2")
        self.assertEqual(self.state_of(task_id), "delayed")

        self.clock.advance(1_000)
        self.queue.recover(self.clock())
        self.assertEqual(self.state_of(task_id), "waiting_confirm")
        self.queue.transition(task_id, "confirm", ["waiting_confirm"], "P2")
        self.assertEqual(self.state_of(task_id), "transferring")

    def test_restart_after_crash_resumes_from_acked_chunks(self) -> None:
        """验收场景 5 的内核部分：杀进程后重启，已完成的分片与状态不丢。"""

        task_id = self.enqueue_delivery(size_bytes=2 * MB)
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())
        self.queue.ack_chunk(task_id, 0, "chunk-0")

        self.conn.close()  # 模拟 daemon/worker 被 SIGKILL
        self.conn = connect(self.db_path)
        revived = Queue(self.conn, clock=self.clock)
        # 崩溃后 claim 仍在，直到可见性超时才被回收（任务不会丢，只是重新排队）
        self.clock.advance(30_001)
        self.assertEqual(revived.recover(self.clock()).claims_expired, 1)
        self.assertEqual(
            self.conn.execute(
                "SELECT acked_chunks FROM delivery_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()["acked_chunks"],
            1,
        )
        reclaimed = revived.claim("w-2", self.clock())
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed.task_id, task_id)


class TestQueryView(QueueContractCase):
    """只读视图（供 P2 管理端使用），以及它的输入校验。"""

    def test_views_return_expected_shapes(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        tasks = self.queue.query_view("tasks", {"state": "transferring"})
        self.assertEqual([row["task_id"] for row in tasks], [task_id])
        self.assertEqual(self.queue.query_view("tasks", {"state": "completed"}), [])

        by_receiver = self.queue.query_view("tasks", {"receiver_id": "u-b", "limit": 1})
        self.assertEqual(len(by_receiver), 1)

        queue_rows = {row["state"]: row for row in self.queue.query_view("queue")}
        self.assertEqual(queue_rows["transferring"]["tasks"], 1)
        self.assertEqual(queue_rows["transferring"]["active_claims"], 1)

        claims = self.queue.query_view("claims")
        self.assertEqual(claims[0]["worker_id"], "w-1")
        self.assertEqual(self.queue.query_view("retry_stats")[0]["retry_count"], 0)

    def test_unknown_view_or_filter_is_rejected(self) -> None:
        with self.assertRaises(InvalidArgument):
            self.queue.query_view("everything")
        with self.assertRaises(InvalidArgument):
            self.queue.query_view("tasks", {"nope": 1})

    def test_views_do_not_write(self) -> None:
        task_id = self.enqueue_delivery()
        before = dict(
            self.conn.execute(
                "SELECT * FROM delivery_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        )
        self.queue.query_view("tasks")
        self.queue.query_view("queue")
        self.queue.query_view("claims")
        self.queue.query_view("retry_stats")
        after = dict(
            self.conn.execute(
                "SELECT * FROM delivery_tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
        )
        self.assertEqual(before, after)


class TestHooksAndDispatch(QueueContractCase):
    def test_dispatch_immediate_for_task_without_confirmation(self) -> None:
        """契约 §1.2 修订：无需接收方确认的任务（version_upload）直接进入 transferring。"""

        task_id = self.queue.enqueue_task(
            "version_upload",
            {
                "file_id": "f-1",
                "base_version": 1,
                "tmp_path": "data/tmp/t-x/t-x.part",
                "checksum": "abc",
                "actor_id": "u-a",
            },
        )
        self.assertEqual(self.queue.dispatch_pending(self.clock()), 1)
        self.assertEqual(self.state_of(task_id), "transferring")
        claim = self.queue.claim("w-1", self.clock())
        self.assertIsNotNone(claim)
        self.assertEqual(claim.kind, "version_upload")

    def test_set_online_probe_is_used_by_dispatch(self) -> None:
        self.queue.set_online_probe(lambda user_id: user_id == "u-b")
        task_id = self.enqueue_delivery(receiver="u-b")
        self.queue.dispatch_pending(self.clock())
        self.assertEqual(self.state_of(task_id), "waiting_confirm")

    def test_hooks_run_when_due_and_isolate_failures(self) -> None:
        calls: list[int] = []
        self.queue.register_periodic_hook("p2.delay_expired", 1_000, lambda now: calls.append(now))

        def boom(_now: int) -> None:
            raise RuntimeError("hook 内部错误不应影响 daemon")

        self.queue.register_periodic_hook("p3.lease_reaper", 1_000, boom)
        with self.assertLogs("queue.hooks", level="ERROR"):
            executed = self.queue.hooks.run_due(self.clock())
        self.assertEqual(set(executed), {"p2.delay_expired", "p3.lease_reaper"})
        self.assertEqual(calls, [self.clock()])

        self.assertEqual(self.queue.hooks.run_due(self.clock()), [])  # 未到期
        self.clock.advance(1_000)
        with self.assertLogs("queue.hooks", level="ERROR"):
            self.assertEqual(len(self.queue.hooks.run_due(self.clock())), 2)

    def test_dispatch_pending_uses_receiver_probe(self) -> None:
        offline = self.enqueue_delivery(receiver="u-b")
        online = self.enqueue_delivery(receiver="u-c")

        dispatched = self.queue.dispatch_pending(
            self.clock(), receiver_online=lambda user_id: user_id == "u-c"
        )
        self.assertEqual(dispatched, 2)
        self.assertEqual(self.state_of(offline), "waiting_online")
        self.assertEqual(self.state_of(online), "waiting_confirm")
        self.assertEqual(self.queue.wake_user("u-b"), 1)
        self.assertEqual(self.state_of(offline), "waiting_confirm")

    def test_dispatch_pending_skips_tasks_scheduled_in_the_future(self) -> None:
        task_id = self.queue.enqueue_task(
            "delivery", delivery_payload(), run_after=self.clock() + 10_000
        )
        self.assertEqual(self.queue.dispatch_pending(self.clock()), 0)
        self.assertEqual(self.state_of(task_id), "queued")
        self.clock.advance(10_000)
        self.assertEqual(self.queue.dispatch_pending(self.clock()), 1)


class RecordingAuditSink:
    """审计模块的测试替身（按 docs/Interfaces.md §3 的 AuditAPI 形状）。"""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, str]] = []
        self.bound_tx: object | None = None

    def write(self, event_type, actor_id, target_type, target_id, detail) -> None:  # type: ignore[no-untyped-def]
        self.calls.append((event_type, actor_id, target_type, target_id))

    def join_tx(self, tx):  # type: ignore[no-untyped-def]
        self.bound_tx = tx
        return self


class _SqliteAuditWriter:
    def __init__(self, tx: sqlite3.Connection) -> None:
        self.tx = tx

    def write(self, event_type, actor_id, target_type, target_id, detail) -> None:  # type: ignore[no-untyped-def]
        self.tx.execute(
            """
            INSERT INTO audit_logs (event_type, actor_id, target_type, target_id, detail, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_type, actor_id, target_type, target_id, json.dumps(detail), 0),
        )


class SqliteAuditSink:
    """把审计写进调用方事务的 audit_logs，用于验证 join_tx 的同事务语义。"""

    def join_tx(self, tx: sqlite3.Connection) -> _SqliteAuditWriter:
        return _SqliteAuditWriter(tx)

    def write(self, *args: object, **kwargs: object) -> None:
        raise AssertionError("P1 必须通过 join_tx 绑定调用方事务后再写审计")


class TestAudit(QueueContractCase):
    """待澄清项第 7 条：P1 把 task.* 事件写进审计，且与业务同事务。"""

    def audit_count(self) -> int:
        return int(self.conn.execute("SELECT COUNT(*) AS c FROM audit_logs").fetchone()["c"])

    def test_p1_emits_its_own_event_types(self) -> None:
        sink = RecordingAuditSink()
        queue = Queue(self.conn, clock=self.clock, audit=sink)

        task_id = queue.enqueue_task("delivery", delivery_payload())
        queue.dispatch_pending(self.clock())
        queue.wake_user("u-b")
        queue.transition(task_id, "confirm", ["waiting_confirm"], "P2")
        queue.claim("w-1", self.clock())
        queue.ack_chunk(task_id, 0, "chunk-0")

        event_types = [call[0] for call in sink.calls]
        self.assertEqual(event_types[0], "task.enqueued")
        self.assertEqual(event_types.count("task.transition"), 4)
        self.assertIn("chunk.acked", event_types)
        self.assertTrue(all(call[2] == "task" and call[3] == task_id for call in sink.calls))
        self.assertIsNotNone(sink.bound_tx, "P1 必须通过 join_tx 把审计绑定到业务事务")

    def test_without_sink_no_audit_is_written(self) -> None:
        self.enqueue_delivery()
        self.assertEqual(self.audit_count(), 0)

    def test_c12_audit_shares_the_caller_transaction(self) -> None:
        sink = SqliteAuditSink()
        queue = Queue(self.conn, clock=self.clock, audit=sink)

        self.conn.execute("BEGIN IMMEDIATE")
        queue.enqueue_task("delivery", delivery_payload())
        self.assertEqual(self.audit_count(), 2)  # task.enqueued + task.transition
        self.conn.execute("ROLLBACK")
        self.assertEqual(self.audit_count(), 0, "业务回滚后审计记录必须一起消失")

        task_id = queue.enqueue_task("delivery", delivery_payload())
        self.assertEqual(self.audit_count(), 2)
        rows = self.conn.execute("SELECT event_type FROM audit_logs ORDER BY log_id").fetchall()
        self.assertEqual([row["event_type"] for row in rows], ["task.enqueued", "task.transition"])
        self.assertIsNotNone(task_id)


class TestDaemon(QueueContractCase):
    def test_tick_dispatches_and_recovers_on_schedule(self) -> None:
        from src.daemon.main import Daemon

        task_id = self.enqueue_delivery()
        daemon = Daemon(self.queue, interval_ms=10, recover_every_ms=1_000, clock=self.clock)
        self.assertIn("requeued", daemon.bootstrap())

        summary = daemon.tick(self.clock())
        self.assertIsNone(summary["recover"])  # 刚恢复过，不在周期内
        self.assertEqual(summary["dispatched"], 1)
        self.assertEqual(self.state_of(task_id), "waiting_online")

        self.clock.advance(1_000)
        self.assertIsNotNone(daemon.tick(self.clock())["recover"])

    def test_run_forever_stops_on_request(self) -> None:
        from src.daemon.main import Daemon

        daemon = Daemon(self.queue, interval_ms=1, recover_every_ms=0, clock=self.clock)
        daemon.request_stop()
        self.assertEqual(daemon.run_forever(), 0)


class TestInfrastructure(unittest.TestCase):
    def test_ensure_data_dirs_creates_controlled_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ensure_data_dirs(root)
            for rel in ("data/db", "data/tmp", "data/objects", "data/logs"):
                self.assertTrue((root / rel).is_dir(), f"缺少目录 {rel}")

    def test_init_database_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "queue.db"
            first = init_database(db_path)
            first.close()
            second = init_database(db_path)
            tables = {
                row["name"]
                for row in second.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            second.close()
            self.assertIn("delivery_tasks", tables)
            self.assertIn("audit_logs", tables)


class TestSqlOwnership(unittest.TestCase):
    """C-10：每张表只出现一个写入方（静态检查 SQL 归属）。"""

    WRITE_RE = re.compile(
        r"\b(INSERT(?:\s+OR\s+\w+)?\s+INTO|UPDATE|DELETE\s+FROM|REPLACE\s+INTO)\s+([a-z_][a-z0-9_]*)",
        re.IGNORECASE,
    )
    QUEUE_TABLES = {"delivery_tasks", "task_chunks", "queue_claims", "task_events"}
    #: 每张表的写入方必须落在这些目录里
    OWNERS = {
        "delivery_tasks": ("src/storage",),
        "task_chunks": ("src/storage",),
        "queue_claims": ("src/storage",),
        "task_events": ("src/storage",),
    }

    def _writes_in(self, directory: Path) -> list[tuple[Path, str, str]]:
        found = []
        for path in sorted(directory.rglob("*.py")):
            for statement, table in self.WRITE_RE.findall(path.read_text(encoding="utf-8")):
                found.append((path, statement.upper(), table.lower()))
        return found

    def test_c10_queue_tables_are_written_only_by_p1(self) -> None:
        violations = []
        for path, statement, table in self._writes_in(ROOT / "src"):
            if table not in self.QUEUE_TABLES:
                continue
            relative = path.relative_to(ROOT).as_posix()
            allowed = any(relative.startswith(p) for p in self.OWNERS[table])
            if not allowed:
                violations.append(f"{relative}: {statement} {table}")
        self.assertEqual(violations, [], f"队列表被 P1 之外的文件写入: {violations}")

    def test_c10_all_known_tables_declare_an_owner(self) -> None:
        written = {table for _, _, table in self._writes_in(ROOT / "src")}
        undeclared = {
            table
            for table in written
            if table.startswith(("task", "delivery", "queue"))
            and table not in self.OWNERS
        }
        self.assertEqual(undeclared, set(), f"有写语句但没有归属声明: {undeclared}")

    def test_c10_detector_sees_p1_writes(self) -> None:
        """反向确认：正则确实能扫到 P1 的写语句，避免上面的检查变成假通过。"""

        found = {table for _, _, table in self._writes_in(ROOT / "src" / "storage")}
        self.assertTrue(
            self.QUEUE_TABLES <= found, f"未扫描到写语句: {self.QUEUE_TABLES - found}"
        )


def _claim_worker(db_path: str, worker_id: str, now: int, out: object) -> None:
    """子进程入口：在独立进程里尝试认领同一个任务。"""

    conn = connect(db_path)
    try:
        claim = Queue(conn, clock=lambda: now).claim(worker_id, now)
        out.put(None if claim is None else claim.task_id)  # type: ignore[attr-defined]
    finally:
        conn.close()


class TestConcurrency(QueueContractCase):
    """C-03 的进程级版本：多进程竞争同一任务，只能有一个赢家。"""

    def test_c03_multiprocess_claim_has_single_winner(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)

        methods = multiprocessing.get_all_start_methods()
        context = multiprocessing.get_context("fork" if "fork" in methods else "spawn")
        out = context.Queue()
        workers = [
            context.Process(
                target=_claim_worker, args=(str(self.db_path), f"w-{i}", self.clock(), out)
            )
            for i in range(4)
        ]
        for proc in workers:
            proc.start()
        for proc in workers:
            proc.join(timeout=30)
        self.assertTrue(all(not proc.is_alive() for proc in workers), "子进程未在 30s 内结束")

        results = [out.get(timeout=5) for _ in workers]
        winners = [result for result in results if result is not None]
        self.assertEqual(len(winners), 1, f"只能有一个赢家，实际: {results}")
        self.assertEqual(winners[0], task_id)

    def test_retry_on_busy_waits_for_the_writer_lock(self) -> None:
        """写者竞争：另一个连接持有写锁时，claim 通过退避重试最终成功。"""

        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)

        holder = connect(self.db_path)
        holder.execute("PRAGMA busy_timeout = 0")
        holder.execute("BEGIN IMMEDIATE")
        holder.execute("UPDATE delivery_tasks SET updated_at = 0 WHERE task_id = ?", (task_id,))

        outcome: dict[str, object] = {}

        def contender() -> None:
            # sqlite3 连接不能跨线程使用：在子线程里自己开一个连接
            seeker = connect(self.db_path)
            seeker.execute("PRAGMA busy_timeout = 0")
            queue = Queue(seeker, clock=self.clock)
            try:
                outcome["claim"] = retry_on_busy(
                    lambda: queue.claim("w-2", self.clock()), attempts=40, base_delay=0.02
                )
            except Exception as exc:  # noqa: BLE001 - 把异常带回主线程断言
                outcome["error"] = exc
            finally:
                seeker.close()

        thread = threading.Thread(target=contender, daemon=True)
        thread.start()
        time.sleep(0.2)  # 让 seeker 先撞上 database is locked
        self.assertNotIn("claim", outcome, "写锁未释放前不应认领成功")
        holder.execute("COMMIT")
        thread.join(timeout=15)
        holder.close()

        self.assertNotIn("error", outcome, f"重试后仍然失败: {outcome.get('error')}")
        self.assertIsNotNone(outcome.get("claim"))
        self.assertEqual(self.state_of(task_id), "transferring")


class TestBackoffCurve(QueueContractCase):
    """重试间隔、退避曲线是 P1 的配置项（契约 §2），这里锁住它的行为。"""

    def test_backoff_curve_and_cap(self) -> None:
        policy = RetryPolicy(initial_delay_ms=1_000, multiplier=2.0, max_delay_ms=5_000)
        self.assertEqual(
            [policy.backoff_ms(attempt) for attempt in range(1, 6)],
            [1_000, 2_000, 4_000, 5_000, 5_000],
        )
        self.assertEqual(policy.backoff_ms(0), 1_000, "attempt 从 1 起算，0 按 1 处理")

    def test_successive_nacks_follow_the_curve_then_fail(self) -> None:
        task_id = self.enqueue_delivery()
        self.move_to_transferring(task_id)
        self.queue.claim("w-1", self.clock())

        for attempt, delay in enumerate([1_000, 2_000, 4_000, 8_000, 16_000], start=1):
            self.queue.nack(task_id, 0, f"fail-{attempt}", True)
            row = self.conn.execute(
                """
                SELECT retry_count, next_run_at, state
                  FROM delivery_tasks WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
            self.assertEqual(row["retry_count"], attempt)
            self.assertEqual(row["next_run_at"], self.clock() + delay)
            self.assertEqual(row["state"], "network_wait")

        with self.assertRaises(RetryExhausted):  # 第 6 次超过 max_retries=5
            self.queue.nack(task_id, 0, "fail-6", True)
        self.assertEqual(self.state_of(task_id), "failed")


class TestMigrations(unittest.TestCase):
    """C-13：迁移文件与 db/schema.sql 的结构必须一致，且迁移可重复应用。"""

    def _structure(self, conn: sqlite3.Connection) -> dict[str, dict[str, set]]:
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        } - {"schema_migrations"}
        structure: dict[str, dict[str, set]] = {}
        for table in tables:
            structure[table] = {
                "columns": {
                    (
                        row["name"],
                        (row["type"] or "").upper(),
                        row["notnull"],
                        row["dflt_value"],
                        row["pk"],
                    )
                    for row in conn.execute(f"PRAGMA table_info({table})")
                },
                "indexes": {
                    row["name"]
                    for row in conn.execute(f"PRAGMA index_list({table})")
                    if not row["name"].startswith("sqlite_autoindex")
                },
            }
        return structure

    def test_c13_migrations_match_schema_sql(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            from_schema = init_database(Path(tmp) / "schema.db")
            expected = self._structure(from_schema)
            from_schema.close()

            migrated = connect(Path(tmp) / "migrated.db")
            try:
                self.assertIn("0001_init.sql", apply_migrations(migrated))
                self.assertEqual(self._structure(migrated), expected)
            finally:
                migrated.close()

    def test_apply_migrations_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            conn = connect(Path(tmp) / "queue.db")
            try:
                self.assertEqual(apply_migrations(conn), ["0001_init.sql"])
                self.assertEqual(apply_migrations(conn), [])
                rows = conn.execute("SELECT name FROM schema_migrations").fetchall()
                self.assertEqual([row["name"] for row in rows], ["0001_init.sql"])
            finally:
                conn.close()
