"""契约数据结构（docs/Interfaces.md §2）。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Claim:
    """``claim()`` 的返回值；None 表示当前无可执行任务。"""

    task_id: str
    kind: str
    payload: str
    state: str
    visibility_deadline: int
    attempt: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "kind": self.kind,
            "payload": self.payload,
            "state": self.state,
            "visibility_deadline": self.visibility_deadline,
            "attempt": self.attempt,
        }


@dataclass(frozen=True)
class Progress:
    """``ack_chunk()`` 的返回值，也是任务进度的对外形态。"""

    acked_bytes: int
    total_bytes: int
    acked_chunks: int
    total_chunks: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "acked_bytes": self.acked_bytes,
            "total_bytes": self.total_bytes,
            "acked_chunks": self.acked_chunks,
            "total_chunks": self.total_chunks,
        }


@dataclass(frozen=True)
class RecoverReport:
    """``recover()`` 的返回值；可重复执行，第二次应为全 0。"""

    requeued: int = 0
    claims_expired: int = 0
    delay_expired: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "requeued": self.requeued,
            "claims_expired": self.claims_expired,
            "delay_expired": self.delay_expired,
        }
