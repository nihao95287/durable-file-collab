"""AuditAPI 的接口声明（docs/Interfaces.md §3）。

实现与 audit_logs 表归 P4（src/audit/）；本文件只声明 P1/P2/P3 依赖的接口形状，
让调用方在审计库交付之前就能按契约接线、并用测试替身验证「与业务同事务」这一语义。
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AuditWriter(Protocol):
    """绑定到某个事务的写入器（join_tx 的返回值）。"""

    def write(
        self,
        event_type: str,
        actor_id: str,
        target_type: str,
        target_id: str,
        detail: dict[str, Any],
    ) -> None: ...


@runtime_checkable
class AuditSink(Protocol):
    """P4 提供的审计模块。只追加、不更新、不删除。"""

    def write(
        self,
        event_type: str,
        actor_id: str,
        target_type: str,
        target_id: str,
        detail: dict[str, Any],
    ) -> None: ...

    def join_tx(self, tx: Any) -> AuditWriter: ...


class _NullWriter:
    def write(
        self,
        event_type: str,
        actor_id: str,
        target_type: str,
        target_id: str,
        detail: dict[str, Any],
    ) -> None:
        return None


class NullAuditSink:
    """缺省实现：什么都不做。用于审计库尚未交付时保持接口可调用。"""

    def write(
        self,
        event_type: str,
        actor_id: str,
        target_type: str,
        target_id: str,
        detail: dict[str, Any],
    ) -> None:
        return None

    def join_tx(self, tx: Any) -> AuditWriter:
        return _NullWriter()


def audit_writer(sink: AuditSink | None, tx: Any) -> AuditWriter | None:
    """把审计模块绑定到调用方事务；没有注入审计模块时返回 None（不写审计）。"""

    if sink is None:
        return None
    join = getattr(sink, "join_tx", None)
    return join(tx) if callable(join) else sink
