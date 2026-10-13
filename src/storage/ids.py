"""ID 生成：契约约定 ID 由创建方生成、全局唯一且不可变（docs/Interfaces.md §0.2）。"""

from __future__ import annotations

import uuid

_PREFIXES = {
    "task": "t",
    "event": "e",
    "user": "u",
    "space": "s",
    "file": "f",
    "lease": "l",
    "worker": "w",
}


def new_id(kind: str) -> str:
    """按前缀生成 ID，如 new_id("task") -> "t-1f3a9c2b"。"""

    try:
        prefix = _PREFIXES[kind]
    except KeyError as exc:  # pragma: no cover - 防御性
        raise ValueError(f"未知的 ID 类型: {kind!r}") from exc
    return f"{prefix}-{uuid.uuid4().hex[:8]}"
