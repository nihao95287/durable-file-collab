"""时间约定：全局使用 UTC 毫秒级整数时间戳（docs/Interfaces.md §0.2）。"""

from __future__ import annotations

import time


def now_ms() -> int:
    """当前 UTC 毫秒时间戳。测试里通过注入假时钟来替代它。"""

    return int(time.time() * 1000)


def seconds_to_ms(seconds: float) -> int:
    return int(seconds * 1000)


def ms_to_seconds(ms: int) -> float:
    return ms / 1000.0
