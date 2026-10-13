"""定时钩子注册表（P1）。

daemon 只认识本模块的 HookRegistry，不认识 P2/P3 的业务代码：
P2 注册「延迟到期检查」、P3 注册「租约过期回收」。钩子抛异常不得中断主循环。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger("queue.hooks")


@dataclass
class PeriodicHook:
    name: str
    interval_ms: int
    fn: Callable[..., Any]
    last_run_at: int | None = None

    def due(self, now: int) -> bool:
        if self.interval_ms <= 0:
            return True
        if self.last_run_at is None:
            return True
        return now - self.last_run_at >= self.interval_ms


@dataclass
class HookRegistry:
    """按名字注册；重复注册同名钩子视为覆盖（便于测试里反复注册）。"""

    hooks: dict[str, PeriodicHook] = field(default_factory=dict)

    def register(self, name: str, interval_ms: int, fn: Callable[..., Any]) -> None:
        if not name:
            raise ValueError("钩子名不能为空")
        if interval_ms < 0:
            raise ValueError("interval_ms 不能为负")
        self.hooks[name] = PeriodicHook(name=name, interval_ms=interval_ms, fn=fn)

    def due(self, now: int) -> list[PeriodicHook]:
        return [hook for hook in self.hooks.values() if hook.due(now)]

    def run_due(self, now: int) -> list[str]:
        """执行到期的钩子并返回其名字；单个钩子失败只记录日志，不影响其他钩子与主循环。"""

        executed: list[str] = []
        for hook in self.due(now):
            try:
                hook.fn(now)
            except Exception:  # noqa: BLE001 - 钩子异常必须被隔离
                logger.exception("定时钩子执行失败: %s", hook.name)
            finally:
                hook.last_run_at = now
            executed.append(hook.name)
        return executed
