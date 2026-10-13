"""队列内核的可调参数。

契约要点（docs/Interfaces.md §2）：重试间隔、退避曲线、可见性超时长度均为 P1 的配置项，
其他人不得写死。分片大小同理，属于 P1 与 P4 之间的契约（见 docs/契约待澄清项.md 第 5 条）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass(frozen=True)
class RetryPolicy:
    """重试策略：指数退避 + 上限，配置项归 P1。"""

    initial_delay_ms: int = 1_000
    max_delay_ms: int = 60_000
    multiplier: float = 2.0
    max_retries: int = 5

    def backoff_ms(self, attempt: int) -> int:
        """第 attempt 次失败后等待多久再执行（attempt 从 1 开始）。"""

        if attempt < 1:
            attempt = 1
        delay = self.initial_delay_ms * (self.multiplier ** (attempt - 1))
        return int(min(delay, self.max_delay_ms))


@dataclass
class QueueConfig:
    """队列内核配置。"""

    chunk_size_bytes: int = 1 << 20  # 1 MiB
    visibility_timeout_ms: int = 30_000
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    #: 调度时需要判断「接收方是否在线」，但 users 表属于 P2。
    #: 为避免 P1 跨模块读表，由 P2 注入一个探针；缺省视为离线（任务进入 waiting_online，
    #: 之后靠 P2 的心跳接口调用 wake_user 唤醒）。见 docs/契约待澄清项.md 第 2 条。
    receiver_online_probe: Callable[[str], bool] | None = None
