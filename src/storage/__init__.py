"""队列内核（P1）：SQLite 持久化、事务原语、状态机、队列原语与守护进程入口。

对外只暴露 QueueAPI（Queue 类）与 init_database；其他模块禁止 import 本包的内部实现文件。
"""

from .config import QueueConfig, RetryPolicy
from .db import ROOT_DIR, connect, ensure_data_dirs, init_database, tx
from .queue import Queue

__all__ = [
    "Queue",
    "QueueConfig",
    "RetryPolicy",
    "connect",
    "init_database",
    "ensure_data_dirs",
    "tx",
    "ROOT_DIR",
]
