"""SQLite 连接、事务与初始化（P1）。

设计要点：

* 连接以 isolation_level=None 打开（自动提交），事务一律由 tx() 显式控制，
  便于落实「禁止跨模块事务」的硬规则：事务句柄只能通过参数显式传递。
* tx() 拒绝嵌套事务；auto_tx() 在已处于事务中时复用当前事务，供同一模块内部组合调用。
* 结构变更只能由 P1 提交（db/schema.sql + db/migrations/）。
"""

from __future__ import annotations

import contextlib
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

from src.common.timeutil import now_ms

T = TypeVar("T")

#: 仓库根目录（src/storage/db.py -> src/storage -> src -> 根）
ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT_DIR / "data" / "db" / "queue.db"
DEFAULT_SCHEMA_PATH = ROOT_DIR / "db" / "schema.sql"
DEFAULT_MIGRATIONS_DIR = ROOT_DIR / "db" / "migrations"

#: 启动时必须存在的受控目录（目录由 P1 创建，目录内文件的写入归各自负责人）
DATA_DIRS = ("data/db", "data/tmp", "data/objects", "data/logs")


def ensure_data_dirs(root: str | Path = ROOT_DIR) -> dict[str, Path]:
    """创建受控目录，返回 {相对路径: 绝对路径}。可重复执行。"""

    root_path = Path(root)
    created: dict[str, Path] = {}
    for rel in DATA_DIRS:
        path = root_path / rel
        path.mkdir(parents=True, exist_ok=True)
        created[rel] = path
    return created


def connect(db_path: str | Path = DEFAULT_DB_PATH, *, timeout: float = 5.0) -> sqlite3.Connection:
    """打开连接并设置 WAL / 外键 / busy_timeout。"""

    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=timeout, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextlib.contextmanager
def tx(conn: sqlite3.Connection, *, mode: str = "IMMEDIATE") -> Iterator[sqlite3.Connection]:
    """显式事务：BEGIN IMMEDIATE -> COMMIT / ROLLBACK。

    嵌套调用直接报错，避免出现「A 模块开事务调用 B 模块」这种无法回滚的边界。
    """

    if conn.in_transaction:
        raise RuntimeError("禁止嵌套事务：请通过参数显式传递事务句柄（见 docs/Interfaces.md §4.2）")
    conn.execute(f"BEGIN {mode}")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextlib.contextmanager
def auto_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """已处于事务中则复用，否则新开一个事务（同模块内部组合用）。"""

    if conn.in_transaction:
        yield conn
    else:
        with tx(conn) as active:
            yield active


def retry_on_busy(
    fn: Callable[..., T],
    *args: Any,
    attempts: int = 5,
    base_delay: float = 0.02,
    **kwargs: Any,
) -> T:
    """对 SQLITE_BUSY / locked 做指数退避重试（写者之间的正常竞争，不是错误）。"""

    for index in range(attempts):
        try:
            return fn(*args, **kwargs)
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            if index == attempts - 1:
                raise
            time.sleep(base_delay * (2 ** index))
    raise AssertionError("unreachable")  # pragma: no cover


def init_database(
    db_path: str | Path = DEFAULT_DB_PATH,
    schema_path: str | Path | None = None,
) -> sqlite3.Connection:
    """在空目录一键建库：创建受控目录并执行全量 DDL（幂等）。"""

    connect(db_path).close()  # 确保父目录存在
    conn = connect(db_path)
    schema_file = Path(schema_path) if schema_path else DEFAULT_SCHEMA_PATH
    conn.executescript(schema_file.read_text(encoding="utf-8"))
    return conn


def apply_migrations(
    conn: sqlite3.Connection,
    migrations_dir: str | Path | None = None,
) -> list[str]:
    """按文件名顺序执行尚未应用的迁移，返回本次应用的文件名（可重复调用）。

    已应用的名字记录在 ``schema_migrations`` 表。注意 ``sqlite3.executescript`` 会隐式提交，
    因此迁移脚本必须写成幂等形式，失败后可以安全重跑。
    """

    directory = Path(migrations_dir) if migrations_dir else DEFAULT_MIGRATIONS_DIR
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name       TEXT PRIMARY KEY,
            applied_at INTEGER NOT NULL
        )
        """
    )
    applied = {row["name"] for row in conn.execute("SELECT name FROM schema_migrations")}
    executed: list[str] = []
    for path in sorted(directory.glob("*.sql")):
        if path.name in applied:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
        with tx(conn):
            conn.execute(
                "INSERT OR REPLACE INTO schema_migrations (name, applied_at) VALUES (?, ?)",
                (path.name, now_ms()),
            )
        executed.append(path.name)
    return executed


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None
