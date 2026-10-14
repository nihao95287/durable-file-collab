-- SyncBox · 全量结构（单一来源）
-- 归属：M02（线 A）独占维护。其他成员以「字段需求」的形式提出变更，由 M02 落库（见 docs/Interfaces.md §6）。
-- 初始化：python3 -c "from src.storage.db import init_database; init_database('data/db/queue.db')"
-- 结构变更：新增 db/migrations/000N_描述.sql，并同步本文件；两者一致性由契约测试 C-10 静态检查。

PRAGMA foreign_keys = ON;

-- ============================ M02（线 A）独占：持久化队列内核 ============================

CREATE TABLE IF NOT EXISTS delivery_tasks (
    task_id       TEXT PRIMARY KEY,              -- 创建方生成，全局唯一且不可变
    kind          TEXT NOT NULL,                 -- 任务类型，取值见 TASK_KINDS
    payload       TEXT NOT NULL,                 -- JSON 字符串，M02 原样存储，不解释业务字段
    state         TEXT NOT NULL,                 -- 见 src/storage/statemachine.py 的 STATES
    sender_id     TEXT,                          -- 路由冗余列：仅从 payload 提取 sender_id/receiver_id/size_bytes
    receiver_id   TEXT,
    size_bytes    INTEGER NOT NULL DEFAULT 0,
    acked_bytes   INTEGER NOT NULL DEFAULT 0,
    total_chunks  INTEGER NOT NULL DEFAULT 0,
    acked_chunks  INTEGER NOT NULL DEFAULT 0,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 5,
    next_run_at   INTEGER NOT NULL,              -- UTC 毫秒；最早可执行时间
    last_error    TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_claimable ON delivery_tasks(state, next_run_at);
CREATE INDEX IF NOT EXISTS idx_tasks_receiver  ON delivery_tasks(receiver_id, state);
CREATE INDEX IF NOT EXISTS idx_tasks_sender    ON delivery_tasks(sender_id, state);

CREATE TABLE IF NOT EXISTS task_chunks (
    task_id      TEXT    NOT NULL REFERENCES delivery_tasks(task_id) ON DELETE CASCADE,
    chunk_no     INTEGER NOT NULL,
    offset_bytes INTEGER NOT NULL,
    length_bytes INTEGER NOT NULL,
    checksum     TEXT,                            -- 首次 ack 时写入；重复 ack 必须与它一致
    state        TEXT    NOT NULL DEFAULT 'pending',  -- pending | acked
    attempts     INTEGER NOT NULL DEFAULT 0,
    acked_at     INTEGER,
    last_error   TEXT,
    PRIMARY KEY (task_id, chunk_no)
);

CREATE TABLE IF NOT EXISTS queue_claims (
    task_id             TEXT PRIMARY KEY REFERENCES delivery_tasks(task_id) ON DELETE CASCADE,
    worker_id           TEXT    NOT NULL,
    attempt             INTEGER NOT NULL DEFAULT 1,
    claimed_at          INTEGER NOT NULL,
    visibility_deadline INTEGER NOT NULL          -- 超时后由 recover 清理并重新入队
);

CREATE INDEX IF NOT EXISTS idx_claims_deadline ON queue_claims(visibility_deadline);

CREATE TABLE IF NOT EXISTS task_events (
    event_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT    NOT NULL,
    event      TEXT    NOT NULL,                  -- 见 src/storage/events.py 的 EVENT_TYPES
    from_state TEXT,
    to_state   TEXT,
    actor      TEXT    NOT NULL,                  -- 线 A / B / C / D
    detail     TEXT,                              -- JSON 字符串
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, event_id);

-- ============================ 其他模块独占（字段草案，待第 1 周评审确认） ============================

-- M05（线 B）独占：用户会话
CREATE TABLE IF NOT EXISTS users (
    user_id      TEXT PRIMARY KEY,
    display_name TEXT,
    online       INTEGER NOT NULL DEFAULT 0,
    last_seen    INTEGER
);

-- M07/M08/M09（线 C）独占：项目空间、成员、版本与租约
CREATE TABLE IF NOT EXISTS project_spaces (
    space_id   TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS space_members (
    space_id TEXT NOT NULL REFERENCES project_spaces(space_id) ON DELETE CASCADE,
    user_id  TEXT NOT NULL,
    role     TEXT NOT NULL,                       -- read | write | admin
    PRIMARY KEY (space_id, user_id)
);

CREATE TABLE IF NOT EXISTS files (
    file_id      TEXT PRIMARY KEY,
    space_id     TEXT NOT NULL REFERENCES project_spaces(space_id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    head_version INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS file_versions (
    file_id      TEXT    NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
    version_no   INTEGER NOT NULL,
    created_by   TEXT,
    base_version INTEGER,
    checksum     TEXT,
    object_path  TEXT    NOT NULL,                -- 只能由 objectstore.publish 产生
    is_conflict  INTEGER NOT NULL DEFAULT 0,
    created_at   INTEGER NOT NULL,
    PRIMARY KEY (file_id, version_no)
);

CREATE TABLE IF NOT EXISTS leases (
    file_id     TEXT PRIMARY KEY REFERENCES files(file_id) ON DELETE CASCADE,
    holder_id   TEXT    NOT NULL,
    token       TEXT    NOT NULL UNIQUE,
    acquired_at INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leases_expiry ON leases(expires_at);

-- M04（线 D）独占：追加式审计
CREATE TABLE IF NOT EXISTS audit_logs (
    log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,                    -- <模块>.<动作>
    actor_id    TEXT,
    target_type TEXT,
    target_id   TEXT,
    detail      TEXT,                             -- JSON 字符串
    created_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_target ON audit_logs(target_id, log_id);
