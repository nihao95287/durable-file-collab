# 结构迁移约定（P1 维护）

- 新建库：`init_database()` 直接执行 `db/schema.sql`（当前全量结构）。
- 存量库升级：`apply_migrations(conn)` 按文件名顺序执行本目录里尚未应用的 `*.sql`，应用记录写在
  `schema_migrations` 表，可重复调用（第二次返回空列表）。
- 每次结构变更新增一个 `NNNN_描述.sql`（如 `0002_add_task_priority.sql`），并在同一个 PR 里把改动同步进 `db/schema.sql`。
- 迁移里的 DDL 必须写成**幂等**形式（`IF NOT EXISTS` 等）：`sqlite3.executescript` 会隐式提交，
  迁移脚本与应用记录不在同一个事务里，失败后要能安全重跑。
- 变更必须写清「兼容方式」或「迁移方式」，PR 里说明影响到的接口与契约测试条目。
- 迁移与 `db/schema.sql` 的结构一致性由契约测试 C-13 校验（表、列、索引逐项比对），不一致时拒绝合并。
- 只有 P1 可以提交本目录的改动；其他成员以字段需求的形式在 PR 里提出。
