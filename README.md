# SyncBox：基于操作系统机制的可靠文件同步与异步投递平台

面向操作系统课程设计的单机多进程原型，围绕 SQLite 持久化队列实现离线文件投递、分片续传、版本历史、冲突检测和文件租约。

## 项目资料

- [需求分析与概要设计](需求分析与概要设计.md)：需求、架构、数据模型、接口和验收场景。
- [模块划分与依赖](docs/模块划分与依赖.md)：`M01`～`M15` 模块目录、**详细依赖关系**、模块定义与认领表。
- [模块认领与协作方案](模块认领与协作方案.md)：认领流程、四条工作线打包、4 周排期、CODEOWNERS 与协作规范。
- [接口契约](docs/Interfaces.md)：状态机、QueueAPI、AuditAPI、ObjectStore、HTTP 接口与契约测试清单。
- [契约修订提案](docs/契约待澄清项.md)：实现时发现的契约歧义、决议与待确认项，第 1 周评审后打 `contract-v1`。

## 目标范围

本阶段以一台主机上的 SQLite、受控文件目录和多个进程完成课程演示，不依赖公网或云服务。离线接收和断点续传通过持久化任务、分片确认和进程恢复体现；项目空间协作通过版本号、冲突副本及短租约体现。P2P、分布式多活和商业云盘功能不在本阶段范围内。

## 核心能力

- 先在 SQLite 事务中保存投递任务，再交给后台队列处理。
- 接收者上线后唤醒任务，允许确认、拒绝或延迟。
- 分片校验后确认进度，故障后从已确认偏移恢复。
- 通过原子发布避免接收方看到未完成文件。
- 以 base_version 检测并发修改，保留历史版本和冲突副本。
- 使用短租约协调编辑，并记录追加式操作审计。

## 模块划分与依赖

系统按 `M01`～`M15` 划分，分四层 + 横切质量层。完整定义与逐条依赖见 [模块划分与依赖](docs/模块划分与依赖.md)。

| 层 | 模块 | 职责 |
| --- | --- | --- |
| L0 基座 | M01 公共契约、M02 队列内核、M03 守护进程、M04 审计 | 错误码/类型、schema 与队列原语、调度与恢复、追加式审计 |
| L1 业务 | M05 会话、M06 投递编排、M07 空间、M08 版本、M09 租约 | 心跳唤醒、确认三路径、权限、冲突副本、加锁 |
| L2 传输 | M10 对象存储、M11 传输 worker | 原子发布、分片读写与断点续传 |
| L3 界面运维 | M12 管理端、M13 进程编排 | 只读展示、一键启停 |
| 质量 | M14 契约测试、M15 端到端验收 | CI 门禁、五个验收场景 |

关键依赖链：

```text
M01 公共契约 ► M02 队列内核 ► M03 守护进程 ► M11 传输 worker ► M13 一键装配 ► M15 验收
                    ▲
     M05/M06/M07/M08/M09/M12 全部依赖 M02 的 QueueAPI
     M08 版本依赖 M09 租约、M10 对象存储；M11 worker 依赖 M10 对象存储
     全体模块通过 M04 的 audit.join_tx 写审计
```

一句话记法：**M02 是根，M04/M10 是被依赖的基座，M05～M09 是互不改表的业务并行区，M12/M13 只做装配与展示。**

## 模块认领

模块是认领单元，先到先得，认领前需确认前置依赖已就位：

1. 在 [模块划分与依赖](docs/模块划分与依赖.md) §6 认领表挑选模块并登记「认领人 / 状态」。
2. 按 [模块认领与协作方案](模块认领与协作方案.md) §5 的排期与依赖顺序开发。
3. 分支、PR 标题带上模块 ID，如 `feat/M08-version-conflict`。

| 线 | 角色 | 模块 | 当前状态 |
| --- | --- | --- | --- |
| A | 队列内核 | M01、M02、M03 | 骨架已交付 |
| B | 会话/投递/管理端/编排 | M05、M06、M12、M13 | 待认领 |
| C | 协作与并发 | M07、M08、M09 | 待认领 |
| D | 传输/存储/审计 | M04、M10、M11 | 待认领 |

## 协作约定

1. 从 `main` 创建短期分支，命名带模块 ID，如 `feat/M02-queue-core`、`fix/M11-chunk-retry` 或 `docs/M08-acceptance`。
2. 提交尽量聚焦一个变更，使用清晰的英文提交前缀，例如 `feat:`、`fix:`、`test:`、`docs:`。
3. 通过 Pull Request 合并到 `main`；描述改动、验证方法和关联模块。至少一名组员审查后再合并。
4. 修改状态流转、数据库结构或跨模块接口时，先同步更新需求/设计文档与 `docs/Interfaces.md`，并说明兼容或迁移方式。
5. 不提交个人数据、凭据、运行时数据库、传输文件、构建产物或本地环境配置。
6. 合并前按项目实际技术栈运行相关测试；验收功能需记录复现步骤和结果。

## 验收重点

- 接收方离线时提交任务，接收方上线后能看到待确认任务。
- 分别验证确认、拒绝和延迟路径。
- 传输中终止 worker 或模拟网络中断，恢复后从已确认分片继续。
- 两个客户端基于同一版本提交修改，后提交内容形成冲突副本且不覆盖 head。
- 入队后终止 daemon 并重启，未完成任务和审计记录仍可恢复和查询。

## 当前状态

- **线 A 骨架已就绪**：`src/common/`、`src/storage/`、`src/daemon/`、`db/schema.sql`、`db/migrations/`、`scripts/kill_daemon.sh`、`tests/contract/test_queue.py`。
- 契约测试现为 53 项通过（含 C-01～C-13 中 P1 已实现部分）。
- 验收场景 5（崩溃恢复）已实跑通过并留有记录：`tests/acceptance/crash_recovery.md`。
- 技术栈：Python 3.10+ 标准库（`sqlite3`），无第三方依赖；测试用 `unittest`（`pytest` 也能收集）。
- 线 B/C/D 的目录按模块认领创建；`users`、`project_spaces`、`space_members`、`files`、`file_versions`、`leases`、`audit_logs` 在 `db/schema.sql` 中为草案，评审后由线 A（M02）落库。

## 运行（线 A 骨架）

```bash
# 空目录一键建库（创建 data/db、data/tmp、data/objects、data/logs）
python3 -c "from src.storage.db import init_database; init_database('data/db/queue.db')"

# 契约测试：C-01 ~ C-05、C-09、C-10 与投递主流程
python3 -m unittest discover -s tests/contract -t .

# 启动 queue-daemon（--once 只跑一个调度周期）
python3 -m src.daemon.main --init-db --db data/db/queue.db --interval-ms 500

# 存量库升级（按 db/migrations/*.sql 顺序应用，可重复调用；迁移须写成幂等 DDL）
python3 -c "from src.storage.db import connect, apply_migrations; print(apply_migrations(connect('data/db/queue.db')))"

# 验收场景 5：崩溃恢复（起 daemon -> 传到 40% -> kill -9 -> 重启 -> 重新认领）
WORK_DIR=/tmp/crash-recovery-demo tests/acceptance/run_crash_recovery.sh

# 验收场景 5：优雅停止 vs 模拟崩溃
SIGNAL=-TERM scripts/kill_daemon.sh
scripts/kill_daemon.sh
```
