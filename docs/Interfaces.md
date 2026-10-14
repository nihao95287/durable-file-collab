# 接口契约（contract-v1 草案）

本文档是 SyncBox 团队并行开发的**唯一接口依据**，配合 [模块划分与依赖](模块划分与依赖.md) 与 [模块认领与协作方案](../模块认领与协作方案.md) 使用。

## 0 使用说明

### 0.1 契约状态

| 项 | 值 |
|---|---|
| 契约版本 | `contract-v1`（草案，第 1 周结束冻结并打 git tag） |
| 冻结后变更 | 必须经「接口所有者 + 全部调用方」Review 后再合并；未打新 tag 前不得私自扩展签名 |
| 落地语言 | 与语言无关的逻辑契约。若选用 Python，命名可直接对应模块函数；若选其他语言，**语义、参数顺序、错误码保持不变** |
| 强制执行 | `tests/contract/` 按接口归属由各所有者维护（见第 7 节），CI 门禁由 P2 汇总；接口不匹配应在 CI 阶段失败 |

### 0.2 通用约定

- **时间**：一律使用 UTC 毫秒级整数时间戳（`int64`），字段名以 `_at` 结尾。
- **ID**：`task_id`、`user_id`、`space_id`、`file_id` 等为字符串，由创建方生成，全局唯一且不可变。
- **JSON**：所有 `payload`、`detail` 字段存储为 JSON 字符串；不得存放二进制内容。
- **金额/大小**：字节数使用整数，字段名以 `_bytes` 结尾。
- **错误处理**：所有接口失败时抛出结构化错误对象，至少包含 `code`、`message`、`retryable`；不得用裸字符串表示失败原因。
- **幂等**：文档标注「幂等」的接口，重复调用必须产生与首次调用等价的结果，且不得破坏已有进度。

### 0.3 统一错误码

| code | 含义 | retryable |
|---|---|---|
| `IllegalTransition` | 状态机非法流转 | 否 |
| `TaskNotFound` | `task_id` 不存在 | 否 |
| `ChunkChecksumMismatch` | 分片校验值不一致 | 是 |
| `ClaimInvalid` | claim 已过期或不属于该 worker | 是 |
| `RetryExhausted` | 重试次数超限 | 否 |
| `LeaseHeldByOther` | 文件已被其他持有者加锁 | 是 |
| `LeaseExpired` | 租约已过期 | 是 |
| `PermissionDenied` | 无空间权限或非任务相关方 | 否 |
| `Unauthorized` | 未登录或身份无效 | 否 |
| `InvalidArgument` | 参数缺失或格式错误 | 否 |
| `ObjectNotFound` | 对象路径或版本不存在 | 否 |

**注意**：版本不一致**不是错误**。`base_version != head_version` 是正常业务结果，返回成功并生成 conflict copy。

## 1 投递状态机契约（P1 拥有，全组遵守）

状态机是**唯一权威定义**，任何模块不得在文档之外新增状态或流转。

### 1.1 状态取值

| state | 含义 | 终态 |
|---|---|---|
| `submitted` | 已完成初始落库 | 否 |
| `queued` | 等待守护进程调度 | 否 |
| `waiting_online` | 等待接收方上线 | 否 |
| `waiting_confirm` | 等待接收方决定 | 否 |
| `delayed` | 已延迟，等待到期 | 否 |
| `transferring` | 有 worker 正在处理分片 | 否 |
| `network_wait` | 网络或 worker 暂不可用 | 否 |
| `completed` | 已校验并原子发布 | 是 |
| `rejected` | 接收方拒绝 | 是 |
| `failed` | 重试超限或最终校验失败 | 是 |

### 1.2 事件与合法流转

| 当前状态 | 事件 | 目标状态 | 触发方 |
|---|---|---|---|
| `submitted` | `enqueued` | `queued` | P1 |
| `queued` | `dispatch_offline` | `waiting_online` | P1 |
| `queued` | `dispatch_online` | `waiting_confirm` | P1 |
| `queued` | `dispatch_immediate` | `transferring` | P1（无需接收方确认的任务，如 `version_upload`） |
| `waiting_online` | `receiver_online` | `waiting_confirm` | P2 心跳 → `wake_user` |
| `waiting_confirm` | `confirm` | `transferring` | P2 决策接口 |
| `waiting_confirm` | `reject` | `rejected` | P2 决策接口 |
| `waiting_confirm` | `delay` | `delayed` | P2 决策接口 |
| `delayed` | `delay_expired` | `waiting_confirm` | P1 定时钩子 |
| `transferring` | `transfer_complete` | `completed` | P4 worker |
| `transferring` | `interrupted` | `network_wait` | P4 worker |
| `network_wait` | `recovered` | `transferring` | P1 调度 |
| `transferring` | `retry_exhausted` | `failed` | P1 |
| `network_wait` | `retry_exhausted` | `failed` | P1 |

### 1.3 状态机规则

1. 终态（`completed`、`rejected`、`failed`）不得被普通调度再次认领。
2. 每次流转必须在事务中完成，并写入状态变更事件（供审计与恢复使用）。
3. 非上表列出的流转一律抛 `IllegalTransition`，**调用方不得提前自行判断合法性**。
4. 状态、累计 offset、版本号、租约的更新必须在同一事务内保持一致。
5. `queued` 任务由 P1 的调度分流（`dispatch_offline` / `dispatch_online` / `dispatch_immediate`），**worker 不认领 `queued`**：这样「接收方确认」这一步不会被传输 worker 绕过。

## 2 QueueAPI（P1 拥有，P2/P3/P4 调用）

**契约要点**：调用方只传 `kind` 与 `payload`，P1 不解释业务字段含义；重试间隔、退避曲线、可见性超时长度均为 P1 配置项，其他人不得写死。

**P1 从 payload 提取的冗余列（仅这三个，其余字段原样存 JSON）**：`sender_id`、`receiver_id`（用于调度与 `wake_user`）、`size_bytes`（用于分片划分与进度展示）。三者可空，缺失时按 0 / 空处理。

### 2.1 入队

```text
enqueue_task(kind: str, payload: dict, run_after: int | None = None) -> str
```

| 参数 | 说明 |
|---|---|
| `kind` | 任务类型，取值见 2.7 |
| `payload` | 业务负载，由提交方定义结构，P1 原样存储 |
| `run_after` | 可选，最早可执行时间；为空表示立即 |

- 返回：`task_id`
- 语义：**单事务**写入任务主表、分片表、初始事件与状态变更记录；事务提交成功后才返回。事务失败时抛出错误，**绝不允许出现「返回成功但库中不存在」的任务**。
- 幂等性：否（重复调用创建两个任务）。需要去重的场景由调用方自行带上幂等键，放在 `payload` 中。
- 分片规则：按 P1 配置的固定分片大小切分（`chunk_size_bytes`，默认 1 MiB）；`size_bytes` 缺失或为 0 时按单个分片处理。
- 异常：`InvalidArgument`

### 2.2 认领

```text
claim(worker_id: str, now: int) -> Claim | None
```

返回 `None` 表示当前无可执行任务。`Claim` 结构：

```json
{
  "task_id": "t-1f3a",
  "kind": "delivery",
  "payload": "{...}",
  "state": "transferring",
  "visibility_deadline": 1760000000000,
  "attempt": 2
}
```

- 语义：在事务中选择可执行任务（`state` 属于 `transferring`、`network_wait`，且 `next_run_at <= now`，且无有效 claim），写入 `queue_claims` 并设置 `visibility_deadline`，同时把任务置为 `transferring`。
- **`queued` 不在认领范围内**：`queued` 必须先由 P1 的调度分流（见 1.2 规则 5）进入 `waiting_online` / `waiting_confirm` / `transferring`。
- **互斥保证**：并发调用 `claim` 时，同一任务在同一时刻只能被一个 worker 认领。
- `network_wait` 被认领时由 P1 按 `recovered` 事件流转回 `transferring`。

### 2.3 分片确认 / 否定

```text
ack_chunk(task_id: str, chunk_no: int, checksum: str) -> Progress
nack(task_id: str, chunk_no: int, reason: str, retryable: bool) -> None
```

`Progress` 结构：

```json
{ "acked_bytes": 4194304, "total_bytes": 10485760, "acked_chunks": 4, "total_chunks": 10 }
```

- `ack_chunk` 语义：校验值一致才写入 `acked_at` 并推进累计进度；**校验失败抛 `ChunkChecksumMismatch` 且不推进进度**。
- `ack_chunk` 幂等性：**是**。重复 ack 已确认分片按 `task_id + chunk_no + checksum` 幂等返回，不得重复累加或破坏文件。
- `task_chunks.checksum` 是**该分片的基准校验值**：入队时未知（payload 只有整文件校验值），由**首次 `ack_chunk` 写入**；之后重复 ack 的校验值必须与它一致，否则抛 `ChunkChecksumMismatch`。分片校验值的计算由传输 worker（P4）负责。
- `nack` 语义：写失败事件、`retry_count + 1`、按退避策略计算 `next_run_at`；`retryable = false` 或超过上限时把任务置为 `failed`，**先提交该状态再抛 `RetryExhausted`**（调用方必须捕获该异常，不得当成自身崩溃；见 2.3 异常清单）。
- `nack` 可重试路径：任务在 `transferring` 时记录为 `interrupted` -> `network_wait`，等 `next_run_at` 到期后可被重新认领。
- `nack` 异常：`ClaimInvalid`（claim 过期）、`TaskNotFound`

### 2.4 状态流转

```text
transition(task_id: str, event: str, expect_states: list[str], actor: str) -> str
```

- 返回：新的 `state`
- 语义：**唯一的状态变更入口**。校验当前状态在 `expect_states` 内且 `event` 在合法流转表中，成功后更新状态并写入状态变更事件。
- `expect_states` 采用乐观并发：当前状态不在列表内时抛 `IllegalTransition`，避免「读了状态再写」的竞态。
- 所有调用方（P2 的确认/拒绝/延迟、P4 worker 的完成/中断）都必须走此接口。

### 2.4b 延迟接收（补充入口）

```text
set_delay(task_id: str, delay_until: int, actor: str = "P2") -> str
```

- 语义：`waiting_confirm --delay--> delayed`，并在**同一事务**内把 `next_run_at` 设为 `delay_until`（`transition` 只改状态，表达不了「到期时间」）。
- 到期后由 `recover()` 用 `delay_expired` 流转回 `waiting_confirm`。
- 异常：`IllegalTransition`（当前状态不是 `waiting_confirm`）、`InvalidArgument`（`delay_until` 不是整数毫秒）。

### 2.5 心跳与唤醒

```text
heartbeat_claim(task_id: str, worker_id: str) -> bool
wake_user(user_id: str) -> int
```

- `heartbeat_claim`：续期 `visibility_deadline`；仅持有者可续，否则返回 `False`。
- `wake_user`：把该用户作为接收方、处于 `waiting_online` 的任务推进为 `waiting_confirm`，返回受影响任务数。由 P2 的心跳接口在**用户由离线转为在线**时调用。

### 2.6 恢复与定时钩子

```text
recover(now: int) -> RecoverReport
register_periodic_hook(name: str, interval_ms: int, fn: callable) -> None
query_view(name: str, filters: dict) -> list[dict]
```

- `recover`：扫描并清理失效 claim（未完成但无有效 claim 的任务、`visibility_deadline` 超时的 claim、到期延迟任务），重新入队。**必须可重复执行**，连跑两次结果一致；daemon 启动时执行一次，此后周期性执行。
- `RecoverReport`：`{ "requeued": 3, "claims_expired": 2, "delay_expired": 1 }`
- `register_periodic_hook`：供其他模块挂载定时任务，避免 daemon 反向依赖业务模块。P2 注册延迟到期检查，P3 注册租约过期回收。钩子执行异常不得中断 daemon 主循环（P1 负责捕获与记录）。
- `set_online_probe(fn)`：P2 在启动时注入「接收方是否在线」的查询（`users` 属于 P2，P1 不跨模块读表）。未注入时 P1 一律按离线分流，任务停在 `waiting_online`，等 P2 的心跳接口调用 `wake_user`。
- `query_view`：只读视图，供 P2 管理端使用，**无写入能力**。已定义的 `name` 取值：`tasks`、`queue`、`retry_stats`、`claims`。

### 2.7 任务类型取值

| kind | 提交方 | payload 关键字段 |
|---|---|---|
| `delivery` | P2 | `sender_id`、`receiver_id`、`file_name`、`source_path`、`size_bytes`、`checksum` |
| `version_upload` | P3 | `file_id`、`base_version`、`tmp_path`、`checksum`、`actor_id` |

新增 `kind` 需同时更新本表与 `tests/contract/`。

## 3 AuditAPI（P4 拥有，P1/P2/P3 调用）

```text
audit.write(event_type: str, actor_id: str, target_type: str, target_id: str, detail: dict) -> None
audit.join_tx(tx: object) -> AuditWriter
audit.query(filters: dict) -> list[dict]
```

- **只追加**：审计记录不得更新或删除；任何模块不得绕过本接口直接写 `audit_logs`。
- `join_tx`：返回绑定到调用方事务的写入器，用于把业务变更与审计记录放进**同一个事务**，保证不出现「业务成功但审计丢失」或反之。这是跨模块事务的唯一合法形态。
- P1 的写入方式：在状态流转、入队、分片 ack、重试超限处通过 `join_tx(tx)` 写入 `task.enqueued` / `task.transition` / `chunk.acked` / `task.retry_exhausted`；`task_events` 表继续保留（崩溃恢复的依据），审计表用于管理员查询。审计模块交付前 P1 接受注入 `None` 或 `NullAuditSink`（见 `src/common/audit.py`）。
- `event_type` 命名：`<模块>.<动作>`，小写点分。已定义取值：

| event_type | 写入方 |
|---|---|
| `task.enqueued`、`task.transition`、`chunk.acked`、`task.retry_exhausted` | P1 |
| `session.heartbeat`、`delivery.submitted`、`delivery.confirmed`、`delivery.rejected`、`delivery.delayed` | P2 |
| `version.created`、`version.conflict_copy`、`lease.acquired`、`lease.renewed`、`lease.released`、`lease.expired` | P3 |
| `object.published` | P4 |

- `detail`：JSON 字符串，存字段级事实（如 `{"from":"waiting_confirm","to":"transferring"}`），**不存文件内容、不存凭据**。
- `query(filters)`：支持 `actor_id`、`event_type`、`target_id`、`from_at`、`to_at`、`limit`、`cursor`。

## 4 ObjectStore（P4 拥有，P2/P3 调用）

```text
tmp_path(task_id: str) -> str
new_version_path(space_id: str, file_id: str, version_no: int, checksum: str) -> str
publish(tmp_path: str, final_path: str) -> None
```

| 项 | 约定 |
|---|---|
| 临时目录 | `data/tmp/{task_id}/{task_id}.part` |
| 正式对象目录 | `data/objects/{space_id}/{file_id}/v{version_no}__{checksum[:8]}.bin` |
| 发布方式 | **只有 `publish` 可以产生正式对象**，内部使用 `os.replace` 原子 rename |

- **禁止**调用方自行拼接正式对象路径或直接写入正式目录；P3 生成版本路径必须调用 `new_version_path()`，传输 worker 写分片必须调用 `tmp_path()`，两者都由 P4 实现。
- **禁止**接收方以正式文件名看到未完成文件：未完成期间只有临时文件存在。
- 路径中的 `space_id`、`file_id` 由系统生成并做白名单校验，**禁止使用用户输入直接拼接任意路径**。
- 同名不同内容不会互相覆盖（路径含校验值前缀）。

## 5 HTTP 接口

### 5.1 统一响应约定

成功：

```json
{ "ok": true, "data": { }, "request_id": "r-8a1" }
```

失败：

```json
{ "ok": false, "error": { "code": "IllegalTransition", "message": "任务当前状态不允许确认", "retryable": false }, "request_id": "r-8a1" }
```

HTTP 状态码：`200` 成功；`400` `InvalidArgument`；`401` `Unauthorized`；`403` `PermissionDenied`；`404` `TaskNotFound` / `ObjectNotFound`；`409` `IllegalTransition` / `LeaseHeldByOther` / `LeaseExpired`；`5xx` 内部错误。

**任何界面都不得直接修改数据库**，只能调用下列接口。

### 5.2 P2 负责

| 接口 | 输入 | 输出/效果 |
|---|---|---|
| `POST /deliveries` | `receiver_id`、`file`、`checksum` | 创建任务并返回 `task_id`、初始 `state` |
| `POST /deliveries/{id}/decision` | `decision`（`confirm`/`reject`/`delay`）、`delay_until` | 推进确认相关状态；`delay` 必填 `delay_until` |
| `GET /deliveries` | `user_id`、`state`、`page` | 返回任务、`acked_bytes`、`retry_count`、失败原因 |
| `POST /sessions/heartbeat` | `user_id` | 更新在线状态与 `last_seen`；**离线转在线时触发 `wake_user`** |

约定：`decision` 接口必须走 `QueueAPI.transition`，自行判断合法性属于违约。`GET /deliveries` 对非任务相关方返回 `PermissionDenied`。

### 5.3 P3 负责

| 接口 | 输入 | 输出/效果 |
|---|---|---|
| `POST /files/{id}/versions` | `base_version`、`object`、`checksum` | 生成新版本或冲突副本 |
| `GET /files/{id}/versions` | — | 版本历史与冲突副本列表 |
| `POST /files/{id}/lease` | `user_id` | 申请短租约，返回 `token`、`expires_at` |
| `POST /leases/{token}/renew` | `token` | 延长有效期；仅持有者可续 |
| `DELETE /leases/{token}` | `token` | 释放租约 |

`POST /files/{id}/versions` 响应必须显式区分结果：

```json
{ "result": "new_version", "version_no": 4, "head_version": 4 }
{ "result": "conflict_copy", "version_no": 4, "conflict_version_no": 5, "head_version": 4 }
```

### 5.4 P2 负责（管理端只读，底层数据来自 P1 与 P4）

| 接口 | 输入 | 输出 |
|---|---|---|
| `GET /admin/tasks` | `state`、`sender_id`、`receiver_id`、`page` | 任务列表与进度 |
| `GET /admin/queue` | — | 队列长度、认领详情、重试分布 |
| `GET /admin/audit-logs` | `actor_id`、`event_type`、`target_id`、`from_at`、`to_at`、`cursor` | 追加式审计日志 |

全部通过 `QueueAPI.query_view` 与 `audit.query` 读取，不新增写入口。

## 6 数据表归属（一表一主）

DDL 与迁移脚本只能由 M02（线 A）提交；其他成员以字段需求的形式提出变更。

| 表 | 所属模块 | 所有者 | 允许写入方 |
|---|---|---|---|
| `delivery_tasks`、`task_chunks`、`queue_claims`、`task_events` | M02 | 线 A | 仅线 A |
| `users` | M05 | 线 B | 仅线 B |
| `project_spaces`、`space_members` | M07 | 线 C | 仅线 C |
| `files`、`file_versions` | M08 | 线 C | 仅线 C |
| `leases` | M09 | 线 C | 仅线 C |
| `audit_logs` | M04 | 线 D | 仅线 D（其他线通过 `audit.write`） |

跨模块需要同事务时，只允许通过 `audit.join_tx(tx)` 或 `QueueAPI` 的显式入参传递事务句柄，**禁止 A 模块开事务直接写 C 表**。

受控目录归属：目录结构由 P1 在初始化时创建；`data/tmp`、`data/objects` 内的文件由 P4 独家写入（P3 通过 `objectstore.publish` 发布正式对象），`data/db`、`data/logs` 由 P1 维护；其他人不得直接读写这些文件。

## 7 契约测试清单（按接口归属维护）

`tests/contract/` 至少覆盖以下条目，任何一条失败即拒绝合并。用例按模块拆分：`test_queue.py`（M02/M03）、`test_http.py`（M05/M06/M12）、`test_collab.py`（M07/M08/M09）、`test_objectstore.py`（M10/M11），公共基座与 CI 门禁由线 B 汇总。

| 编号 | 检查内容 | 维护人 |
|---|---|---|
| C-01 | `QueueAPI` 全部函数签名、参数顺序与返回结构符合第 2 节 | P1 |
| C-02 | 非法流转（如 `completed` 上执行 `confirm`）抛 `IllegalTransition` | P1 |
| C-03 | 并发 `claim` 同一任务时只有一个成功 | P1 |
| C-04 | 重复 `ack_chunk` 不重复累加进度 | P1 |
| C-05 | `recover` 连续执行两次结果一致 | P1 |
| C-06 | `AuditAPI` 三个函数的签名与 `join_tx` 同事务语义 | P4 |
| C-07 | `ObjectStore` 路径格式与 `publish` 原子性 | P4 |
| C-08 | 全部 HTTP 接口的路径、字段名、错误码符合第 5 节 | P2 |
| C-09 | `kind` 与 `event_type` 取值均在已定义枚举内 | P1、P4 |
| C-10 | 每张表只出现一个写入方（静态检查 SQL 归属） | P1 |
| C-11 | `queued` 任务经 `dispatch_offline`/`dispatch_online`/`dispatch_immediate` 分流后，`claim` 只认领 `transferring`/`network_wait` | P1 |
| C-12 | P1 的审计事件通过 `join_tx` 与业务变更同事务（回滚后审计记录一并消失） | P1、P4 |
| C-13 | 迁移文件（`db/migrations/*.sql`）与 `db/schema.sql` 的结构一致，且可重复应用 | P1 |

## 8 变更记录

| 版本 | 日期 | 变更 | 参与人 |
|---|---|---|---|
| contract-v1（草案） | — | 初稿，待第 1 周评审后冻结并打 tag | 全组 |
| contract-v1（草案）修订 | 2026-10-13 | `ObjectStore` 归属由 P2 调整为 P4，管理端只读接口归属由 P4 调整为 P2，传输语义明确归 P4；契约测试清单增加维护人列 | 全组 |
| contract-v1 修订提案 | 2026-10-13 | ①`queued` 不再可被 `claim`，新增 `dispatch_immediate` 流转（无需确认的任务）；②`nack` 超限「先提交 failed 再抛 `RetryExhausted`」；③新增 `set_delay` 与 `set_online_probe`；④明确 payload 冗余列、分片规则与 `task_chunks.checksum` 语义；⑤P1 通过 `join_tx` 写审计 | 待 P2、P4 确认 |
| 模块化改版 | 2026-10-14 | 项目更名为 SyncBox；表与用例归属由 P1～P4 改为模块 ID（`M01`～`M15`）与四条工作线 A～D；配套《模块划分与依赖》《模块认领与协作方案》 | 全组 |
