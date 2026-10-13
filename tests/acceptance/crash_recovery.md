# 验收场景 5：崩溃恢复

**负责人**：P1　**状态**：通过（2026-10-13 实跑记录）　**一键复现**：`tests/acceptance/run_crash_recovery.sh`

## 场景目标

对应需求 3.10 第 5 条：**任务入队后杀掉 queue-daemon 再启动，任务与已完成进度仍然存在、可以继续处理**。
验证的是「状态、分片进度、claim 全部落在 SQLite 里」这一 OS 课设核心点。

## 涉及文件

| 文件 | 作用 |
|---|---|
| `tests/acceptance/run_crash_recovery.sh` | 一键复现：启停 daemon、kill -9、断言、打印证据 |
| `tests/acceptance/crash_recovery_driver.py` | 驱动：入队、等待分流、确认、认领、ack 前 40%（P4 worker 未交付前由它扮演 worker） |
| `scripts/kill_daemon.sh` | 故障注入：默认 `SIGKILL`，可用 `SIGNAL=-TERM` 对比优雅停止 |
| `src/daemon/main.py` | 启动时执行一次 `recover()`，此后按 `--recover-every-ms` 周期扫描 |

## 复现步骤与命令

```bash
WORK_DIR=/tmp/crash-recovery-demo VISIBILITY_MS=3000 \
  tests/acceptance/run_crash_recovery.sh
```

1. 启动 `queue-daemon`（可见性超时 3000ms，便于观察超时回收）；
2. 入队一个 5 MiB 的 `delivery` 任务，daemon 把 `queued` 分流为 `waiting_online`；
   接收方上线确认后，worker 认领并 ack 前 2/5 片（约 40%）；
3. `scripts/kill_daemon.sh` 用 `SIGKILL` 杀掉 daemon（真实崩溃，不给清理机会）；
4. 直接查库：任务仍是 `transferring`，`acked_chunks=2`；
5. 重启 daemon，等可见性超时被 `recover()` 回收；
6. 查库：任务回到 `network_wait`、claim 被清除、已 ack 的分片数不变；随后可被重新认领。

## 实际运行结果（原文摘录）

```
== 1) 启动 queue-daemon（可见性超时 3000ms）
   daemon pid=15
== 2) 入队任务，并由 daemon 分流；接收方上线确认；worker 认领并 ack 40%
[prepare] 入队 task_id=t-ca535657 state=queued
[prepare] daemon 已分流 -> waiting_online
[prepare] 认领成功 worker=w-1 attempt=1 可见性超时=+3000ms
[prepare] 已 ack 2/5 片 (2097152/5242880 字节)
TASK_ID=t-ca535657
== 3) kill -9 queue-daemon（模拟崩溃，不是优雅停止）
[kill_daemon] 已向 pid 15 发送 -KILL
   15 Killed   python3 -m src.daemon.main --init-db --db ... --visibility-timeout-ms 3000
== 4) 崩溃后检查：任务与已 ack 进度必须仍在库中
{
  "task_id": "t-ca535657",
  "state": "transferring",
  "acked_chunks": 2,
  "total_chunks": 5,
  "acked_bytes": 2097152,
  "size_bytes": 5242880,
  "claims": [
    { "worker_id": "w-1", "attempt": 1, "visibility_deadline": 1791882743015 }
  ],
  "events": [
    "None--task.enqueued-->submitted(P1)",
    "submitted--task.transition-->queued(P1)",
    "queued--task.transition-->waiting_online(P1)",
    "waiting_online--task.transition-->waiting_confirm(P2)",
    "waiting_confirm--task.transition-->transferring(P2)",
    "None--chunk.acked-->transferring(P4)",
    "None--chunk.acked-->transferring(P4)"
  ]
}
[PASS] 崩溃后任务仍在 transferring，已 ack 的 2/5 分片未丢
== 5) 重启 daemon，等可见性超时被 recover 回收
   daemon 启动时执行了恢复扫描
== 6) 恢复后检查：应回到 network_wait，且可被重新认领
{
  "task_id": "t-ca535657",
  "state": "network_wait",
  "acked_chunks": 2,
  "total_chunks": 5,
  "claims": [],
  "events": [ ... , "transferring--task.transition-->network_wait(P1)" ]
}
[reclaim] 重新认领成功 task_id=t-ca535657 attempt=1
[PASS] 崩溃恢复完成：同一任务被重新认领，已 ack 的 2/5 分片保留
== 场景 5 全部通过
```

## 断言点（脚本里逐个检查，任一失败即 `[FAIL]` 退出）

| # | 断言 | 依据 |
|---|---|---|
| 1 | `kill -9` 之后任务仍在，且 `acked_chunks=2` | 崩溃不丢任务与进度 |
| 2 | 崩溃后状态仍是 `transferring`，claim 记录仍在 | claim 是持久化的，重启即可见 |
| 3 | daemon 重启时日志出现「启动恢复完成」 | 启动即执行 `recover()` |
| 4 | 可见性超时后状态变 `network_wait`、claim 被清空 | `recover()` 清理失效 claim 并重新入队 |
| 5 | 再次 `claim` 拿到**同一个** `task_id` | 未完成任务可被继续处理 |
| 6 | 恢复前后 `acked_chunks` 都等于 2 | 已 ack 的分片不会被重传覆盖 |

## 注意事项

- **可见性超时由认领方写入**：`visibility_deadline = claim 时的 now + 认领方配置`。所以 daemon 与 worker 必须用同一份
  `visibility_timeout_ms` 配置，否则会出现"任务比预期更早/更晚被回收"。真实实现里两者都取 P1 的配置项，天然一致；
  本脚本为了快速演示，把 daemon 与 driver 都设为 3000ms。
- **本场景由驱动脚本扮演 P4 的 worker**（真实 worker 尚未交付）。P4 的 worker 就绪后应把这个脚本换成
  "真 worker + kill_worker.sh" 重跑一次，作为场景 2（断点续传）的输入。
- **未覆盖**：网络中断（用 `net_break.sh` 模拟）、原子发布后的成品校验、daemon 在恢复过程中再次被杀。
  前两项属于 P4 的场景 2，第三项建议在集成周补一个「连续两次崩溃」的用例。

## 结论

场景 5 通过：`SIGKILL` 杀掉 daemon 后，任务、状态、分片进度与 claim 全部保留；重启后 daemon 执行恢复扫描，
在可见性超时后把任务送回 `network_wait` 并允许重新认领，已确认的分片没有被丢弃。
