# Skein v5：表结构

日期：2026-09-06。状态：**表结构已定稿**（§2 ~ §5），流程、故障恢复、接口、实施步骤按定稿表编写（§6 ~ §10）。M1 ~ M6 已实现；M7 真实负载与多实例验收见 §11，smoke 已实现，full / soak 待实现，验证记录见 docs/scenarios.md；M8 两节点外部任务与纯 Snooze 已实现并通过 PG 13 / 18 验收（§12）；§6 / §8 已同步。

2026-09-06 评审修订：节点取消改由父行状态表达，不再对 running 节点写 `cancel_requested`（§6.5 / §6.6 / §7.3）；结算的非终态出口统一先看取消（§6.5）；Shutdown 不再拒绝 Trigger（§6.8）；定时触发用 DB 时钟（§6.2）；领取分支二重领时记 `interrupted`（§6.3）；`schedule` 的 FK 改 RESTRICT（§3）；列表索引与游标改用 `id`（§3 / §8）。2026-09-07：角色开关改名 `DisableWorker` / `DisableScheduler`，零值即启用（§1 / §8）；`ScheduleSpec.Enabled` 同理改为 `Disabled`（§8）；清理改用会话级 advisory lock，每步独立短事务（§6.10）。2026-09-07 评审修订二：清理的 DELETE 外层重判终态条件（§6.10 / §7.3）；fail-fast 先取消再读节点（§6.5）；普通实例取消改为一条 UPDATE（§6.6）；无下一触发点的计划被拒绝或禁用（§6.2）；本地心跳期限只由心跳刷新、`active` 按 lease_token 记、领取后检查有界并在进入 Executor 前看停机（§6.4 / §6.8）；配置取值范围与 `retry_policy` 范围校验、`MaxPayload` 语义、`Metrics` 并发契约、`exec_duration` 标签取落库状态（§8）。2026-09-07 评审修订三：停止领取与 Executor 登记共用 `inflight` 互斥锁，lease-loss 的移除与结果丢弃标记原子发布（§6.4 / §6.8）；未启动节点取消先以 `SKIP LOCKED` 锁定候选，被跳过的 pending 由领取后检查收敛，锁序证明以不等待候选行为依据（§6.3 / §6.5 / §6.6 / §7.3，同步 §2 / §5 概述）。2026-09-07 交付补充：README 使用入口、MIT 许可证（署名 Jaken）、Go 1.27 与 PG 13/18 的 GitHub Actions 矩阵；CI 验证数据库连接并禁止测试缓存（§8.2）。2026-09-07 源码组织补充：保持包边界与执行职责，README 增加阅读导航，里程碑测试文件改用行为名称（§8.3）。2026-09-07 评审修订四：search_path 显式列出 pg_temp（§8）；cron 拒绝内嵌时区前缀、`@every` 与 `Local` 时区（§6.1 / §8）；jsonb 拒绝的输出按不可重试失败（§6.5）；分支二重领计数封顶（§6.3）；清理批次跟随停止信号、解锁有界（§6.8 / §6.10）；GetRun 单语句读取、timeout 上界与 NaN jitter 校验（§8）；口径修正：§6.2 日与星期 OR 语义、§7 结算不重试、§8.1 HOT 告警按心跳口径、§9 M5 实测边界。2026-09-07 评审修订五：TriggerTx 恢复 search_path 用独立有界 ctx，恢复失败单独返回（§8）；停机第 6 步取消在途心跳（§6.8）；回滚、恢复、解锁共用 5s 收尾期限（§6.10）；`hot_update_ratio` 改为趋势口径（§8.1）。2026-09-07 评审修订六（精准触发）：领取与定时扫描改为事件驱动，轮询降为兜底并把默认周期放宽到 5s（§1 / §6 / §8）；`job_run` 与 `schedule` 加唤醒触发器，NOTIFY 只做提前唤醒（§3 / §6.11）；定时扫描睡到最近的 `next_run_at`，领取睡到最近的 `run_at`、槽位释放即再领（§6.2 / §6.3）；停机同时关闭监听连接（§6.8 / §6.9）；故障窗口与锁序补监听与触发器（§7.1 / §7.3 / §7.4）；新增 `listener_reconnect_total`（§8.1）；新增 M6 验收（§9）；LISTEN/NOTIFY 从 §10 移出。2026-09-07 评审修订七：errors 条目先规范化再落库、任何结算的 class 22 都按不可重试失败（§6.5）；到点读取含已到期行、`db_now` 取 `clock_timestamp()` 并扣本地耗时（§6.2 / §6.3 / §6.11）；后到的 Shutdown 调用等待完成或自己的 ctx（§6.8 / §8）；超长 `executor_type` 退化为广播唤醒（§3 / §6.11）；列表多读一行判断尾页（§8）；M6 验收改为 Executor 入口时刻 ≤ 300ms（§9）；口径修正：DB 时钟前跳才提前过期（§7.4）、收尾期限各自 5s（§6.10）。2026-09-08 评审修订八：去重撞键后查不到在途 run 则重跑 INSERT，最多 3 轮（§6.1 / §8）；名字 ≤ 255 字节（§8）；部分填写的重试策略按原样校验（§8）；领取的到点读取放到本轮末尾并扣除耗时（§6.3）；`Stats` 无注册类型时传空数组（§8）；日志的 instance 由 Logger 统一携带。2026-09-08 验收设计补充：新增 M7 真实负载与多实例验收，明确混合任务、故障注入、逐次执行证据、计时口径、容量分档与通过条件（§9 / §11）；不改变 §3 DDL、§6 事务协议、§7.3 锁序或 §8 公共 API。2026-09-08 M7 smoke 实现口径补充：cron 精度以 scheduled_at 覆盖扫描耗时；使用 Go 原生 -artifacts 保存制品（§11）。2026-09-08 M7 smoke 交付：独立保存并核对预期 At / cron 拍次，增加坏证据负例及共享预算下的并发清理；更新运行入口，完整负载与 soak 仍待实现（§9 / §11）。2026-09-08 Snooze 方案确认：采用 submit → poll 两节点与纯 Snooze，task id 和固定 deadline 保存在 submit 的成功 output，不引入运行中 checkpoint、不改变 Resume 语义；补充 M8 待实施设计与验收（§9 / §10 / §12），开始实施前另行确认并同步 §6 / §8，§3 DDL 与 §7.3 锁序不变。2026-09-08 M8 实施确认：同步纯 Snooze 的结算、唤醒、Attempt 计数及指标标签（§4 / §5 / §6.5 / §6.11 / §8 / §12）；复用两节点的成功 output 与既有 Resume，DDL 与锁序不变。2026-09-08 M8 兼容性补充：Snooze 使用 pgx 原生 interval 参数，在 SQL 中补足最多 1µs；避免 PG 13 大微秒数字面量越界和 Go Duration 向上取整溢出（§6.5 / §12.2）。2026-09-08 M8 交付：纯 Snooze、两节点示例、崩溃 / 取消 / fence / 期限验收落地；PG 13.23 与 18.4 均通过 test-ci 和三轮 race，lint 通过，更新完成状态与验收映射（§9 / §12）。2026-09-08 全量审查修订：空输出统一写 SQL NULL（§6.5）；启动查库移出生命周期锁并随停机取消，停机后不得补启动循环（§6.8 / §8）；PG 13 性能基线缺失的 active_time 标为 N/A（§9），不改变 DDL 或锁序。2026-09-08 M7 Snooze 补充：原 smoke 批次不变，追加普通 Snooze、全槽位复用屏障与 submit → poll → verify 工作流，保存 pending 快照并独立核对等待期限（§11）；不改生产协议或 DDL。

---

## 1. 整体架构

Go 库 + 一个共享 PostgreSQL。业务进程注册执行器、声明定义、启动 Engine；多个业务进程同时运行 Engine，共享同一批表。没有独立的调度服务、没有选主、没有 Redis 或 MQ。

```
宿主进程 A（嵌入 Engine）                              宿主进程 B（嵌入 Engine）
┌───────────────────────────────────────┐             ┌───────────────────────────┐
│ Register(executor_type, fn)            │             │ 同左；注册的类型可以不同     │
│ Jobs / Workflows / Schedules 声明       │             │                           │
│ Trigger / Cancel / Resume（同步事务）    │             │                           │
│                                        │             │                           │
│ ① Claimer    唤醒或到点：两支领取 → 执行池 │             │                           │
│ ② Scheduler  唤醒或到点：扫描计划 → 建 run │             │                           │
│ ③ Heartbeat  每 15s：一条 SQL 续全部租约  │             │                           │
│ ④ Maintainer 每 1h：advisory lock 单实例 │             │                           │
│ ⑤ Listener   专用连接 LISTEN → 唤醒 ①②   │             │                           │
│   轮询 5s 只兜底：断线、他人改计划、僵尸  │             │                           │
└─────────────────┬──────────────────────┘             └─────────────┬─────────────┘
                  │ pgxpool（毫秒级短事务；执行中不占连接）                │
                  ▼                                                    ▼
      PostgreSQL，schema `skein`：job · workflow · workflow_node · schedule │ job_run · workflow_run
                                    定义（强约束）                          实例（弱引用 + 快照）
```

五条原则，后面每一章都是它们的展开：

| 原则 | 含义 |
|---|---|
| 实例对等 | 每个实例默认运行全部循环，靠行锁与 `SKIP LOCKED` 分摊工作；实例只领自己注册过的 `executor_type`，滚动发布时新旧实例可以共存。`DisableWorker` / `DisableScheduler` 两个开关可以让 API 进程只提交不执行，协议不变 |
| 状态全在 PG | 进程内不持久化任何东西；重启不需要恢复动作，过期租约由领取自然回收 |
| 定义 / 计划 / 实例三层 | job 回答"什么"，workflow 回答"顺序"，schedule 回答"何时"，run 是一次意图。实例创建时快照定义，之后不回读 |
| 实例行就是队列项 | `job_run` 既是事实记录也是队列与租约；工作流节点实例也是它。一条领取路径、一个结算函数 |
| 三个循环，没有第四个 | 没有僵尸回收循环（回收 = 领取分支二），没有工作流推进循环（推进在结算事务内），没有超时扫描（ctx deadline + 租约）。监听不是循环：它不扫表，只把提交事件转成对领取与扫描的唤醒（§6.11）；轮询降为兜底，触发精度不依赖它 |

一次执行的生命周期：

```
Trigger ──► [事务] INSERT job_run(state=pending, run_at=期望时刻)；工作流则再加 workflow_run + N 个节点行；提交时触发器 NOTIFY 唤醒各实例的领取（§6.11）
Claim   ──► [事务] SKIP LOCKED 选到期的 pending 或租约过期的 running → running, lease_token=新 uuid, lease_expires_at=now()+60s
Exec    ──► Executor(ctx, req)，ctx 带 timeout；Heartbeat 每 15s 推后 lease_expires_at，并带回取消信号
Settle  ──► [事务] (节点：锁父行) → UPDATE … WHERE lease_token=? 做 fence → 成功 / 退避重排 / 终态
                    → (节点：解锁后继、fail-fast、收敛父行)
```

部署上只有两个数字要按任务特征调：`LeaseTTL`（默认 60s，决定崩溃接管等待）和 `ShutdownGrace`（默认 30s，长任务设为 p99 时长）。连接数 = 池大小加一条监听连接，不随并发增长。

---

## 2. 模型

| 层 | 表 | 回答 | 明确不放 |
|---|---|---|---|
| 任务定义 | `job` | 什么、怎么跑：executor、params 模板、超时、重试 | cron、next_run_at、enabled |
| 工作流定义 | `workflow` + `workflow_node` | 哪些 job、什么顺序 | 执行语义（节点引用 job，不自带 executor / params） |
| 执行计划 | `schedule` | 何时：cron、时区、开关、重叠策略、下一触发点；目标是 job 或 workflow，恰好一个 | 参数覆盖 |
| 任务实例 | `job_run` | 一次执行意图：定义快照 + 状态 + 租约 + 结果。**工作流的节点实例就是 job_run**，多一个归属列 `workflow_run_id` 和一个 `blocked` 态 | 回读定义 |
| 工作流实例 | `workflow_run` | 一次工作流执行：输入、DAG 快照、状态、时间 | 执行细节（全在节点 job_run 上） |

关系规则：

1. **一次性 / 延迟任务不是计划**，是一条 `run_at` 在未来的 run。`schedule` 只表示周期规则。
2. **run 自身就是队列项**：pending 行靠 `run_at` 排队，running 行靠 `lease_expires_at` 判僵尸，两者各走一个部分索引；心跳只写 `lease_expires_at`，它不进任何索引，因此是 HOT 更新。节点实例与普通实例共用同一条领取 SQL、同一个结算函数。
3. **定义之间强约束，定义与实例之间弱引用**：`schedule → job / workflow`、`workflow_node → job` 有 FK；`job_run / workflow_run → 定义` 只存名字，历史在定义删除后仍在。被节点或计划引用的 job、被计划引用的 workflow 不能删（FK RESTRICT）。
4. **节点以 job 标识**：一个 job 在一个工作流里最多出现一次；同一步骤要跑两次，声明两个 job（与"不同参数就声明两个 job"同一规则）。
5. **创建实例时快照，之后不读定义**：job_run 复制 job 的 executor / params / timeout / retry_policy；workflow_run 把整张图快照进 `dag`，图属于工作流实例，不拆散存进节点行；定义改了删了不影响在途。因此 job 和 workflow 都不做版本。
6. **推进与收敛在 workflow_run 行锁下完成**：节点成功 → deps 全部成功的 blocked 节点变 pending；任一节点最终失败 → 工作流 fail-fast 进入 `cancelling`，可立即加锁的未开始节点直接 cancelled，正在领取的节点跳过并经父状态检查取消；在跑节点不写，持有者经心跳看到父行 `cancelling` 后自行结算为 cancelled；全部节点终态后收敛。加锁顺序全系统唯一：`workflow_run → job_run`。
7. **一个 run 多次 attempt 用计数器**，不建 attempts 表。`output` 只在成功时写一次；`errors` 每次失败追加一条。

---

## 3. DDL

所有对象放在独立 PostgreSQL schema 里（默认 `skein`，可配置），表名不带前缀。宿主通过限定名或 `search_path` 访问。要求 PostgreSQL ≥ 13。

```sql
CREATE SCHEMA IF NOT EXISTS skein;
SET search_path TO skein;

-- 迁移版本：Start 时校验，不匹配即拒绝启动
CREATE TABLE schema_version (
    version    integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

-- ───────────── 定义 ─────────────

-- 任务定义：回答"什么、怎么跑"
CREATE TABLE job (
    name          text PRIMARY KEY,                            -- 名字即身份
    executor_type text NOT NULL,                               -- 进程内注册的执行器类型
    params        jsonb NOT NULL DEFAULT '{}',                 -- 参数模板
    timeout       integer NOT NULL CHECK (timeout > 0),        -- 单次执行超时，秒
    retry_policy  jsonb NOT NULL DEFAULT '{"max_attempts":3}', -- max_attempts 必填；base_sec / max_sec / jitter 可选，缺省用引擎默认退避
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CHECK (jsonb_typeof(params) = 'object'),
    CHECK (jsonb_typeof(retry_policy) = 'object' AND coalesce((retry_policy->>'max_attempts')::int, 0) >= 1)
);

-- 工作流定义：回答"哪些 job、什么顺序"
CREATE TABLE workflow (
    name       text PRIMARY KEY,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE workflow_node (
    workflow_name text NOT NULL REFERENCES workflow(name) ON DELETE CASCADE,
    job_name      text NOT NULL REFERENCES job(name) ON DELETE RESTRICT,   -- 节点以 job 标识；被引用的 job 不能删
    deps          text[] NOT NULL DEFAULT '{}',                            -- 直接前驱的 job_name，边就是它；存在性与无环在保存时校验
    PRIMARY KEY (workflow_name, job_name)                                  -- 一个 job 在一个工作流里最多出现一次
);
CREATE INDEX idx_workflow_node_job ON workflow_node (job_name);           -- FK 索引，兼答"哪些工作流用了这个 job"

-- 执行计划：只回答"何时"
CREATE TABLE schedule (
    name          text PRIMARY KEY,
    job_name      text REFERENCES job(name)      ON DELETE RESTRICT,  -- 被计划引用的定义不能删，与 workflow_node 一致
    workflow_name text REFERENCES workflow(name) ON DELETE RESTRICT,
    cron          text NOT NULL,                               -- 标准 5 字段
    timezone      text NOT NULL DEFAULT 'UTC',                 -- IANA 名称
    overlap       text NOT NULL DEFAULT 'skip' CHECK (overlap IN ('skip','allow')),  -- 上一拍未结束时：跳过 / 照常
    enabled       boolean NOT NULL DEFAULT true,
    next_run_at   timestamptz NOT NULL,                        -- 下一逻辑触发点；唯一的热列，触发只写它
    updated_at    timestamptz NOT NULL DEFAULT now(),
    CHECK ((job_name IS NULL) <> (workflow_name IS NULL))      -- 恰好一个目标
);
CREATE INDEX idx_schedule_due ON schedule (next_run_at)   WHERE enabled;
CREATE INDEX idx_schedule_job ON schedule (job_name)      WHERE job_name IS NOT NULL;       -- FK 索引，删 job 时校验引用用
CREATE INDEX idx_schedule_wf  ON schedule (workflow_name) WHERE workflow_name IS NOT NULL;  -- FK 索引

-- ───────────── 实例 ─────────────

-- 工作流实例
CREATE TABLE workflow_run (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workflow_name text NOT NULL,                               -- 弱引用
    schedule_name text,                                        -- 非空 = 定时触发
    scheduled_at  timestamptz,
    dedup_key     text,                                        -- 在途唯一键；overlap = skip 时为 'sched:<schedule_name>'
    input         jsonb NOT NULL DEFAULT '{}',                 -- 触发时传入，所有节点可读
    dag           jsonb NOT NULL DEFAULT '{}',                 -- {job_name: [dep_job_name, ...]}，创建时从 workflow_node 快照；推进、续跑、取前驱 output 都读它
    state         text NOT NULL CHECK (state IN ('running','cancelling','succeeded','failed','cancelled')),
    created_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    CHECK ((state IN ('succeeded','failed','cancelled')) = (finished_at IS NOT NULL)),
    CHECK ((schedule_name IS NULL) = (scheduled_at IS NULL)),
    CHECK (jsonb_typeof(input) = 'object' AND jsonb_typeof(dag) = 'object')
);
CREATE UNIQUE INDEX idx_workflow_run_dedup     ON workflow_run (workflow_name, dedup_key)   WHERE dedup_key IS NOT NULL AND state IN ('running','cancelling');
CREATE UNIQUE INDEX idx_workflow_run_sched     ON workflow_run (schedule_name, scheduled_at) WHERE schedule_name IS NOT NULL;
CREATE INDEX        idx_workflow_run_wf        ON workflow_run (workflow_name, id DESC);         -- 列表按 id 排序、游标翻页
CREATE INDEX        idx_workflow_run_retention ON workflow_run (finished_at)                 WHERE finished_at IS NOT NULL;

-- 任务实例：一次执行意图。同一行兼任队列项、租约记录、定义快照、结果记录；工作流节点实例也是这张表
CREATE TABLE job_run (
    -- 身份与来源
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_name         text NOT NULL,                            -- 弱引用；节点实例也以它标识节点
    schedule_name    text,                                     -- 非空 = 定时触发（节点实例为空，计划信息在父 workflow_run 上）
    scheduled_at     timestamptz,
    dedup_key        text,                                     -- 在途唯一键；overlap = skip 时为 'sched:<schedule_name>'
    workflow_run_id  bigint REFERENCES workflow_run(id) ON DELETE CASCADE,  -- 非空 = 工作流节点实例；前驱在父行 dag 里
    -- 执行快照：创建时从 job 复制，params 已合并调用方覆盖；之后不读定义
    executor_type    text NOT NULL,
    params           jsonb NOT NULL,
    timeout          integer NOT NULL,                         -- 秒
    retry_policy     jsonb NOT NULL,
    -- 状态
    state            text NOT NULL CHECK (state IN ('blocked','pending','running','succeeded','failed','cancelled')),
    attempt          smallint NOT NULL DEFAULT 0,              -- 已以失败 / 中断结束的启动次数；续跑归零
    cancel_requested boolean NOT NULL DEFAULT false,           -- 普通实例的取消请求：pending 直接终态；running 经心跳回传，持有者结算为 cancelled。节点实例不用它，取消看父行 state
    -- 排队与租约
    run_at           timestamptz NOT NULL DEFAULT now(),       -- pending 时的可领取时刻（延迟执行 / 重试退避）
    lease_expires_at timestamptz,                              -- running 时的租约到期时刻；心跳只写它，不进任何索引 → HOT 更新
    lease_token      uuid,                                     -- 每次领取新生成；心跳 / 结算 / 释放的 fence
    lease_owner      text,                                     -- 实例标识，仅诊断
    -- 时间
    created_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,                              -- 最近一次启动
    finished_at      timestamptz,
    -- 结果
    output           jsonb,                                    -- 成功时写一次
    errors           jsonb NOT NULL DEFAULT '[]',              -- 每次失败追加 {attempt, at, kind, message}
    CHECK ((state IN ('succeeded','failed','cancelled')) = (finished_at IS NOT NULL)),
    CHECK ((state = 'running') = (lease_token IS NOT NULL)),
    CHECK ((lease_token IS NULL) = (lease_owner IS NULL) AND (lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((schedule_name IS NULL) = (scheduled_at IS NULL)),
    CHECK (state <> 'blocked' OR workflow_run_id IS NOT NULL), -- 只有节点实例会 blocked
    CHECK (jsonb_typeof(params) = 'object' AND jsonb_typeof(retry_policy) = 'object' AND jsonb_typeof(errors) = 'array')
);
CREATE INDEX        idx_job_run_claim     ON job_run (executor_type, run_at)        WHERE state = 'pending';   -- 到期领取
CREATE INDEX        idx_job_run_running   ON job_run (executor_type)                WHERE state = 'running';   -- 僵尸扫描；running 行很少，lease_expires_at 在堆上过滤
CREATE UNIQUE INDEX idx_job_run_dedup     ON job_run (job_name, dedup_key)          WHERE dedup_key IS NOT NULL AND state IN ('blocked','pending','running');
CREATE UNIQUE INDEX idx_job_run_sched     ON job_run (schedule_name, scheduled_at)  WHERE schedule_name IS NOT NULL;
CREATE UNIQUE INDEX idx_job_run_node      ON job_run (workflow_run_id, job_name)    WHERE workflow_run_id IS NOT NULL;  -- FK 索引，兼作"载入本工作流全部节点"
CREATE INDEX        idx_job_run_job       ON job_run (job_name, id DESC);               -- 列表按 id 排序、游标翻页
CREATE INDEX        idx_job_run_retention ON job_run (finished_at)                  WHERE finished_at IS NOT NULL AND workflow_run_id IS NULL;  -- 节点实例随父级联删，不进此索引
-- 心跳要成为 HOT 更新，除了 lease_expires_at 不进索引，还要新版本放得进同一页：
--   toast_tuple_target = 256：超过 256B 的行把 params / errors / output 压缩或移出堆（TOAST），主行缩到 ~250B，心跳不碰 TOAST 指针
--   fillfactor = 70：每页留 ~2.4KB 给同页多行并发心跳的版本链，页内 HOT 剪枝随访问回收，不等 vacuum
--   vacuum / analyze 阈值降到 100：小库也会收；不 analyze 领取计划会钝
ALTER TABLE job_run SET (
    fillfactor = 70,
    toast_tuple_target = 256,
    autovacuum_vacuum_scale_factor = 0.02,  autovacuum_vacuum_threshold = 100,
    autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold = 100
);

-- ───────────── 唤醒（§6.11） ─────────────
-- pending 行出现（含 run_at 变化）或计划规则变化时通知本 schema 的监听者。通知在提交后投递，随事务回滚作废；
-- 频道名 = schema 名，payload 区分领取与扫描。心跳只写 lease_expires_at，不在 OF 列表里，触发器不评估；
-- 领取把 state 改成 running，WHEN 为假；AdvanceSchedule 只写 next_run_at，不通知
CREATE FUNCTION notify_wake() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME = 'job_run' THEN
        -- a payload must stay under 8000 bytes; an absurdly long executor_type falls back to the broadcast wake ('')
        PERFORM pg_notify(TG_TABLE_SCHEMA, CASE WHEN octet_length(NEW.executor_type) < 7000 THEN 'run:' || NEW.executor_type ELSE '' END);
    ELSE
        PERFORM pg_notify(TG_TABLE_SCHEMA, 'schedule');
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER job_run_wake  AFTER INSERT OR UPDATE OF state, run_at ON job_run
    FOR EACH ROW WHEN (NEW.state = 'pending') EXECUTE FUNCTION notify_wake();
CREATE TRIGGER schedule_wake AFTER INSERT OR UPDATE OF cron, timezone, enabled OR DELETE ON schedule
    FOR EACH ROW EXECUTE FUNCTION notify_wake();
```

列数：`job` 6、`workflow` 2、`workflow_node` 3、`schedule` 9、`workflow_run` 10、`job_run` 22。共 7 张表（含 `schema_version`），外加一个触发器函数 `notify_wake` 与两个触发器（§6.11）。

---

## 4. 列的读方

| 列 | 读方 |
|---|---|
| `job_run.params / timeout / retry_policy` | 领取事务一条 `RETURNING` 交给 Worker。快照而非冗余：job 改了或删了，在途 run 的语义不变；run → job 因此不需要 FK |
| `run_at` | 只对 pending 行有意义：延迟执行、重试退避与 Snooze 的可领取时刻。领取分支一：`state = 'pending' AND run_at <= now()`，走 `idx_job_run_claim` |
| `lease_expires_at` | 只对 running 行有意义。领取分支二：`state = 'running' AND lease_expires_at <= now()`，走 `idx_job_run_running` 找到 running 行后在堆上过滤；僵尸回收就是这条分支。两支是两条独立语句，各自 `FOR UPDATE SKIP LOCKED`，第二支只在第一支没领满时执行（`FOR UPDATE` 不能作用于 `UNION`）。心跳只写它；它不在任何索引里，配合 `toast_tuple_target = 256` 与 `fillfactor = 70`，心跳是 HOT 更新，不写索引、死元组页内剪枝回收。续期封顶在 `started_at + timeout + CancelTimeout`，Executor 忽略 ctx 也占不住 |
| `lease_token` | 每次领取新生成的随机 uuid，本行当前合法持有者的凭证。心跳、结算、释放全部 `WHERE lease_token = 我的`；0 行即租约已失。与选主无关，系统没有 leader |
| `lease_owner` | 不进任何 WHERE，排障时回答"这条卡住的 run 在哪个实例手上" |
| `cancel_requested` | 普通实例取消 running 的唯一通道。心跳 `RETURNING` 带回 Worker → 取消 ctx → 结算 cancelled；持有者已崩溃则重领方看到标记不执行直接结算 cancelled。节点实例不用它：节点的取消由父行 `state = 'cancelling'` 表达，心跳 `RETURNING` 的 `wf_cancelling` 与领取后读父行两条路都能看到；这样除心跳外没有事务一次锁多条 running 行（§7.3） |
| `executor_type` 在两个领取索引里 | 实例只领自己注册过的类型，滚动发布时新类型不被旧实例领走 |
| `dedup_key` 部分唯一索引 | 在途即占位、终态自动释放；`overlap = skip` 复用它，触发事务里 `ON CONFLICT DO NOTHING` 就是跳过。节点实例不带 dedup_key，去重在 workflow_run 上 |
| `(schedule_name, scheduled_at)` 唯一 | 两实例同时扫到同一计划的兜底；正常靠 `FOR UPDATE SKIP LOCKED` 已互斥 |
| `attempt` | 失败结算 +1、租约过期重领 +1、优雅释放与 Snooze 不加；`attempt >= max_attempts` 即终止，崩溃循环也有上限 |
| `errors` | 逐次"这次启动为什么没跑完"，`kind` 区分 business / timeout / interrupted / panic / upstream_failed / released；结算时 `errors = errors \|\| $1::jsonb` 追加；`interrupted` 条目在领取分支二重领时追加，崩溃过又成功的 run 也留痕。`released` 条目 ≥ `ReleaseAlertThreshold`（3）即告警：任务被滚动发布反复打断却永不失败，没有这条就永不报警（v2 FR-9.8） |
| `started_at` | 当前这次跑了多久 |
| `created_at / finished_at` | 展示、保留清理、终态 CHECK；列表排序与游标用 `id`，不用 `created_at`（并发插入下两者顺序可能不一致）。清理按终态分两个窗口（成功 7d、失败 30d），`(finished_at)` 单列索引对两个窗口都够用：7d 前的行几乎都是成功的，30d 前的行只剩失败的 |
| `workflow_node.deps` → `workflow_run.dag` | 保存时校验存在与无环；创建 workflow_run 时整张图快照进 `dag`。推进与续跑在父行锁下读它判断 blocked → pending；Worker 领到节点后读父行取 input，顺手拿到自己的 deps 再取前驱 output |
| `job_run.workflow_run_id` + `job_name` | 结算时先锁父行，再按 `idx_job_run_node` 载入本工作流全部节点，扫描推进与收敛；节点以 job_name 标识 |
| `workflow_run.input` | 节点执行时与 params、直接前驱 output 一起交给 Executor |
| `workflow_run.state = cancelling` | 无租约的行用状态表达取消中，也是节点实例取消的唯一信号源（心跳 `wf_cancelling`、领取后读父行）。收敛时：任一节点 failed → failed；否则 cancelling → cancelled；否则 succeeded。不需要单独的 error 列 |
| `idx_workflow_node_job` / `idx_schedule_job` / `idx_schedule_wf` | FK 索引；PG 不自动给 FK 建索引，删父行时靠它们避免全表扫 |

---

## 5. 已定决策

| 决策 | 理由 |
|---|---|
| 实例表主键 `bigint GENERATED ALWAYS AS IDENTITY` | 规范首选；PK、FK、5 个部分索引每项比 uuid 少 8 字节。id 由 DB 分配，`Submit / SubmitTx` 用 `RETURNING id`。定义表仍用名字做主键：实例侧弱引用靠名字，历史自描述 |
| 保留清理索引 `(finished_at) WHERE finished_at IS NOT NULL AND workflow_run_id IS NULL` | bigint 不嵌时间，没有索引清理就得全表扫。只在结算时写入，插入不付代价；排除节点实例后大小 ≈ 终态普通实例数。BRIN 因滚动删除后页面复用而失效；分区因去重唯一索引必须含分区键而不可行 |
| 租约到期拆成 `lease_expires_at`，不进索引 | 规范：更新密集表不要更新索引列。心跳每 15s 一次，是全系统最频繁的写；拆出后心跳 HOT。代价：领取多一支、多一个很小的 `idx_job_run_running` |
| 心跳续期封顶 `started_at + timeout + CancelTimeout`（v3） | 租约只由 Worker 心跳延长，DB 从不主动续期；封顶只防「Executor 忽略 ctx 且 Worker 停续租逻辑失效」这一种情况，Worker 挂了和正常执行的行为不变。两个时间都取 DB 时钟 |
| `toast_tuple_target = 256` + `fillfactor = 70` | HOT 还要求新版本放得进同一页。默认 2KB 才 TOAST，一行 jsonb 留在堆内就超过 `fillfactor = 90` 留的 800B，第一次心跳就 HOT 失败、7 个索引全部打新条目。逼出 jsonb 后主行 ~250B，每页 2.4KB 够版本链周转。代价：历史行多占 30% 堆空间；M1 用 `n_tup_hot_upd / n_tup_upd > 95%` 验证。不达标不再叠加参数，直接退回单列方案：`run_at` 兼作 running 行的租约到期时刻，一条领取语句、一个索引，代价是心跳写索引，在途行 < 5000 时看不见 |
| 节点以 job_name 标识，删 `node_key` | 一个 job 在一个工作流里最多出现一次；与"不同参数就声明两个 job"同一规则，少一个要用户起名的概念 |
| 节点引用 job，不自带执行语义 | "什么、怎么跑"只在 job 一处；job 可单跑、挂计划、进多个工作流；改一次处处生效。写工作流多一步声明 job，以后可在 API 层加语法糖自动生成 `<workflow>.<job>` 的 job |
| 节点实例就是 job_run | 领取 / 心跳 / 重试 / 超时 / 取消 / 停机完全共用；只多 1 列 1 态。v2 的 node_run 与 job_run "列集合对齐"就是在弥补这个重复 |
| 边用 `deps` 数组，不建边表 | 规范建议关系用 junction table；这里是定义快照，存在性与无环只能在 Go 校验，没有"谁依赖 X"的反向查询 |
| DAG 快照在 `workflow_run.dag`，不在节点行 | 图属于工作流实例。推进和续跑本来就先锁父行，图在手上；节点行各存一份 deps 是把一张图拆散存 N 份 |
| 不拆 `job_run` | 部分索引已让热集合 = 在途行；jsonb 大值走 TOAST，心跳不重写它们；心跳已是 HOT。拆出 `queue_item` 只在在途行持续上万时才值得，届时挪 4 列改 3 条 SQL |
| job 与 workflow 不做版本 | 实例创建时快照，等价于不可变版本；版本表只多出"查看定义历史"，需要审计时再加，run 行不用动 |
| 上游失败 fail-fast | 任一节点最终失败 → 工作流 `cancelling`，可立即加锁的未开始节点 cancelled（errors 记 upstream_failed），跳过的领取由父状态检查收敛，在跑节点不写，持有者经心跳看到父行 cancelling 后自行结算为 cancelled，全部终态后 failed。与用户取消共用一条路径 |
| 续跑在原 workflow_run 上 | 重置集合 = failed / cancelled 节点及其未成功后代；`attempt` 归零、清 started / finished / output，`errors` 继续追加；deps 全成功的回 pending，其余 blocked；成功节点不动；工作流回 running。回到在途会重新占去重键，撞上同键新 run 返回 ErrDuplicate |
| 不做工作流级超时 | timeout 限制单次执行，max_attempts 限制真实失败 / 中断；不约束正常 Snooze 的总等待。外部任务通过 submit 的成功 output 保存固定 deadline，由 poll 检查（§12），不新增超时扫描 |
| `retry_policy` 用 jsonb，只有 `max_attempts` 必填 | 按 job 调退避是少数需求，缺省走引擎公式 |
| 快照列不折叠成 jsonb | 20 多列是这一行兼任队列项、事实、快照三个角色的结果；折叠只省 2 列 |
| `cancel_requested` 用布尔，不用 `cancelling` 态 | job_run 有租约，取消只是"请求"；加状态会让在途集合多一个值，6 处谓词跟着改。有租约用布尔，无租约（workflow_run）用状态。布尔只给普通实例；节点的取消由父行状态表达，否则取消事务要批量写 running 节点，与心跳按不同顺序锁同一批行会死锁（§7.3） |
| `errors` 追加、`output` 覆盖 | 要追踪每次失败原因；追加长度被 max_attempts × 续跑次数封顶 |
| `schedule` 不带参数覆盖 | 计划只回答"何时"；一次性参数来自 API 调用，直接合并进那条 run |
| `schedule` 两列互斥 FK | 关系型里表达"恰好一个目标且都有 FK"的标准写法；`kind + name` 多态列数一样还丢 FK |
| jsonb 列加 `jsonb_typeof` CHECK，成对空列加 CHECK | 规范要求；零代价 |
| 快照列不满足 3NF，有意保留 | `job_run.params` 是这次执行的时点事实，不是 job.params 的副本，不存在更新异常 |
| 保留清理：成功 7d、失败 / 取消 30d | 差异化保留是 DELETE 的一个 WHERE 条件，不需要归档表（v2 结论）。节点实例随父 `workflow_run` 级联删除，清理 job_run 时必须 `AND workflow_run_id IS NULL`，否则会删掉在途工作流里已完成的节点 |
| 不建 `queue_item` / `queue_state` / `attempts` / 版本表 / 边表 | 均无读方 |
| 表名无前缀，独立 schema | `job` / `schedule` 是通用词，靠 schema 隔离 |

规范里一句需要修正：它说部分唯一索引不能做 `ON CONFLICT` 目标。PG 支持 `ON CONFLICT (col) WHERE 谓词`；本方案只用无目标的 `ON CONFLICT DO NOTHING`，对任意唯一违反都生效。

---

## 6. 核心流程

所有事务默认 READ COMMITTED；跨行不变量靠行锁、条件 UPDATE 与唯一索引，不靠进程内状态。时间一律取数据库时钟，事务都是毫秒级短事务，`now()` 足够。每实例三个循环：领取与定时扫描（事件驱动，§6.11：本地事件、NOTIFY、下次到点定时器三类唤醒，轮询 5s 只兜底）、心跳（15s）；外加 advisory lock 单实例执行的清理（1h）。没有选主。正确性只依赖事务与轮询：通知或定时器丢了、迟了只影响精度，不影响结果。

### 6.1 提交

**普通任务** `Jobs.Trigger(name, params, At(t)?, DedupKey(k)?)`，一条 INSERT … SELECT 完成快照：

```sql
INSERT INTO job_run (job_name, dedup_key, executor_type, params, timeout, retry_policy, state, run_at)
SELECT j.name, $2, j.executor_type, j.params || $3, j.timeout, j.retry_policy, 'pending', coalesce($4, now())
  FROM job j WHERE j.name = $1
ON CONFLICT DO NOTHING
RETURNING id;
```

- `j.params || $3`：浅合并，调用方参数覆盖模板同名键；合并结果就是这次执行的事实。
- 0 行依次判断：`job` 不存在 → `ErrNotFound`；否则同键在途 → 查 `SELECT id FROM job_run WHERE job_name = $1 AND dedup_key = $2 AND state IN ('blocked','pending','running')`，返回 `(existingID, ErrDuplicate)`。查不到在途 run 说明它在两条语句之间结束了，去重此时本就允许新建：重跑 INSERT，最多 3 轮；3 轮仍撞键才返回 `(0, ErrDuplicate)`，id 为 0 的含义是"重复 run 在调用期间结束，重试即可"。没有 `dedup_key` 时撞的只能是 `(schedule_name, scheduled_at)`，那一拍已存在，直接 `(0, ErrDuplicate)`。工作流同一套循环。
- 延迟执行只是 `run_at` 在未来。
- 提交后的唤醒由 `job_run` 上的触发器完成（§6.11）：`Trigger` 另外直接唤醒本实例；`TriggerTx` 的通知随调用方提交发出，本地不唤醒。

**工作流** `Workflows.Trigger(name, input, DedupKey(k)?)`，一个事务三步：

```sql
-- 1. 读定义（全流程唯一一次读定义表）
SELECT n.job_name, n.deps, j.executor_type, j.params, j.timeout, j.retry_policy
  FROM workflow_node n JOIN job j ON j.name = n.job_name
 WHERE n.workflow_name = $1;                                   -- 0 行 → ErrNotFound
-- 2. 父行；dag 由上一步的 (job_name, deps) 组装成 {job_name: deps}
INSERT INTO workflow_run (workflow_name, dedup_key, input, dag, state)
VALUES ($1, $2, $3, $4, 'running')
ON CONFLICT DO NOTHING RETURNING id;                           -- 0 行 → ErrDuplicate
-- 3. 物化全部节点（≤ 500 行，逐行或 unnest 批量）
INSERT INTO job_run (job_name, workflow_run_id, executor_type, params, timeout, retry_policy, state)
VALUES ($job_name, $wf_id, $executor_type, $params, $timeout, $retry_policy,
        CASE WHEN $has_deps THEN 'blocked' ELSE 'pending' END);
```

`TriggerTx(ctx, tx, …)` 在调用方事务内执行同样的语句，业务变更与任务创建原子提交；调用方提交前任务不可见。

**定义保存**：`Jobs.Declare` 是 `INSERT … ON CONFLICT (name) DO UPDATE`。`Workflows.Declare` 先在内存校验（节点 ≥ 1、引用的 job 存在、deps 引用存在、Kahn 排序无环、节点数 ≤ `MaxNodes`），再一个事务 upsert `workflow` 行、删旧节点、插新节点。`Schedules.Put` 在 Go 里拒绝非法 5 字段 cron 与非法 IANA 时区，DB 不校验这两列。cron 只接受 5 字段与 `@hourly` 一类描述符：内嵌 `TZ=` / `CRON_TZ=` 前缀会覆盖 `Timezone`（无空格时 robfig 还会 panic），`@every` 相对扫描时刻而不是固定格点，时区 `Local` 取决于扫描的主机，三者都拒绝。

### 6.2 定时触发

每实例在被唤醒或最近的 `next_run_at` 到点时跑一个事务（§6.11）：

```sql
SELECT name, job_name, workflow_name, cron, timezone, overlap, next_run_at, now() AS db_now
  FROM schedule
 WHERE enabled AND next_run_at <= now()
 ORDER BY next_run_at LIMIT 50
   FOR UPDATE SKIP LOCKED;
-- 每行（Go 侧）：
--   due  := next_run_at
--   next := cron 在 timezone 下严格晚于 db_now 的首个时刻    ← 唯一规则，错过多少拍都只补这一拍；用 DB 时钟，不用本地时钟
--   UPDATE schedule SET next_run_at = $next WHERE name = $name;   -- 不碰 updated_at
--   按 §6.1 创建 run：schedule_name = name, scheduled_at = due,
--       dedup_key = (overlap = 'skip') ? 'sched:' || name : NULL
--   INSERT 返回 0 行（上一拍仍在途）→ 记 skipped 日志与指标，next_run_at 照常推进
-- 批末：本轮推进之后最近的到点，决定下次醒来的时刻；idx_schedule_due 首项。
-- db_now 取 clock_timestamp()：now() 是事务开始时刻，会把本轮触发的耗时再等一遍
SELECT next_run_at AS next_due, clock_timestamp() AS db_now FROM schedule WHERE enabled ORDER BY next_run_at LIMIT 1;
```

- 等待 = `min(jitter(PollInterval), next_due − db_now − 语句返回后的本地耗时)`，下限 20ms：`SKIP LOCKED` 跳过了别人正在触发的行时不空转，几轮内读到推进后的 `next_due`。本批取满 50 行则不等待直接再扫。`next_due` 与 `db_now` 出自同一条语句，本地时钟（单调）只量"已经过去了多久"，不与 DB 时钟比较。
- `Schedules.Put / Delete` 直接唤醒本实例扫描，并经触发器通知其它实例（§6.11）。多个实例同时到点会同时扫描，`SKIP LOCKED` 让一个赢、其余空手。

- 跳过走 `ON CONFLICT DO NOTHING`，不抛错、不回滚，一个坏计划不会拖住同批其他计划；瞬时 DB 错误整体回滚，下一秒重来。
- `next` 不存在（robfig 只向前找 5 年，`0 0 31 2 *` 永不触发；注意日与星期同时限定是 OR 语义，`0 0 29 2 1` 每周一都触发）：`Put` 拒绝这样的规则；扫描时遇到则 `UPDATE schedule SET enabled = false`、记 error 日志与 `schedule_skipped_total{reason="no_next"}`，不建 run。零时间落库会让它每轮都到期、每轮都撞唯一索引，长期占掉扫描批次。
- 停机数天后重启：每个计划只补一拍，`scheduled_at` 是当初错过的那个时刻。
- DST 由 `robfig/cron/v3` 的 `WithLocation` 处理；跳过的本地时刻不触发，回拨产生的重复 UTC 时刻各算一拍。
- `next` 用同一语句带出的 `db_now` 计算，不用实例本地时钟：本地钟慢于 DB 时，按本地钟算出的 `next` 可能等于 `due`，同一拍会每秒重复 INSERT 撞唯一索引直到本地钟追上，并误报 skipped。

`Schedules.Put` 是 upsert，只在规则变了或从禁用转启用时重算 `next_run_at`，避免滚动发布反复推迟触发；`$8` 先 `SELECT now()` 用 DB 时钟计算：

```sql
INSERT INTO schedule (name, job_name, workflow_name, cron, timezone, overlap, enabled, next_run_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (name) DO UPDATE SET
    job_name = EXCLUDED.job_name, workflow_name = EXCLUDED.workflow_name,
    cron = EXCLUDED.cron, timezone = EXCLUDED.timezone, overlap = EXCLUDED.overlap,
    enabled = EXCLUDED.enabled, updated_at = now(),
    next_run_at = CASE WHEN schedule.cron IS DISTINCT FROM EXCLUDED.cron
                         OR schedule.timezone IS DISTINCT FROM EXCLUDED.timezone
                         OR (NOT schedule.enabled AND EXCLUDED.enabled)
                       THEN EXCLUDED.next_run_at ELSE schedule.next_run_at END;
```

### 6.3 领取

先取本地空闲槽位 `n`，再领 `n` 条，不预取。两支各一条语句，第二支只在第一支没领满时执行：

```sql
-- 分支一：到期的 pending，走 idx_job_run_claim
WITH picked AS (
    SELECT id FROM job_run
     WHERE state = 'pending' AND executor_type = ANY($1) AND run_at <= now()
     ORDER BY run_at LIMIT $2
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r
   SET state = 'running', lease_token = gen_random_uuid(), lease_owner = $3,
       lease_expires_at = now() + $4::interval, started_at = now()
  FROM picked WHERE r.id = picked.id
RETURNING r.id, r.job_name, r.workflow_run_id, r.executor_type, r.params, r.timeout,
          r.retry_policy, r.attempt, r.cancel_requested, r.lease_token;

-- 分支二：租约过期的 running（僵尸），走 idx_job_run_running，lease_expires_at 在堆上过滤
WITH picked AS (
    SELECT id FROM job_run
     WHERE state = 'running' AND executor_type = ANY($1) AND lease_expires_at <= now()
     LIMIT $2
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r
   SET attempt = LEAST(r.attempt + 1, 32767),                    -- 上一任以中断结束，计一次；smallint 封顶
       errors = r.errors || jsonb_build_array(jsonb_build_object(  -- 留痕：崩溃过又成功的 run 也看得出来
                  'attempt', LEAST(r.attempt + 1, 32767), 'at', now(), 'kind', 'interrupted',
                  'message', 'lease expired; last owner ' || coalesce(r.lease_owner, '?'))),
       lease_token = gen_random_uuid(), lease_owner = $3,
       lease_expires_at = now() + $4::interval, started_at = now()
  FROM picked WHERE r.id = picked.id
RETURNING 同上;
```

`FOR UPDATE SKIP LOCKED` 在 READ COMMITTED 下加锁后重判 WHERE，被别人刚改过的行不会被领走。新 `lease_token` 覆盖旧值，旧持有者的心跳立刻返回 0 行。`attempt` 是 smallint 而 `max_attempts` 允许 32767：持有者在上限处结算前崩溃时，不封顶的 +1 会溢出并让整条重领语句失败，同批其他过期行也领不走；封顶后由下面第 3 条结算为 failed，反复崩溃也收敛。

领取后、执行前，Worker 依次判断，命中即按 §6.5 结算、不执行：

1. `cancel_requested` → `cancelled`。
2. 节点实例：读父行 `SELECT state, input, dag FROM workflow_run WHERE id = $1`；`state = 'cancelling'` → `cancelled`。
3. `attempt >= max_attempts`（只可能来自分支二）→ `failed`；`interrupted` 条目分支二已追加，这里只改状态，attempt 不再 +1。这是崩溃循环的上限。
4. 节点实例：按 `dag[job_name]` 取前驱输出 `SELECT job_name, output FROM job_run WHERE workflow_run_id = $1 AND job_name = ANY($2)`。

取消工作流或 fail-fast 会跳过正在被领取事务锁定的 pending 节点（§6.5 / §6.6）。领取提交后仍执行上述父状态检查；若原领取事务回滚或提交前崩溃，节点仍为 pending，由后续正常领取完成取消，不增加推进循环。

实现时用 EXPLAIN 确认两支各走各的部分索引。

**领取循环何时跑**（§6.11）：本实例 `Trigger` / 扫描建 run / 槽位释放的直接唤醒，监听到 `run:<executor_type>` 且该类型已注册，到点定时器，轮询兜底。分支一领满 `n` 条时不读到点：槽位释放会再唤醒，短任务吞吐不再被轮询封顶（`docs/baseline.md` 记录的 3 × 16 / 1s 上限就此解除）。没领满时同一轮再读一次本进程类型里最早的 pending `run_at`，自己的短事务：

```sql
-- 到点读取：每个已注册类型各一次 idx_job_run_claim 探测；已到期的行也算
SELECT x.run_at AS next_due, clock_timestamp() AS db_now
  FROM unnest($1::text[]) AS t(executor_type)
  CROSS JOIN LATERAL (
    SELECT run_at FROM job_run
     WHERE state = 'pending' AND executor_type = t.executor_type
     ORDER BY run_at LIMIT 1) x;
```

等待 = `min(jitter(PollInterval), next_due − db_now − 语句返回后的本地耗时)`，下限 20ms；`next_due` 为空则等轮询。已到期的行故意不排除：在领取语句之后才到期的行、被别的实例领取跳过又回滚的行，都在这里被读到，`next_due ≤ db_now` 就按下限 20ms 重领；只有别人长期持有行锁时才会每 20ms 空转一轮。延迟任务与重试退避因此和 cron 一样是毫秒级到点。分支二没有到点定时器：租约过期由轮询发现，崩溃接管等待 ≈ `LeaseTTL + PollInterval`（§10）。

### 6.4 执行与心跳

- Executor 在独立 goroutine 执行，`ctx` 带 `timeout` 秒的 deadline；panic 被 recover，记栈，按可重试失败结算。只覆盖 Executor 自己的 goroutine，Executor 另起的 goroutine 里的 panic 不在范围内，会打挂宿主进程。
- 幂等键跨 attempt 与续跑稳定：普通实例 `run:<id>`，节点实例 `wf:<workflow_run_id>/<job_name>`。有外部副作用的 Executor **必须**用它去重；框架只保证 at-least-once，不做补偿、无法检测。
- 心跳：每进程一个 goroutine，每 15s 一条 SQL 续全部在途租约。`lease_expires_at` 不在任何索引里，这条 UPDATE 是 HOT 更新。续期有硬上限 `started_at + timeout + CancelTimeout`：DB 不会主动续期，只是拒绝把到期时刻推过这个点，Executor 忽略 ctx、Worker 侧停止续租的逻辑又有 bug 时，租约照样到点过期、被重领：

```sql
UPDATE job_run r
   SET lease_expires_at = LEAST(now() + $1::interval,
                                r.started_at + r.timeout * interval '1 second' + $4::interval)   -- $4 = CancelTimeout
  FROM unnest($2::bigint[], $3::uuid[]) AS v(id, token)
 WHERE r.id = v.id AND r.lease_token = v.token AND r.state = 'running'
RETURNING r.id, r.cancel_requested,
          coalesce((SELECT w.state = 'cancelling' FROM workflow_run w WHERE w.id = r.workflow_run_id), false) AS wf_cancelling;
```

- 未返回的 id → 租约已失：按对象身份，在 `inflight` 互斥锁下标记 `dropped` 并移除租约，然后 cancel 该 Executor 的 ctx，结果**不上报**。心跳连续失败超过本地期限也走同一条路径。执行返回同样在这把锁下移除租约，再检查 `dropped`：失租先完成移除则禁止结算；执行返回先完成移除则仍按数据库 fence 结算。不能先解锁移除、再标记丢弃，否则本地判失租但数据库 token 未变时，结果可能在两步之间写入。
- `cancel_requested`（只有普通实例会为真）或 `wf_cancelling`（节点取消的唯一通道）→ cancel ctx，Executor 返回后结算为 `cancelled`。
- 心跳 SQL 连续失败：以**最后一次成功心跳的发起时刻**（单调时钟，`time.Time` 值而不是纳秒数）起算，超过 `LeaseTTL − HeartbeatInterval`（45s）仍未成功即 cancel 全部 Executor 并丢弃结果；不凭内存假设租约仍有效。领取成功不算：它只证明此刻连得上库，不证明旧租约续上了。
- Worker 用两个集合：`inflight` 是它续租的租约，按对象身份匹配（同一 run id 可能已属于后一个 attempt）；`active` 是仍在跑的 Executor goroutine，按 `lease_token` 记而不是按 run id，因为忽略取消的旧 attempt 和本实例重领的新 attempt 可以同时存在，按 id 记会在任一方退出时把另一方一起清掉，Shutdown 就少报。
- ctx 超时或取消后 Executor 超过 `CancelTimeout`（10s）仍未返回：停止为它续租、记日志、不再追踪；租约到期后由任意实例按分支二重领。即使这段逻辑失效，上面的 `LEAST` 也保证租约不会超过硬上限。领取时的初始租约仍是 `LeaseTTL`，上限从第一次心跳起生效。

### 6.5 结算与推进

Executor 返回后调用 `settle`，全系统唯一的"进终态"入口：

```
BEGIN
  节点实例：w := SELECT state, dag FROM workflow_run WHERE id = $wf FOR UPDATE       -- 锁序第一环
  n := UPDATE job_run SET <按 outcome> WHERE id = $id AND lease_token = $token AND state = 'running'   -- fence
       RETURNING state
  n = 0 → ROLLBACK，ErrLeaseLost，丢弃

  取消命中 := cancel_requested OR (节点 且 w.state = 'cancelling')
      -- cancel_requested 是本行的列，在同一条 UPDATE 里读；w.state 在父行锁下读。只有普通实例的 cancel_requested 会为真
  outcome 分支（同一条 UPDATE 的 SET；lease_token / lease_owner / lease_expires_at 一律置 NULL）：
    succeeded              : state = 'succeeded', output = $out, finished_at = now()
    cancelled              : state = 'cancelled', finished_at = now()
    以下五个出口先看取消命中：命中则 state = 'cancelled', finished_at = now()，各分支其余 SET 照旧；未命中：
    snoozed（正常等待）    : state = 'pending', run_at = now() + delay   -- attempt / errors / output 不变
    released（优雅停机）    : state = 'pending', run_at = now(), errors = errors || $err(kind=released)   -- attempt 不变，留痕供告警
    failed 且可重试
      且 attempt + 1 < max_attempts
                           : state = 'pending', attempt = attempt + 1, run_at = now() + backoff(attempt + 1),
                             errors = errors || $err
    failed 其他            : state = 'failed', attempt = attempt + 1, errors = errors || $err, finished_at = now()
    interrupted（§6.3 第 3 条）: state = 'failed', finished_at = now()   -- errors 分支二已追加，attempt 不加

  succeeded 的 $out：nil 与非 nil 的零长度文档都表示无输出，统一写 SQL NULL；Go 侧非空必须是合法 JSON，超过 MaxPayload 按不可重试失败；jsonb 仍拒绝的（反斜杠 u0000 转义、超出 numeric 的数字，SQLSTATE 22 类）
      由 Worker 改按不可重试失败再结算一次，errors 记数据库给出的原因。确定性的编码错误不留给重领，否则行会留在 running、过期后被重领、最后只剩 interrupted 记录。
  errors 条目的 message 是 Executor 唯一能控制的文本：落库前替换非法 UTF-8 与 NUL、截到 4KB，编码仍失败则记固定文案；
      任何 outcome 的结算撞到 22 类错误都走同一条"改按不可重试失败"的路，否则一个带 NUL 的 Permanent 错误会变成三次执行加一条 interrupted
  SQL 写法：state = CASE WHEN cancel_requested OR $wf_cancelling THEN 'cancelled' ELSE '<分支值>' END，finished_at 用同一个 CASE。
  取消后恰好赶上 Snooze / 重试 / 停机 / 重领的行因此不会回到 pending，也不会在取消中的工作流留下 failed 节点

  节点实例且 RETURNING 的 state 为终态 → propagate
COMMIT
```

`Snooze(delay)` 是控制错误而非失败：支持普通 `%w` 包装；取消、失租、停机、超时及显式 Permanent 不被它覆盖。delay > 0，按微秒向上取整；结算时使用 pgx 原生 interval 参数并在 SQL 补足最多 1µs，兼容 PG 13 且避免大时长浮点转换或 Go Duration 向上取整溢出。Snooze 返回的 output 不写库，无论是否非空；不写 errors，不走退避，等待期间不占槽位或持租约。完整契约见 §12。

退避 `backoff(n) = min(max_sec, base_sec × 2^(n−1)) × U[0.8, 1.2)`，缺省 5s / 5min；结果落库，恢复时不重抽随机数。`Permanent(err)` 与输出超限直接 `failed`。`$err = [{"attempt": n, "at": now, "kind": ..., "message": ...}]`。

**propagate**（同一事务、持 `workflow_run` 行锁）：

```
若本节点 = 'failed' 且 w.state = 'running'：                                  -- fail-fast，先写后读
    UPDATE workflow_run SET state = 'cancelling' WHERE id = $wf
    WITH picked AS (
        SELECT id FROM job_run
         WHERE workflow_run_id = $wf AND state IN ('blocked','pending')
           FOR UPDATE SKIP LOCKED)
    UPDATE job_run r SET state = 'cancelled', finished_at = now(),
           errors = r.errors || '[{"kind":"upstream_failed", ...}]'
      FROM picked WHERE r.id = picked.id AND r.state IN ('blocked','pending')
    在跑节点不写：持有者在下一次心跳看到 wf_cancelling；崩溃后的重领方在领取后读父行看到 cancelling
    正被领取的行不等待：下面的读可能仍见 pending 或已见 running，两者都保留为非终态；它经领取后检查或心跳取消

nodes := SELECT job_name, state FROM job_run WHERE workflow_run_id = $wf      -- 走 idx_job_run_node；必须在 fail-fast 写之后读
在内存中套用本节点的新终态

若本节点 = 'succeeded' 且 w.state = 'running'：
    ready := blocked 节点中 dag[节点] 的每个 dep 都 succeeded 的
    UPDATE job_run SET state = 'pending', run_at = now()
     WHERE workflow_run_id = $wf AND job_name = ANY($ready) AND state = 'blocked'

若 nodes 中无非终态行 → finalize：
    任一 failed → 'failed'；否则 w.state = 'cancelling' → 'cancelled'；否则 'succeeded'
    UPDATE workflow_run SET state = $final, finished_at = now() WHERE id = $wf
```

不变量：

- 每个 job_run 至多一次从非终态转终态：fence + `state = 'running'` CAS 双重保护。
- blocked → pending 只在父行锁下发生；两个并行前驱同时完成会串行化，汇合恰好激活一次。
- 领取只改 pending → running，不改终态性；推进写回只动 `blocked` 行，与领取无交集。fail-fast 先取消可立即加锁的节点，再读节点状态；剩余非终态节点可能是被跳过的 pending 或 running，它们进入终态都要经持有父行锁的结算，所以到本事务提交前非终态数不会减少。不能把旧快照里的 pending 直接在内存中改记为 cancelled，否则会提前结束工作流。
- 用户取消的工作流里没有 failed 节点（取消中的失败、中断都结算为 cancelled），fail-fast 的工作流恰有一个 failed 节点，因此 finalize 不需要 error 列。

### 6.6 取消

**普通实例** `Runs.Cancel(id)`，只锁 job_run，一条 UPDATE 覆盖两种状态：

```sql
UPDATE job_run
   SET state            = CASE WHEN state = 'pending' THEN 'cancelled' ELSE state END,
       finished_at      = CASE WHEN state = 'pending' THEN now() ELSE finished_at END,
       cancel_requested = CASE WHEN state = 'running' THEN true ELSE cancel_requested END
 WHERE id = $1 AND state IN ('pending','running') AND workflow_run_id IS NULL
RETURNING state;   -- 'cancelled'：完成；'running'：已打标，等心跳回传；0 行：已终态（幂等返回）、是节点或不存在
```

正在被领取或结算的行：UPDATE 等对方提交后在新版本上重判 WHERE 并套用 CASE，pending 就终结、running 就打标。不能拆成两条按状态各试一次：第一条对 running 行影响 0 行不加锁，第二条等结算提交后重判 `state = 'running'` 不成立也影响 0 行，两条都 0 行会被当成"已终态"返回成功，而行已回到 pending 且未打标，这是 §7.2 不允许的第四种含义。该实例在下一次心跳（≤ 15s）得到信号。节点实例不能单独取消，取消走工作流。

**工作流** `Workflows.Cancel(id)`，锁序 `workflow_run → job_run`：

```sql
BEGIN;
UPDATE workflow_run SET state = 'cancelling' WHERE id = $1 AND state = 'running';   -- 0 行：已终态或已在取消，幂等返回；也是父行锁
WITH picked AS (
    SELECT id FROM job_run
     WHERE workflow_run_id = $1 AND state IN ('blocked','pending')
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r SET state = 'cancelled', finished_at = now()
  FROM picked WHERE r.id = picked.id AND r.state IN ('blocked','pending');
-- 在跑节点不写：取消信号经心跳 RETURNING 的 wf_cancelling 送达持有者（≤ 15s），持有者结算为 cancelled
-- 再读全部节点；若已无非终态节点则将父行终结为 cancelled，否则由最后一个节点的结算收敛
COMMIT;
```

与 fail-fast 共用候选加锁路径。普通条件 UPDATE 会先锁住新版本、再重判状态：即便新版本已是 running 而不再匹配，也可能先等待心跳，形成死锁；必须先用 `FOR UPDATE SKIP LOCKED` 获取候选锁，外层 UPDATE 只写自己已锁住的行。

被跳过的 pending 保留原 `run_at`，当前领取提交则经执行前检查取消；若领取回滚或提交前崩溃，则由后续正常领取处理。这条异步收敛路径需要仍有注册对应执行器的 Worker，以及到期和可用槽位；重复 Cancel 对已 cancelling 的父行仍是幂等返回，不另起扫描。延后结算的节点不追加批量取消用的 `upstream_failed` 条目。持父行锁期间，库内没有其它事务能锁定 blocked 节点：推进、取消、续跑都先锁父行，领取与心跳不选 blocked，因此 blocked 节点不会遗留在跳过集合里。

### 6.7 续跑

`Workflows.Resume(id)`，只对 `failed / cancelled` 的工作流：

```
BEGIN
  w := SELECT state, dag FROM workflow_run WHERE id = $1 FOR UPDATE；要求 state IN ('failed','cancelled')，否则 ErrNotResumable
  nodes := SELECT job_name, state FROM job_run WHERE workflow_run_id = $1
  S := {failed, cancelled 的节点} ∪ 它们在 dag 上的后代中 state ≠ succeeded 的      -- 成功节点永不重跑
  对 S 中每个节点：state = dag 上全部 dep 都 succeeded ? 'pending' : 'blocked'
  UPDATE job_run SET state = v.state, attempt = 0, run_at = now(),
         started_at = NULL, finished_at = NULL, output = NULL             -- errors 保留，继续追加
    FROM unnest($names, $states) v WHERE job_run.workflow_run_id = $1 AND job_run.job_name = v.job_name
  UPDATE workflow_run SET state = 'running', finished_at = NULL WHERE id = $1
      -- dedup_key 非空时重新占去重键；撞上同键在途 run 触发唯一索引错误 23505 → ErrDuplicate
COMMIT
```

整体重跑不是续跑：再 `Trigger` 一次，得到新的 workflow_run。

### 6.8 优雅停机

```
Shutdown(ctx):
  1. 在 inflight 互斥锁内停止领取、定时扫描与监听，解锁后等待当前循环事务跑完；监听连接在 5s 收尾期限内关闭（§6.10），它早已脱离池，不归还。Trigger / TriggerTx 不受影响：提交只是一条 INSERT，run 由其它实例领走。
     已领取的 run 完成领取后检查（读父行、取前驱输出，有界 ctx）后，在同一把锁内检查循环是否已停止并登记 inflight：停止先发生则按 released 结算，不调用 Executor；登记先发生则该 Executor 必定在第 3 步的取消集合内，除非此前已返回或失租。检查与登记不得拆开，锁内不得等待循环或执行器结束
  2. 等待在途 Executor 自然结束，最长 ShutdownGrace（默认 30s；长任务部署设为 p99 时长，并同步放大容器终止宽限）；心跳照常
  3. 到期后 cancel 全部 Executor ctx，等最多 CancelTimeout（10s）
  4. 已返回的：成功的正常结算；因取消返回的以 released 结算 → 回到 pending、attempt 不变、errors 追加 released 条目、立刻可被其他实例领取
  5. 仍未返回的：停止续租、记录 id，返回 ErrNotDrained 告知宿主；租约到期后由他人按分支二重领（attempt +1）
  6. 停心跳，在途的心跳语句一并取消（inflight 已清空，没有要续的租约）；只关闭自有 goroutine，不关闭宿主连接池
```

`Start` 在生命周期锁内登记启动中，随后释放锁再检查 `schema_version`；该查询同时受调用方 ctx 与停机信号控制。查询结束后重新持锁检查停机状态，只有尚未停机才能启动循环。启动失败允许重新 Start，但 Shutdown 后永远不允许；不能让启动查库的连接等待或行锁等待挡住首个 Shutdown，也不能在 Shutdown 返回后补启动循环。

宿主负责信号与是否结束进程；库不调用 `os.Exit`、不接管信号。`Shutdown` 只执行一次：首个调用者执行上面的步骤，后到的调用者等待它完成或自己的 ctx 先到，返回它的结果或自己的 ctx 错误，不会卡在生命周期锁后面继承前者的预算（父 ctx 先触发后台 Shutdown、宿主随后带短预算再调用是常见场景）。`Start` 的父 ctx 取消等价于发起 Shutdown；续租与结算 SQL 用独立、有上限的收尾 ctx，避免父 ctx 一取消就同时取消所有收尾写入。pgx 事务显式 Commit / Rollback。整个 Shutdown 的上限是调用方 ctx 加第 1 步等待的一个在途领取或扫描事务（各自有 10s 上限，实际是毫秒）；清理批次与在途心跳不计入，前者跟随循环 ctx 取消（§6.10），后者由第 6 步取消。

### 6.9 重启恢复

不做任何"重置 running 行"的启动动作。`Start`：校验 `schema_version` → 启动循环与监听（§6.11）。监听建立前发出的通知会错过，建立后立即唤醒一次领取与扫描补上。过期租约由领取分支二自然回收，其他实例持有的有效租约不受影响；计划从持久化的 `next_run_at` 继续并按 §6.2 补一拍。

### 6.10 保留清理

每实例每 1h 在一条独占连接上尝试 `pg_try_advisory_lock(hashtext('skein:maint'))`（会话锁，清理结束即释放），拿到者执行，每步独立事务；不用事务级锁，否则要么每步一个锁要么整个清理一个长事务，而长事务会拖住 xmin 视界、让心跳的 HOT 剪枝失效：

```sql
-- 差异化保留：成功 7d、失败 / 取消 30d。两个窗口各一条 DELETE，走 idx_job_run_retention 范围扫，state 在堆上过滤；循环到影响行数 < 5000。
-- 外层 WHERE 重复终态与保留条件：子查询按语句快照选行，DELETE 等到并发事务提交后只重判外层条件（READ COMMITTED 的 EvalPlanQual），
-- 只写 id IN (...) 会把等锁期间被续跑改回 running 的 workflow_run 连同节点一起删掉
DELETE FROM job_run r WHERE r.id IN (
    SELECT id FROM job_run WHERE state = 'succeeded' AND finished_at < now() - $retention_succeeded
       AND workflow_run_id IS NULL LIMIT 5000)
   AND r.state = 'succeeded' AND r.finished_at < now() - $retention_succeeded;
DELETE FROM job_run r WHERE r.id IN (
    SELECT id FROM job_run WHERE state IN ('failed','cancelled') AND finished_at < now() - $retention_failed
       AND workflow_run_id IS NULL LIMIT 5000)
   AND r.state IN ('failed','cancelled') AND r.finished_at < now() - $retention_failed;
-- 工作流实例同法；节点随 FK 级联删除，因此失败工作流里的成功节点跟父行一起留 30d
DELETE FROM workflow_run w WHERE w.id IN (
    SELECT id FROM workflow_run WHERE state = 'succeeded' AND finished_at < now() - $retention_succeeded LIMIT 5000)
   AND w.state = 'succeeded' AND w.finished_at < now() - $retention_succeeded;
DELETE FROM workflow_run w WHERE w.id IN (
    SELECT id FROM workflow_run WHERE state IN ('failed','cancelled') AND finished_at < now() - $retention_failed LIMIT 5000)
   AND w.state IN ('failed','cancelled') AND w.finished_at < now() - $retention_failed;
-- 在途巡检：只告警不删
SELECT count(*) FROM workflow_run WHERE state IN ('running','cancelling') AND created_at < now() - $retention_failed;
SELECT count(*) FROM job_run WHERE state IN ('pending','running') AND workflow_run_id IS NULL AND created_at < now() - $retention_failed;
```

单实例执行是为了避免多副本互相争 DELETE 锁，不是正确性需要。任一步失败记指标并输出 error 日志：静默失败是这个作业最大的风险。

清理跟随循环 ctx：Shutdown 取消在途批次，pgx 关闭那条独占连接，Shutdown 不等它。仍在等行锁的后端要拿到锁才会退出，期间会话锁还由它持有，其他实例这段时间 skipped，锁随后端退出释放。解锁语句、失败后的回滚、TriggerTx 的 search_path 恢复各自有 5s 的收尾期限（脱离调用方 ctx，但不无界）；解锁失败时关闭该连接而不归还池：会话锁随连接留在池里会让所有实例长期 skipped。

清理语义：终态行删除后，同一 `(schedule_name, scheduled_at)` 与同一 `dedup_key` 都可以再次插入。唯一性只覆盖保留窗口，窗口外允许重放，这是故意的。

### 6.11 唤醒与提前触发

目标：cron 到点、`Trigger` 提交、节点激活、released 回队与 Snooze 到期都在毫秒级被某个实例领到，且这个精度不依赖 `PollInterval`。手段是三类唤醒源加轮询兜底。状态仍只在 PG：进程内没有任务副本，内存里只有"下次该醒的时刻"这一个时间戳，它由 DB 时钟算出、过期无害。

| 唤醒源 | 领取循环 | 扫描循环 |
|---|---|---|
| 本地事件 | `Trigger` / 扫描建 run / 槽位释放 | `Schedules.Put / Delete` |
| NOTIFY | `run:<executor_type>`，类型已注册才醒 | `schedule` |
| 到点定时器 | 最早的 pending `run_at`，含已到期（§6.3） | 最近的 `next_run_at`（§6.2） |
| 轮询兜底 | `jitter(PollInterval)` | 同左 |

**通知由触发器发出**（§3）：`job_run` 上 `AFTER INSERT OR UPDATE OF state, run_at … WHEN (NEW.state = 'pending')`，`schedule` 上 `AFTER INSERT OR UPDATE OF cron, timezone, enabled OR DELETE`。频道名 = schema 名（`TG_TABLE_SCHEMA`，本身就是合法标识符，监听方 `LISTEN "<schema>"`），payload 是 `run:<executor_type>` 或 `schedule`；`pg_notify` 的 payload 必须小于 8000 字节，`executor_type` 超过 7000 字节时发空 payload，监听方按广播唤醒两个循环，提交永远不会因通知失败。选触发器而不是在每条语句后手写 `pg_notify`：产生 pending 行的事务有八处（提交 §6.1、定时 §6.2、推进激活 §6.5、Snooze / released / 退避 §6.5、续跑 §6.7、`TriggerTx` 在调用方事务里），触发器把"可领取的行出现即通知"变成 schema 级不变量，与去重靠唯一索引是同一思路。退避与 Snooze 重排的行 `run_at` 在未来也通知：别的实例要据此重算到点定时器，代价是一次空领取。心跳只写 `lease_expires_at`，不在 `OF` 列表里，触发器不评估；领取把 state 改成 running，WHEN 为假；同一事务里相同 payload 的通知 PG 只投递一次，500 节点的工作流提交按 executor_type 去重。`AdvanceSchedule` 只写 `next_run_at`，不通知，否则每次触发会让全部实例重扫一遍；`Put` 的 SET 总是列出 `cron, timezone, enabled`，`OF` 按列出而不是按值变化触发，所以每次 `Put` 都通知。

**监听**：每个运行了领取或扫描的实例一条专用连接，从池里 `Acquire` 后 `Hijack`，`LISTEN "<schema>"`，成功后立即唤醒本实例的领取与扫描各一次（补上连接建立前错过的通知），然后阻塞在 `WaitForNotification`。任何错误：关闭连接、`listener_reconnect_total` +1、等 `jitter(PollInterval)` 后重连；断开期间靠轮询。监听连接不开事务、不持锁、不参与 §7.3。停机第 1 步随循环停止并在收尾期限内关闭（§6.8）。`DisableWorker` 且 `DisableScheduler` 的实例不监听；只关一个的实例只处理对应的 payload。

**投递语义**：NOTIFY 在事务提交后投递，监听者随后的领取语句一定看到该行；回滚的事务不发通知。通知只是"去看一眼"：领取仍走 §6.3 的 `SKIP LOCKED`，多个实例同时被唤醒是正常情况。唤醒信号是容量 1 的 channel，一轮进行中收到的任意多通知合并成下一轮一次，所以通知风暴最多让每个实例连续跑领取，而不是每条通知一次事务；一次空领取是两支各一次索引探测加到点读取。

**轮询仍然覆盖**：监听断开或 LISTEN 建立前发出的通知；别的实例 `Put` 了更早的计划而通知丢失；分支二的租约过期（没有事件源，见 §10）。`PollInterval` 的含义因此变成"这三种情况的最大延迟"，默认 5s，抖动只加在轮询上，定时器不抖。正常路径的精度是一次 DB 往返加定时器误差。

**精度账**：cron 到点 → 扫描事务 → run 创建 ≈ 一次往返；run 创建 → 本实例领取（直接唤醒）或他实例领取（NOTIFY）≈ 再一次往返。`schedule_lag` 与 `claim_latency` 分别量这两段。

---

## 7. 故障恢复

### 7.1 故障窗口

| 故障位置 | 结果 |
|---|---|
| 提交事务内崩溃 | 回滚；调用方带同样 `dedup_key` 重试 |
| 提交已落库、响应丢失 | 重试命中在途唯一 → 返回已有 id + `ErrDuplicate` |
| 领取后、Executor 启动前崩溃 | 租约 60s 过期 → 任意实例按分支二重领，attempt +1 |
| 外部副作用完成、结算前崩溃 | 重领后重复执行；靠幂等键去重 |
| 结算事务中途崩溃（含推进） | 整体回滚，行仍 running 持旧租约 → 过期重领 |
| 结算已提交、响应丢失 | Worker 不重试结算：行已终态、租约不再续，结果已持久化；未提交的情形见上一行 |
| 旧实例 GC 停顿 / 分区后复活 | 心跳 0 行 → cancel ctx；结算 0 行 → 丢弃；改不了新持有者的状态 |
| 两个实例同时扫到同一计划 | `SKIP LOCKED` 互斥；`(schedule_name, scheduled_at)` 唯一索引兜底 |
| 监听连接断开、通知在 LISTEN 建立前发出 | 唤醒丢失，领取与扫描退化到 `PollInterval` 精度；重连后立即唤醒一次 |
| 通知早于行可见 | 不会发生：NOTIFY 在提交后投递，监听者的下一条语句看到已提交的行 |
| 并行前驱同时完成 | `workflow_run` 行锁串行化，汇合只解锁一次 |
| DB 不可用 | 停止领取；心跳超本地期限 → cancel 全部 Executor、丢弃结果；恢复后重新竞争 |
| 所有实例下线 | 状态全在 PG；任一兼容实例启动即继续；每个计划补一拍 |
| 同一任务反复把进程打挂 | 每次重领 attempt +1，≥ `max_attempts` 时领取方直接结算 failed |

### 7.2 影响行数 = 0 的三种含义

| 场景 | 含义 | 动作 |
|---|---|---|
| fence（`WHERE lease_token = ?`：心跳、结算、释放） | 租约已不属于本实例 | 回滚整个事务，丢弃，不重试 |
| 条件状态转移（`WHERE state IN (...)`：取消、推进写回、续跑守卫） | 已被别的事务处理 | 静默返回 |
| `ON CONFLICT DO NOTHING` / `SKIP LOCKED` 空集 | 别人赢了 / 被去重 | 本次放弃，不回滚已落定事实 |

不允许有第四种解释。

### 7.3 加锁顺序：`workflow_run → job_run`

| 事务 | 锁 | 备注 |
|---|---|---|
| 领取（两支） | job_run（`SKIP LOCKED`） | 只跳过不等待，不锁父行 |
| 心跳 | job_run（本实例全部 running 行，按 PK + token） | 批量续租，可等待节点行锁；不等待父行锁 |
| 结算 / 推进 | workflow_run → 本 job_run → blocked / pending 后继 | 取消候选以 SKIP LOCKED 加锁，不等待其它节点；推进只写 blocked |
| 取消工作流 / 续跑 | workflow_run → blocked / pending / 终态节点 | 取消候选以 SKIP LOCKED 加锁；续跑只写终态节点 |
| 取消普通实例 | job_run | 只允许 `workflow_run_id IS NULL` 的行 |
| 提交 / 定时 | schedule 行 + INSERT | INSERT 不取已有行锁 |
| 清理 | 终态行 | 与续跑争同一 workflow_run 行：DELETE 等续跑提交后重判外层终态条件，已改回 running 的行不删 |
| 唤醒触发器 / 监听 | 无 | `pg_notify` 只入队，不取行锁，在写方自己的事务里；监听连接不开事务 |

结算、取消、续跑先锁父行，顺序一致。领取与取消未启动节点都用 `SKIP LOCKED` 获取候选行锁；结算可能等待本节点，但在持有它后不再等待其它运行节点。READ COMMITTED 在状态重判之前可能锁住已被领取的新版本，所以不能以 `WHERE state IN ('blocked','pending')` 推导“不会锁 running 行”；安全性来自候选加锁不等待，且外层 UPDATE 的目标已归本事务持锁。blocked 节点的修改都由父行锁串行化，不会被库内并发领取或心跳占锁而跳过。

反例：结算已锁 x，批量取消的旧快照含 pending y/z；等待领取 y 时 z 已被领取，心跳先锁 z 再等 x；领取 y 提交后取消再等 z，就形成环，即使最终状态重判会排除 z。直接批量更新 running 节点也同样不安全，节点取消仍由父行状态表达。

集成测试：领取 vs 取消、两个结算 vs 同一 workflow_run、结算 vs 续跑、心跳 vs fail-fast 各并发 1000 轮，断言无 `deadlock_detected`；另用行锁屏障覆盖清理 vs 续跑、取消 vs 重排、fail-fast vs 领取的确定性交错，并验证候选锁冲突时取消不等待、领取提交或回滚后仍能收敛。进程内回归同时覆盖停机与 Executor 登记的临界区，以及失租移除与结果丢弃的竞态。

### 7.4 风险与假设

| # | 风险 | 缓解 |
|---|---|---|
| 1 | 单表兼任队列：在途行上万时心跳与领取的膨胀可见 | HOT 参数（§3）+ `hot_update_ratio` / 索引大小监控；触发条件到达即拆 `queue_item`（§10） |
| 2 | at-least-once 双执行：租约过期到旧持有者感知之间、Executor 忽略 ctx、结算前崩溃 | 幂等键是 Executor 的强制契约；心跳封顶与 `ErrLeaseLost` 只缩短窗口不消除 |
| 3 | 单 workflow_run 推进串行、结算时扫描全部节点 O(N) | `MaxNodes = 500`；不同 workflow_run 之间完全并行 |
| 4 | 结算函数是单点，任何缺陷影响全部路径 | 测试覆盖要求最高；DB 侧四道独立兜底：终态 ⇔ `finished_at`、running ⇔ 有租约、租约三列同生同灭、只有节点会 blocked |
| 5 | 定义无版本：Executor 语义变更时旧实例可能领到按新语义写的 params | 语义变更用新的 `executor_type` 名；实例只领注册过的类型，旧类型排空后再下线 |
| 6 | 时钟：租约与触发全靠 DB 时钟，实例时钟只用于心跳失败的本地保守期限 | 本地期限用单调时钟；DB 时钟前跳会让在途租约提前过期，后果是一次多余的重领；回拨则让过期推后，接管等待变长。两者都不影响正确性 |
| 7 | NOTIFY 让带通知的事务在提交时经全局锁串行化，万级提交/s 时可见 | 触发器只在 pending 行出现时发，同一事务同 payload 去重；到瓶颈时删掉两个触发器、`PollInterval` 调回 1s，协议不变，轮询仍是正确性基础 |
| 8 | 通知风暴：大量重试或提交让每个实例连续跑空领取 | 唤醒信号容量 1，一轮进行中的所有通知合并成下一轮一次；空领取是三次索引探测 |

### 7.5 at-least-once 与幂等

框架只保证：每个 job_run 在数据库里同一时刻只有一个被认可的持有者；过期持有者的写入被 fence 拒绝。物理执行仍可能重叠（租约过期到旧持有者感知之间、Executor 忽略 ctx、结算前崩溃）。`lease_token` 只隔离调度库的陈旧写入，不能阻止旧进程继续调用外部系统；业务资源需要 fencing 时由该资源自己校验。

---

## 8. 接口与配置

```go
type Executor func(ctx context.Context, req *Request) (json.RawMessage, error)

type Request struct {
    RunId          int64
    JobName        string
    Attempt        int                        // 失败 / 中断计数 +1；Snooze 与优雅释放不增加，不是调用次数
    Params         json.RawMessage            // job.params ⊕ 调用方覆盖；代码里 json.RawMessage 即 RawJSON = jsontext.Value
    WorkflowRunId  *int64                     // 节点实例非空
    Input          json.RawMessage            // workflow_run.input；普通实例为 nil
    Deps           map[string]json.RawMessage // 直接前驱的 output，按 job_name
    IdempotencyKey string                     // "run:<id>" 或 "wf:<wf_id>/<job_name>"，跨 attempt 与续跑稳定
}

type Engine struct{ /* ... */ }
func New(pool *pgxpool.Pool, cfg Config) (*Engine, error)
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error       // 显式执行，不在 Start 里
func (e *Engine) Register(executorType string, fn Executor)                      // Start 前完成，重复报错
func Register[P any](e *Engine, executorType string,
    fn func(ctx context.Context, req *Request, params P) (json.RawMessage, error)) // 泛型糖：Params 解码进 P，解码失败按不可重试失败结算
func (e *Engine) Start(ctx context.Context) error                                 // 失败不留下半启动的循环
func (e *Engine) Shutdown(ctx context.Context) error                              // 首个调用执行，后到者等待完成或自己的 ctx；未排空返回 ErrNotDrained
func (e *Engine) Stats(ctx context.Context) (Stats, error)                        // 一条 SQL：到期 pending 数、running 数、最老 pending 年龄、无注册类型的积压数

// 定义
func (e *Engine) Jobs().Declare(ctx, JobSpec) error                               // {Name, ExecutorType, Params, Timeout, Retry}
func (e *Engine) Jobs().Delete(ctx, name) error                                   // 被节点或计划引用 → ErrReferenced（FK RESTRICT）
func (e *Engine) Workflows().Declare(ctx, WorkflowSpec) error                     // {Name, Nodes: []Node{Job, Deps}}
func (e *Engine) Workflows().Delete(ctx, name) error                            // 被计划引用 → ErrReferenced
func (e *Engine) Schedules().Put(ctx, ScheduleSpec) error                         // {Name, Job | Workflow, Cron, Timezone, Overlap, Disabled}；Disabled 零值即启用，与 DisableWorker 同理；Cron 只接受 5 字段或 @hourly 等描述符，不接受 TZ= 前缀与 @every；Timezone 是 IANA 名，不接受 Local
func (e *Engine) Schedules().Delete(ctx, name) error

// 触发
func (e *Engine) Jobs().Trigger(ctx, name, params, ...TriggerOption) (int64, error)      // At(t)、DedupKey(k)；ErrDuplicate 带在途 run 的 id，0 表示它在调用期间结束（§6.1）
func (e *Engine) Jobs().TriggerTx(ctx, tx pgx.Tx, name, params, ...TriggerOption) (int64, error)   // 调用方事务的 search_path 用独立有界 ctx 恢复；恢复失败单独返回，事务不可再信任
func (e *Engine) Workflows().Trigger(ctx, name, input, ...TriggerOption) (int64, error)
func (e *Engine) Workflows().TriggerTx(ctx, tx pgx.Tx, name, input, ...TriggerOption) (int64, error)

// 实例
func (e *Engine) Runs().Get(ctx, id) (*JobRun, error)
func (e *Engine) Runs().List(ctx, RunFilter, cursor) (page, next cursor, error)   // 按 job_name / state 过滤，按 id DESC 排序，游标 = 上页最后一个 id，走 idx_job_run_job
func (e *Engine) Runs().Cancel(ctx, id) error
func (e *Engine) Workflows().GetRun(ctx, id) (*WorkflowRun, error)                // 含全部节点；父行与节点来自同一条语句的快照，不会读到"failed 父行 + pending 节点"
func (e *Engine) Workflows().ListRuns(ctx, WorkflowRunFilter, cursor) (page, next cursor, error)
func (e *Engine) Workflows().CancelRun(ctx, id) error
func (e *Engine) Workflows().Resume(ctx, id) error

func Permanent(err error) error                                                   // 标记不可重试
func Snooze(delay time.Duration) error                                            // 正常等待后再次执行，delay > 0，不消耗 attempt、不保存 output（§12）
var ErrNotFound, ErrDuplicate, ErrReferenced, ErrNotDrained, ErrNotResumable error
```

| 配置 | 默认 | 说明 |
|---|---|---|
| `Schema` | `skein` | 每应用一个 schema |
| `DisableWorker` / `DisableScheduler` | false / false | 角色开关，零值即启用（Go 的 bool 无法默认 true，因此用反义命名）：前者关掉领取与执行，后者关掉定时扫描与清理。只提交不执行的 API 进程两者都设 true |
| `Concurrency` | 16 | 本实例执行槽位；不是集群总限额 |
| `PollInterval` | 5s（±20% 抖动） | 领取与定时扫描的兜底周期：只决定通知丢失、他人改计划、僵尸重领这三种情况的最大延迟；正常触发精度与它无关（§6.11） |
| `HeartbeatInterval` / `LeaseTTL` | 15s / 60s | `LeaseTTL = 4 × Heartbeat`；崩溃接管等待 ≈ `LeaseTTL + PollInterval` + 排队 |
| `DefaultTimeout` / `DefaultRetry` | 3600s / `{"max_attempts":3}` | `Declare` 未填时写入 job。`Retry` / `DefaultRetry` 只有整个策略为零值才取默认，部分填写按原样校验（`{base_sec:-1}` 不能靠省略 `max_attempts` 绕过） |
| `BackoffBase` / `BackoffMax` | 5s / 5min | `retry_policy` 未覆盖时的退避 |
| `ShutdownGrace` / `CancelTimeout` | 30s / 10s | 长任务部署把前者设为 p99 任务时长 |
| `ReleaseAlertThreshold` | 3 | 同一 run 的 released 条目达到即告警 |
| `RetentionSucceeded` / `RetentionFailed` | 7d / 30d | 成功实例 7 天；失败 / 取消 30 天，留足排障时间 |
| `MaintenanceInterval` | 1h | |
| `MaxNodes` / `MaxPayload` | 500 / 256KB | 定义与提交时校验。`MaxPayload` 限制的是每个输入文档（params 模板、Trigger 覆盖、工作流 input）和输出，各自单独校验；落库的 params 是模板与覆盖合并后的结果，最多 2 倍。输出超限按不可重试失败 |

名字（job / workflow / schedule / `executor_type`）非空且 ≤ 255 字节：它们进 dedup key、errors 条目（`<job> failed`）与 NOTIFY payload，源头限长让这三处都有界。`New` 填默认值后校验每个字段的取值范围：所有周期、退避、宽限、保留时长必须 > 0，`Concurrency` / `MaxNodes` / `MaxPayload` / `ReleaseAlertThreshold` ≥ 1，`LeaseTTL > HeartbeatInterval`，`BackoffMax ≥ BackoffBase`，`RetentionFailed ≥ RetentionSucceeded`；`retry_policy`（`Declare` 与 `DefaultRetry` 同一规则）要求 `1 ≤ max_attempts ≤ 32767`（`attempt` 是 smallint），`base_sec` / `max_sec` 在 0 到 30 天之间，`0 ≤ jitter < 1`（NaN 同样拒绝）；`DefaultTimeout` 与 `JobSpec.Timeout` 在 1s 到 2³¹−1 秒之间（列是 integer 秒，更大的值转 int32 会回绕）。负周期会让 `time.NewTicker` 在后台 goroutine 里 panic，必须挡在 `New`。`Start` 的状态登记、循环启动与 `Shutdown` 在生命周期锁下串行；启动查库不持锁，随停机取消（§6.8）。`Shutdown` 之后不能再 `Start`。

`Metrics` 会被执行 goroutine 与各循环并发调用，实现必须并发安全且不阻塞。`exec_duration` 的 `outcome` 标签取结算后落库的状态：取消命中把 retry / released / snoozed 改成 cancelled 时标签也是 cancelled。

连接预算：执行不占连接；监听从池里取一条并脱离池（`Hijack`），进程存续期间不归还。池大小由宿主决定，建议 ≥ 5 并为心跳与结算保留余量，避免领取 SQL 耗尽连接造成大面积误过期。运行账号不用超级用户；迁移由发布步骤显式串行执行。依赖只有 `pgx/v5`、`robfig/cron/v3` 与标准库。

耐久性：库假设 PG 保持默认的 `synchronous_commit = on`。关掉它意味着已向调用方确认的提交可能丢失；异步复制的故障切换同样可能丢掉主库最新事务。这两项由数据库层的容灾与备份负责，库不做补偿。

### 8.1 观测

日志用 `log/slog`，每条带 `run_id / job_name / attempt / instance`。指标通过 `Config.Metrics` 回调接口暴露（宿主接 Prometheus 或 OTel），不引入依赖。指标固定这 11 个：

| 指标 | 来源 | 告警条件 |
|---|---|---|
| `pending_due` | `Stats()`：`state = 'pending' AND run_at <= now()` 计数 | 持续增长 = 执行力不足或无注册类型 |
| `pending_oldest_age` | 最老到期 pending 的 `now() − run_at` | > 领取周期数倍 |
| `claim_latency` | 领取时 `now() − run_at` | |
| `exec_duration{executor_type, outcome}` | 结算时打点，outcome = succeeded / failed / cancelled / released / snoozed | |
| `lease_lost_total` | 心跳或结算 fence 返回 0 行 | 持续非 0 = 租约参数偏紧或有慢 Executor |
| `reclaim_total` | 领取分支二命中数 | 持续非 0 = 有实例反复崩溃或假死 |
| `released_alert_total` | 结算 released 时该 run 条目数 ≥ `ReleaseAlertThreshold` | > 0 |
| `schedule_lag` / `schedule_skipped_total{reason}` | 触发时 `now() − scheduled_at`；overlap 跳过计数 | lag 持续 > 1s = 扫描被阻塞或监听长期断开；skipped 持续增长 = 任务比周期长 |
| `listener_reconnect_total` | 监听连接断开后每次重连计数 | 持续增长 = 唤醒退化为轮询，查网络或 `max_connections` |
| `retention_deleted_total{table}` / `maintenance_failures_total{step}` | 清理作业 | failures > 0；deleted 长期为 0 且表在涨 |
| `stale_active_total` / `hot_update_ratio` | 在途巡检计数；`pg_stat_user_tables.n_tup_hot_upd / n_tup_upd`，只看趋势：claim、settle、取消、Resume、节点激活都改索引列，永远不是 HOT，比例随任务组成变化，M5 基线整表约 7%。基线用 `hot / (upd − 2 × 启动次数)` 折算心跳口径，只在"普通任务、无 released、无取消、无 Resume"的受控场景成立 | stale > 0；比例相对自身基线明显下滑。95% 只是 `TestHeartbeatIsHot` 在安静库上对 §3 存储参数的验收，不是生产阈值 |

---

### 8.2 开源交付与验证

`README.md` 是使用入口：安装 Go 1.27 与库、PostgreSQL ≥ 13、宿主 `DATABASE_URL` 与测试 `SKEIN_TEST_DSN`、显式 `Migrate` 后再 `Start` 的示例，以及本地与 CI 检查命令；完整嵌入示例仍在 `example_test.go`。迁移在发布步骤串行执行，不加入 `Start`。项目采用 MIT 许可证，版权署名 Jaken。

GitHub Actions 在 push、pull request 和手动触发时，以 Go 版本文件 `go.mod` 和 PostgreSQL 13/18 服务容器矩阵运行；sqlc 固定为 1.31.1。服务 TCP 健康检查通过后，先以测试 DSN 执行真实连接查询，再依次执行 `make lint`、`make test-ci`、`make race`。整个测试 job 设置 `SKEIN_TEST_REQUIRE_DB=1`，数据库不可达必须失败；`test-ci` 使用 `go test -count=1 ./...`，不能用上次成功的测试缓存代替本次数据库检查。M5 性能基线仍为显式运行，不加入普通 CI。

### 8.3 源码组织与贡献导航

保持根目录 `skein` 主包、`internal/store` 与嵌入迁移的现有包边界，不新增执行内核包或服务框架。Engine 及包内执行函数负责本进程的生命周期、执行槽位与租约跟踪；Store 负责数据库事务、持久状态转换及工作流推进。职责按文件划分，不按目录数量判断是否需要拆包；本文 §3 DDL、§6 事务协议与 §7.3 锁序不变。

README 提供从公共入口、提交、执行、数据库事务到回归测试的源码阅读导航。测试仍与实现位于同一包，文件按行为而非里程碑编号命名：`reliability_test.go` 覆盖 M2 多实例可靠性，`workflows_test.go` 覆盖 M3 工作流，`schedules_maintenance_test.go` 保留 M4 定时、清理、Stats 与列表的验收用例，`wake_test.go` 覆盖 M6 精准触发。本次仅重命名文件，不拆分测试、不修改测试函数或共享夹具；§9 继续保留里程碑与验收条件的对应关系。

---

## 9. 实施步骤

单人估算，含集成测试。每步用真实 PostgreSQL 与两个 OS 进程验证，不用内存替身。

| 里程碑 | 内容 | 必过检查 | 估算 |
|---|---|---|---|
| **M1 普通任务内核** | 迁移、7 张表、Register / Declare / Trigger / TriggerTx / Get、领取两支、执行池、心跳、结算（成功 / 重试 / 永久失败）、去重 | 提交响应丢失后同键得原 id；panic 计一次失败；`Permanent` 直接终态；EXPLAIN 两支各走各的部分索引；心跳 `pg_stat_user_tables.n_tup_hot_upd` 占比 > 95%，不达标则退回单列 `run_at` 方案而不是加存储参数 | 4 天 |
| **M2 多实例可靠** | 僵尸重领、attempt 上限、优雅停机（released）、心跳失败本地期限、重启恢复、取消普通实例 | kill -9 A 后 B 在 ≤ 75s 内接管且 attempt +1；暂停 A 超 60s 再恢复，A 的旧 token 写不进任何行；第 `max_attempts` 次重领直接 failed；Shutdown 时不合作 Executor 返回 `ErrNotDrained` 且随后被他人重领 | 3 天 |
| **M3 工作流** | Workflows.Declare 校验、Trigger 物化、propagate / finalize、fail-fast、取消、续跑 | 100 个并行前驱同时完成汇合只激活一次；空定义拒绝；一节点失败后其余节点全部终态且 run 为 failed；续跑只重跑失败链、成功节点 `started_at` 不变；四对事务（含心跳 vs fail-fast）并发 1000 轮无死锁 | 4 天 |
| **M4 定时与交付** | Schedules.Put upsert、扫描事务、补一拍、overlap = skip、DST 用例、保留清理、Stats、嵌入示例 | 两实例同时扫描同一计划只创建 1 个 run；反复 Put 相同配置不推迟 `next_run_at`；跨 3 个周期停机再启动只补 1 拍；上一拍在途时本拍 skipped；清理不删在途工作流里的已完成节点 | 3 天 |
| **M5 性能基线** | 3 实例、10 万条存量 job_run、长短任务混合、256KB 与 1KB payload 各一组（256KB 组是重复字符，压缩率极高，只是可压缩 payload 的基线） | 报告吞吐、触发 / 排队延迟 p50 / p95 / p99、心跳口径的 `n_tup_hot_upd` 比例、`idx_job_run_claim` 峰值与手动 VACUUM 后的死元组（B-tree 页只标记可重用，大小不回落；autovacuum 周期内的行为未测）、`pg_stat_database.active_time`（后端活跃时间，不是 CPU；PG 13 无此列，标 N/A，其余测量照常）与锁等待。不设 SLA，只留基线供后续对比。报告与解读见 `docs/baseline.md`，复跑用 `SKEIN_BENCH=1 go test -run TestPerformanceBaseline` | 1 天 |
| **M6 精准触发** | 唤醒触发器、监听连接、扫描与领取的到点定时器、槽位释放再领、`PollInterval` 默认 5s、`listener_reconnect_total` | 全部在 `PollInterval = 5s` 下、以 Executor 入口时刻（DB 时钟 `clock_timestamp()`）减 `run_at` 计：跨实例 `Trigger` 与 `TriggerTx` 提交、`At(now+700ms)`、退避重试、cron 到点、released 回队、被锁跳过后的到期行，各 ≤ 300ms（CI 断言，机器负载下的上限，不是精度分布）；精度分布由 `docs/baseline.md` 的 5s 轮询排队延迟 p50 / p99 记录；`pg_terminate_backend` 杀掉监听连接后仍在 `PollInterval` 内领到、重连后 `listener_reconnect_total` = 1 且恢复毫秒级；EXPLAIN 到点读取每类型一次 `idx_job_run_claim` 探测；心跳 HOT 比例不变；短任务吞吐不再被轮询封顶（对照 `docs/baseline.md` 的 42/s） | 2 天 |
| **M7 真实负载与多实例验收（smoke 已实现）** | `TestScenarioAcceptance` 已实现 3 进程 smoke，两个 `TestScenarioVerifier*` 验证坏证据与退避核查；1 / 3 / 6 进程完整负载、故障恢复与稳态仍待实现（§11） | 提交清单与终态 / 副作用逐项对账；低负载不提前执行且启动延迟 ≤ 300ms；负载中的取消、重试、DAG、去重与旧租约 fence 正确；容量分档报告积压与排空，不将超载排队判成调度精度问题 | 首版约 2 天，运行预算见 §11.5 |
| **M8 两节点外部任务与纯 Snooze（已完成）** | 新增 Snooze 控制结果，原 run 延迟回 pending；视频任务以 submit 成功 output 向 poll 传递 task id 与固定 deadline（§12），不改 DDL / output / Resume 契约 | Snooze 不消耗失败次数、不写 output；等待不占槽位；24h 延迟可持久化；重启、旧租约 fence、取消与工作流推进正确；Resume 保留已成功提交的外部任务与原 deadline | 1–2 天 |

全量审查回归映射：M1 的 `TestEmptyOutputSucceeds` 覆盖普通任务与工作流节点的 nil / 非 nil 空输出；`TestDedupWhileWinnersFinish` 覆盖并发完成窗口，`TestDedupRoundsWhenWinnersFinish` 用真实事务的确定性交错覆盖 job / workflow 在一轮竞争后成功和三轮耗尽返回零 id。M2 的 `TestShutdownCancelsStart` 验证池耗尽时首个 Shutdown 不等待启动查库、查询随停机取消且不得补启动。

M1 ~ M6 直线 17 个工作日；含故障回归与联调按 **3 周** 排。M7 单独交付，不因现有 M5 / M6 已通过而视为完成。M8 已确认实施，完成 §12 验收前不将功能标记为已交付。

---

## 10. 明确不做与加回条件

| 不做 | 加回条件与方式 |
|---|---|
| 独立队列表 `queue_item` | 在途行持续 > 5000 或领取索引在 autovacuum 周期内不回落：挪 4 列改 3 条 SQL，需停写迁移在途行 |
| 定义版本表 | 需要"查看定义历史"的审计需求：加 `job_version` / `workflow_version` 追加表，run 行不动 |
| API 语法糖：工作流内联节点自动生成 `<workflow>.<job>` 的 job | 写工作流嫌声明 job 烦时加，存储模型不变 |
| `on_upstream_failure = continue`、工作流级超时、动态工作流 | 真实需求出现时按节点列 / `deadline_at` / 激活键分别加 |
| 专用异步执行器（Submit / Poll / Abort）、运行中 checkpoint | 当前视频场景采用两节点 + 纯 Snooze（§12），task id 放 submit 的成功 output；只有两节点无法表达业务阶段或必须由库管理外部 Abort 时，再评审专用接口与持久化字段 |
| 僵尸重领的到点定时器 | 接管等待由 `LeaseTTL` 主导，多等一个 `PollInterval` 不值得每轮多读；需要时在领取轮次里对本进程类型的 running 行取 `min(lease_expires_at)`（走 `idx_job_run_running`，行少，列在堆上），不改索引、不碰心跳 HOT |
| HTTP API / Web / 内置执行器 / OTel | 薄封装同一个 Engine 即可，不改协议 |
| 分区表 | 去重唯一索引必须含分区键，当前不可行；量级到达时先拆去重槽位表再分区 |

列出加回条件不等于授权实现；涉及 §3 / §6 / §7.3 的变更仍须单独确认。§12 的纯 Snooze 已单独确认实施，不承诺专用异步执行器或外部 Abort。

---

## 11. M7：真实负载与多实例验收

状态：**smoke 已实现，full / soak 待实现，M7 未完成**。运行方式与实测见 `docs/scenarios.md`。这里的“真实”指真实 PostgreSQL、独立 OS 进程和可核对的业务副作用；执行器先用确定性的模拟业务，不把合成数据称为生产流量回放。取得实际业务的任务比例、耗时和 payload 分布后，再增加对应案例。

### 11.1 边界与复用

- 只增加测试与报告，不新增 CLI、服务、运行时依赖、公共 Config 字段或生产表；§3 / §6 / §7.3 不变。若验收发现需要改变协议的问题，先单独评审，不在压测实现中顺带修改。
- 新用例集中在 `scenarios_test.go`，复用 `freshSchema`、子进程夹具、`percentiles` 和现有故障夹具；长批次等待使用 M7 的总预算，不套用快速单例测试的短等待期限；`helper_test.go` 增加 scenario 模式，以便从父进程控制就绪、暂停、退出和恢复。只创建实际需要的测试代码，不搭通用压测框架。
- M5 的 `TestPerformanceBaseline` 与 `docs/baseline.md` 保持原场景，仍用于同参数纵向比较；M7 单独输出报告。M2 / M3 的确定性交错测试继续负责证明具体 fence / 锁序，M7 验证这些行为在混合负载下能否收敛，不以随机压力替代确定性证明。
- 每个案例使用独立测试 schema、固定随机种子（首版为 1）与有限执行期限；只操作测试 DSN，所有测试 SQL 和审计表均属于 `_test.go` 夹具，退出时清理。没有独立测试 PG 则不运行，显式运行时数据库不可达必须失败。

### 11.2 负载与场景

完整混合批次包含 **10,000 次普通任务触发**，另加 **100 次四节点工作流触发**；父工作流、节点行和执行次数分别计数，重试 / Resume 不算新提交。smoke 按十分之一缩小批次，仍保留全部任务种类。

| 普通任务 | 数量 | 预期行为 |
|---|---|---|
| 短任务 | 6,000 | 可核对输入 / 输出的 JSON 处理、摘要计算或模拟 I/O；主要耗时 5–20ms |
| 长任务 | 1,500 | 1–3s，正常响应 ctx；其中长于 2s 的子集用于核对心跳续租 |
| 重试任务 | 1,000 | 前两次失败、第三次成功；`max_attempts = 3`，检查每次退避后的 `run_at` |
| 预期失败 | 1,000 | 500 条 Permanent、500 条超时后耗尽两次机会；各自失败种类及次数明确 |
| 取消任务 | 500 | pending 与已进入执行器各半；用入口事件 / 夹具屏障确认取消时机，不能靠猜测 sleep |

工作流分为 40 条成功菱形图、30 条成功链、20 条 fail-fast 图和 10 条失败后 Resume 的链；检查依赖输出、后继启动顺序及成功节点不被 Resume 重跑。这里的“100 次”不包含 Resume 调用。短任务中一部分延迟 700ms–3s，其余立即投递；cron 另设低负载案例，用 `dueAt` 设置近期的 `next_run_at`，不等待自然分钟边界。

smoke 在原混合批次排空后，另加 **48 个普通 Snooze 任务**（各 Snooze 两次、每次 3s）与 **48 个槽位探针**；确认前者首次全部进入 pending 后，让探针在最早 Snooze 到期前同时进入并停在测试屏障，证明 3 × 16 个槽位均可复用，再开放屏障。另加一个 **submit → poll → verify** 三节点工作流：submit 成功输出经依赖传给 poll，poll Snooze 四次、每次 700ms，verify 是检查后继不得提前执行的测试节点。Snooze 的 max_attempts 固定为 1，调用始终为 Attempt 1、最终 attempt 0 / errors 空，不能用 Attempt 作为调用计数。两组共 100 次 Snooze 再启动；独立窗口沿用 0–300ms 精度阈值，不以这组小型等待 / 槽位竞争替代 full 的混合容量验收。

每次 Snooze 保存请求时长、同次 claim 的 started_at，以及观察到的 pending / run_at 快照；快照须属于该次 claim，不能用下一次 pending 冒充。检查租约字段清空、attempt / errors 不变、Snooze 随带输出未写入，工作流仍 running、后继仍 blocked；下一次入口的 run_at 必须等于已观察到的到期点，且到期点落在「返回审计时刻 + 请求时长」至「结算观察时刻 + 请求时长」之间。每次 poll 的依赖摘要及 submit 的成功输出、执行次数、started_at 保持一致。制品及核查器负例覆盖缺失快照、错误时长 / 到期点、状态污染与不同的下一次 run_at。追加后总清单为 **1,642 个 job_run、11 个 workflow_run**；原 1,000 普通任务、10 个四节点工作流及五类精度批次均保持不变。

payload 按固定种子生成合法 JSON，按完整编码后的字节数控制大小：90% 约 1KB、9% 约 32KB、1% 约 250KB，大文档使用低压缩率内容而非重复字符。报告实际大小和压缩特征；这组比例只是首版基准，不代表用户业务。

| 场景 | 执行方式 | 核查重点 |
|---|---|---|
| 低负载精度 | 有空闲执行槽位时逐类投递；立即、TriggerTx、延迟、cron、重试各至少 100 个启动样本 | DB 时钟下不提前执行、启动延迟分布；TriggerTx 精度样本显式指定未来 At 并在到期前提交，长事务持有时间另计 |
| 混合批次 | 相同数据分别交给 1 / 3 / 6 个进程，每进程 Concurrency = 16 | 每项提交收敛到预期状态；每进程执行槽位上限；短任务尾延迟与长任务竞争 |
| 注册差异 | 3 个进程有重叠及不重叠的执行器集合；先留下一个无人注册的类型，再启动支持它的进程 | 不误执行未注册类型；Stats 报告该积压；加入可执行者后排空，不要求各进程领取均匀 |
| 负载中故障 | 3 个进程持续投递，分别执行优雅退出并补新进程、杀死持租进程、暂停持租进程到接管后恢复、终止本案例的 LISTEN 后端 | 不丢已确认提交；允许租约语义内的重复执行；旧 token 不能结算；监听中断可退化为轮询再恢复 |
| 持续投递与稳态 | 同配比定速投递、逐档加压，再以已验证的稳定速率运行 30 分钟 | 实际提交速率、积压斜率、排空耗时、心跳 / 连接 / 锁等待及资源趋势 |

故障由“执行器已进入”“目标 token 已落库”“新持有者已接管”等事件驱动；暂停必须在有界清理中恢复，kill / 后端终止只针对本案例创建的进程或连接。故障后须先排空并完成对账，再开始下一种故障，避免把多种原因混成一个结果。首版不包含 PG 主从切换、磁盘故障或公网服务故障；这些不属于当前库的持久性保证。

### 11.3 证据与计时口径

测试保存两类证据：父进程的提交清单，以及独立于 worker 生命周期的审计 / 模拟业务结果。审计可使用测试 schema 内的小表，业务幂等键用唯一约束兜底；不得为审计改写 `job_run` 或新增生产 attempts 表。进程被 kill 后，这些证据仍须可读。

1. **提交清单。** 保存案例、输入摘要、预期结果、返回 id 与错误、提交耗时；显式 At 与预期 cron 拍次从投递端独立记录，并检查实际 run_at / scheduled_at 一致，不能只用待验证的数据库字段自证准点。普通任务和工作流分别对账。`TriggerTx` 只有调用方提交确认后才算已确认提交；回滚不应产生可见 run。另设长事务案例，验证提交前不执行、提交后最终可执行；其 `run_at` 到入口的等待包含事务持有时间，不套用 300ms 精度断言。故障期间调用结果不确定的提交单独列出，用测试业务标识核对，不能当作从未提交或直接归入丢任务。
2. **逐次执行。** 每次进入执行器生成独立 invocation 标识，记录 run id、Request.Attempt、instance、关联租约、输入摘要、进入 / 返回和业务结果；入口读出本次 `run_at` 与 DB 时钟。不能只读最终行推断前几次执行，`attempt` 在 released 时不增加、Resume 时归零，也不能单独充当执行记录主键。无法与同次租约对应的采样保留为未知，不用后来那次的 `run_at` 补算。
3. **幂等副作用。** 用 `Request.IdempotencyKey` 写一份可核对的模拟业务结果，唯一键判重；包含“副作用已提交但执行器尚未返回时被 kill”的案例。重试可再次进入执行器，但同一业务键的效果不能重复；该保证来自测试业务的幂等实现，不宣称 Skein exactly-once。被取消或失败的任务可能已有副作用，不能一律要求其副作用数为零。
4. **证据完整性。** 正常返回的调用必须有可归属记录，崩溃留下的未闭合调用由故障时间线及最终状态解释。审计失败使案例失败；不能把观测错误吞掉后输出“通过”。审计使用独立、有界的小连接池，不持有连接模拟业务耗时；报告其 SQL 数量 / 开销，所得性能是带审计的端到端数据，不冒充无观测开销的调度器极限。

| 指标 | 定义 | 限制 |
|---|---|---|
| 提交耗时 | 同一进程单调时钟上的 API 调用耗时 | TriggerTx 另记 Commit 调用耗时，不把 TriggerTx 返回当成已提交 |
| 到期后启动延迟 | 普通任务为执行器入口的 `clock_timestamp()` − 本次租约对应的 `run_at`；cron 另以 `scheduled_at` 衡量整段启动延迟 | 两项来自 DB 时钟；cron 不能仅用扫描后创建的 `run_at` 掩盖扫描晚点。负载中包含排队，不等同纯调度开销；重领单列，不拿旧 `run_at` 评价接管精度 |
| 执行耗时 | 同一 invocation 内单调时钟的业务开始到返回前耗时 | 超时 / kill 的未完成调用单列，不用下一次执行或最终 `finished_at` 补齐 |
| 结算确认延迟（观测上界） | 执行器返回前的 DB 审计采样，到观察者首次读到可归属结算结果的 DB 采样 | 包含审计、观察轮询及查询耗时，不是精确 commit 耗时；`finished_at` 是事务时间，也不能直接当作提交时刻。无法区分结算与重领的样本记缺测并报告数量 |

按场景、任务类型、payload 档位分别输出样本数及 p50 / p95 / p99 / max；故障与无故障窗口分开。整体平均值不能掩盖短任务尾延迟；不足样本、未返回、缺测和提交端限速都必须出现在报告里。

### 11.4 通过条件与容量边界

以下正确性条件在低负载和超载下都成立；超载只允许排队变长，不允许静默丢失、错误结算或突破配置的执行槽位上限。

| 检查 | 必过条件 |
|---|---|
| 对账与收敛 | 所有已确认提交均能按返回 id / 测试标识核对；停止投递并解除故障后，在案例排空期限内达到预期终态，不能只看 completed 总数或把未知提交丢弃 |
| 任务与工作流语义 | 无故障任务的输出、失败种类和尝试次数符合清单；取消不变成成功；后继不先于必要前驱成功而执行；Resume 不重跑已成功节点；固定计划拍次不重复创建 |
| 去重与副作用 | 在途同键互斥；赢家在 INSERT / lookup 之间完成时，允许新建，三轮耗尽允许 `(0, ErrDuplicate)` 并按新契约处理；副作用按业务键唯一，不把所有故障场景都断言为“只执行一次” |
| 租约与并发 | 单进程存活执行器数 ≤ Concurrency；已失租但未返回的调用仍占槽位，也计入存活数，有效持租数另报。死进程的未闭合记录不能继续算存活槽位。重领遵循数据库租约到期，旧 token 的心跳 / 结算不能生效；暂停恢复的旧执行器不能覆盖新结果 |
| 无故障窗口 | 不允许未计划的终态失败、死锁或陈旧租约写入；心跳失败 / lease lost / reclaim 必须记录并定位，不能仅因“压力大”就忽略，未解释的非预期恢复使该档不通过 |

低负载精度沿用 M6 的 **0 ≤ 启动延迟 ≤ 300ms**，只在测试数据库可用、执行类型有空闲槽位且没有待执行积压的独立窗口判定；每次运行都报告分布和超限样本。300ms 是该验收环境的上限，不是生产 SLA，也不用于判断混合批次或超载场景。高负载机器上的超限记录为该次精度验收失败，保留环境信息后独立复跑；不能在实现中偷偷放宽阈值。

接管另计：无额外排队且有健康可执行者时，从死亡 / 暂停后数据库保存的 `lease_expires_at` 到新执行器入口，允许 **1.2 × PollInterval + 2s** 的检测与查询预算；不得早于该到期时刻。故障发生到租约到期的等待单独记录。槽位不足时报告排队，但必须在解除负载后的排空期限内收敛，不能据此宣称满足上述接管预算。

容量测量使用以下固定步骤，不预设所有机器都能达到的 tasks/s：

1. 同配比、同进程数、同连接预算先跑有界批次，取排空吞吐 C 作为定档参考；C 不是已证明的稳态容量。普通触发与工作流触发的速率按逻辑提交计，另报节点行吞吐和实际 invocation 吞吐。
2. 每档预热 30s、测量 120s，以 0.5C / 0.8C / 1.2C 固定速率投递，每档结束先排空。发生器按独立时间表发起请求，不等待任务完成再发下一批；提交并发有界，发不出的请求记发生器滞后 / 未发数量，不能悄悄降低目标速率。
3. 当目标速率实际达到、正确性检查通过、后 60s 的未终态积压不呈持续增长且停止后按预算排空时，才将该档记为已验证的稳定速率；积压增长的档只报告超载表现。短窗口结论还需 30 分钟稳态确认，不宣称绝对容量上限或实例数线性扩容。
4. 稳态采用已通过的最高档；若没有稳定档则先判容量测试失败，不启动长跑。每秒采样积压、每类型最老等待、连接池等待 / 占用、心跳 / 重领 / 锁等待及进程内存；最大未终态预算初版为 20,000 个 job_run，触顶就停止投递并排空，报告“触顶中止”，不算稳态通过。保留自然 autovacuum 行为，不在测量中插入手工 VACUUM；30 分钟内未发生 autovacuum 就明确标注未覆盖该周期。

初版负载配置固定并写入报告：PollInterval = 5s、HeartbeatInterval = 1s、LeaseTTL = 10s、CancelTimeout = 5s；smoke 的 ShutdownGrace = 5s、BackoffBase = 100ms、BackoffMax = 200ms、Jitter = 0.2，普通任务 timeout = 1min，超时案例 timeout = 1s；低负载计时与混合长任务不复用 `fastConfig` 的 400ms 租约。每 worker 执行池 MaxConns = 8、审计池 MaxConns = 2，提交 / 观察池预算另列，并计入 LISTEN 所占连接；开跑前核对总连接预算。连接配置属于宿主测试池，不新增 Engine Config 字段。任何配置变化都生成新一轮结果，不能混合不同参数的分位数。

### 11.5 实施顺序与交付

先实现 smoke 的提交清单、逐次审计和对账，再补完整批次 / 进程数对照，然后增加故障及持续投递 / 稳态。每步先用能明确失败的小案例验证核查器，例如故意重复一次业务效果或制造错误输出；核查器没有被验证前，不用“全部通过”作为结论。

入口为 `TestScenarioAcceptance`。**smoke 已可运行，full / soak 以下仍是计划接口，当前调用会明确失败**：

```sh
# 3 进程，1,000 次普通任务 + 10 个工作流，并覆盖低负载精度
mkdir -p /tmp/skein-m7-smoke
SKEIN_TEST_REQUIRE_DB=1 SKEIN_SCENARIO=1 SKEIN_SCENARIO_PROFILE=smoke go test -count=1 -parallel=1 -run '^TestScenarioAcceptance$' -timeout 15m -artifacts -outputdir=/tmp/skein-m7-smoke

# 待实现：1 / 3 / 6 进程完整批次、3 进程故障案例及定速分档
SKEIN_TEST_REQUIRE_DB=1 SKEIN_SCENARIO=1 SKEIN_SCENARIO_PROFILE=full go test -count=1 -parallel=1 -run '^TestScenarioAcceptance$' -timeout 45m -artifacts -outputdir=/tmp/skein-m7-full

# 待实现：3 进程，重新定档后运行 30 分钟稳态；总预算包含预热和排空
SKEIN_TEST_REQUIRE_DB=1 SKEIN_SCENARIO=1 SKEIN_SCENARIO_PROFILE=soak go test -count=1 -parallel=1 -run '^TestScenarioAcceptance$' -timeout 60m -artifacts -outputdir=/tmp/skein-m7-soak
```

- `SKEIN_SCENARIO` 未设置时跳过；设置后使用现有 `SKEIN_TEST_DSN`，未知 profile 或不可达 DB 必须报错。M7 案例不调用 `t.Parallel()`，也不与性能基线或普通全集同时测量，减少争抢机器导致的不可解释延迟；这不关闭 worker 内部或跨进程并发。
- 每个等待、故障控制、投递和排空阶段都有显式 deadline，所有阶段共享 profile 总预算。单次排空最多 10 分钟，实际期限取阶段上限与总剩余时间中的较小者，不能把每段 10 分钟无条件累加。根据 `t.Deadline()` 至少提前 60s 停止负载，留给有界的 worker 停止、制品保存和 schema 清理；smoke 将 worker 并发停止、制品与宿主 Engine 收尾放在一个 45s 预算内，随后 schema 清理另限 5s。总预算不足时报告当前阶段及未执行项并失败，不让最外层 `go test -timeout` 强杀后才收尾。sample / 事件流写入制品，不无限堆在父进程内存里；清理失败也必须记录。
- 制品放在 `t.ArtifactDir()`：运行清单、逐次事件和 Markdown 报告。显式运行必须加 Go 原生 `-artifacts -outputdir <目录>`，否则 ArtifactDir 会在测试结束后删除，不能保留证据；可用测试专用 `SKEIN_SCENARIO_OUT` 另外指定报告输出路径。报告包含 commit、种子、PG / Go / OS 版本、进程与连接预算、所有时长参数、目标 / 实际提交速率、预期 / 实际状态、缺测和失败证据，以及机器负载。不同 PG 版本缺少的统计字段标注 N/A，不跳过正确性用例；`active_time` 不是 CPU，混合负载也不套用 M5 受控场景的心跳 HOT 折算公式。
- 首版在独立测试环境验证 PG 13 与 18；性能数字按版本、机器分别保存。普通 CI 仍执行现有检查，M7 显式运行，不把 30 分钟长跑塞进每次提交。实现后 README 增加运行入口，代表性结果与边界写入 `docs/scenarios.md`，不修改 M5 历史基线。只有 smoke、full 与稳态的必过条件及核查器负例都取得证据，才将 M7 标记为完成。

---

## 12. M8：两节点外部任务与纯 Snooze

状态：**已实现，PG 13 / 18 验收通过**（2026-09-08）。§6 / §8 已同步；新增验收、lint、完整数据库回归与三轮 race 均通过，记录见 §12.5。只复用现有队列和工作流，不修改 §3 DDL、§7.3 锁序、output 的成功结果含义或 Resume 的重置规则。

### 12.1 两节点与持久化

视频生成任务用宿主声明的两个普通 job 组成工作流；submit / poll 是业务角色，不是新的 Executor 接口或内置执行器：

```text
submit：提交外部任务 → succeeded，output = {task_id, deadline}
    ↓ Request.Deps[submit 的 job_name]
poll：查询一次 → 未完成则 Snooze；完成则 succeeded，output = 最终视频结果
```

- submit 成功结算后，task id 与固定 deadline 已在 PG；poll 每次领取都通过现有 `Request.Deps` 重新读取，进程重启不丢失。task id 是“提交动作”的成功结果，不是运行中 checkpoint。
- poll 在多次 Snooze 期间仍未成功，自己的 output 不写入中间状态，后继仍 blocked，父工作流仍 running。成功后才保存最终结果并推进后继。
- Resume 保持 §6.7：成功的 submit 不动；失败 / 取消的 poll 重置后继续读取同一个 task id 和 deadline。查询节点自身的 output 仍按原规则清空。要重新生成视频应重新 Trigger，不能把 Resume 当成重新提交。
- 纯 Snooze 同样适用于普通 job_run；两节点是本视频场景的建模选择，不是使用 Snooze 的强制条件。

### 12.2 纯 Snooze 接口

新增公共函数，保留现有 Executor 返回类型与 Request 字段：

```go
func Snooze(delay time.Duration) error
```

Executor 通过 `return nil, Snooze(delay)` 表示“本次检查正常结束，稍后再调用我”，不是执行失败。规则：

1. delay 必须 > 0；非正值返回 Permanent 参数错误，不新增公共 sentinel。转为数据库 interval 时按微秒精度向上取整，不能因截断或溢出变为零 / 负延迟。使用现有 pgx 原生 interval 参数传递截断后的微秒值，余数非零时在 SQL 补 1µs：不拼接总微秒数文本（PG 13 存在输入字段范围限制），也不在 Go 中构造可能溢出的向上取整 Duration。支持 24h，但 24h 不是框架的单次延迟上限。
2. 返回内部类型的控制错误，Worker 用 `errors.AsType` 识别，支持普通 `%w` 包装。失租、取消、停机与超时仍按现有规则优先处理；被显式 `Permanent` 包装的错误不按 Snooze 重排。
3. Snooze 不保存 Executor 返回的 output，即便非空也不写入；只有 succeeded 才走现有输出校验与写入。无需为 Request 增加 Output / Checkpoint，也不提供更新运行中 output 的接口。
4. 不增加或清零 attempt，不追加 errors，不走指数退避、不增加 released 告警。真实业务失败、超时、panic 与租约过期仍按原规则计数；`Request.Attempt` 是失败 / 中断计数 +1，不是查询次数或 Executor 总调用次数。
5. delay 由 Executor 每轮决定；框架不额外加抖动，不新增 Snooze 周期、次数上限或总期限的 Config。两节点的每次外部请求仍受各自 JobSpec.Timeout 限制。

### 12.3 结算、唤醒与恢复

新增内部结算 outcome `Snoozed`，不新增 RunState。仍由 `Store.Settle` 打开事务：节点先锁父 workflow_run，再以 `id + lease_token + state = running` 校验持有者；0 行依旧是 ErrLeaseLost，不能重试陈旧写入。未命中取消时，在同一条 UPDATE 中完成：

| 字段 | Snooze 的写入 |
|---|---|
| state / run_at | pending / `now() + delay`，now 为数据库事务时间 |
| lease_token / lease_owner / lease_expires_at | 全部 NULL |
| finished_at | NULL |
| attempt / errors | 不变 |
| params / output | 不变 |

- 与 Retry / Released 一样，UPDATE 内先应用 `cancel_requested OR wf_cancelling`：取消命中则 cancelled 并设置 finished_at，不能因 Snooze 回到 pending。节点仅在实际落库状态为终态时 propagate；取消与 fail-fast 继续使用原锁序与 SKIP LOCKED 路径。
- 结算后沿用 worker 的退出流程释放槽位。等待期间这个 run 没有 Executor 或租约，不为它续心跳；数据库仍保留同一行、同一 RunId 与去重占位。再次领取才生成新 token。
- 原有 pending 唤醒触发器、槽位释放唤醒、NextPendingAt 与轮询兜底全部复用；不加 timer 表、扫描循环或每个任务一个常驻 goroutine。run_at 是可领取时刻，不保证到点必有空闲槽位；延迟起点是结算事务时间，不是提交完成后的本地时刻。
- Snooze 已提交后，重启只需正常领取到期 pending。结算遇到数据库错误则保持 §6.5 的故障路径：留下 running 等租约过期重领，后续可能增加 interrupted 与 attempt；不能把“成功 Snooze 不计失败”扩大为“故障也不计失败”。
- `exec_duration` 复用现有指标，实际回 pending 的 Snooze 标为 `outcome=snoozed`，取消命中则为 cancelled；不误标 failed / released，不增加独立计数器或 errors 条目。

### 12.4 24h 与外部副作用边界

1. **24h 是业务等待期限，不是单次执行 timeout。** submit / poll 每次只做一个有界外部请求；不把等待写成占住 worker 的 24h 循环。固定 deadline 由业务确定并随 submit 的成功 output 保存，可来自已持久化的业务输入或外部任务的稳定创建时间；提交重试找回同一任务时也不能重新取“本轮时间 +24h”。
2. poll 每轮按同一个可信时间基准检查 deadline，将本次请求与下次 Snooze 限制在剩余预算内；超期按 Permanent 业务失败处理。Resume 不延长 deadline。Skein 不新增工作流级 deadline 或超时扫描；所有 Worker 或 PG 不可用时，无法保证在第 24h 准点终结，只能恢复执行后检查超期。
3. 无 deadline 的 Executor 可以一直 Snooze。§5 不再以 timeout 与 max_attempts 推导工作流总时长有界：它们约束单次执行和真实失败次数，不约束正常 Snooze 的总等待。
4. 外部提交成功但 submit 尚未成功落库时仍可能崩溃。业务必须用 `Request.IdempotencyKey` 配合供应商幂等提交或按稳定业务键找回任务；两节点不提供 exactly-once，也不靠进程内缓存 task id。
5. 取消保持现有框架语义，只停止本地执行 / 后续查询，不自动终止供应商任务。外部 Abort 属于宿主业务，本里程碑不新增 Submit / Poll / Abort 协议。

### 12.5 验收与交付

复用真实 PG、现有子进程和确定性交错夹具；不访问真实视频供应商、不实际等待 24h。下表各项用聚焦测试或子测试验收，普通测试随现有 CI 的 PG 13 / 18 矩阵执行：

| 验收 | 必过条件 | 覆盖测试 |
|---|---|---|
| 纯 Snooze 语义 | MaxAttempts=1 时仍能多次 Snooze 后成功；RunId、attempt、errors 与输入快照保持不变，中途 output 为空；无输出暂存。覆盖非法 / 极小 / 大时长、包装错误、Permanent 与 ctx 原因的优先级，以及 snoozed / cancelled 指标标签 | `TestSnoozePreservesRun`、`TestSnoozeInvalidAndPermanent`、`TestSnoozeContextWins`、`TestExecDurationLabelFollowsSettledState` |
| 到点与槽位 | slowPoll 下，以 DB 入口时刻减 run_at 验证不提前且延迟 ≤ 300ms；Concurrency=1 时，snoozing run 不阻塞另一个立即任务；验证 24h 延迟准确落库，再用夹具推进 run_at 验证可再次领取，不用真实 sleep 等一天 | `TestSnoozeDurationPersistence`、`TestSnoozeReleasesSlotAndWakesOnTime` |
| 重启与 fence | Snooze 已提交后结束原进程，由另一个进程继续同一 run；旧 token 的 Snooze 被拒绝；结算失败与过期重领仍保留既有 attempt / interrupted 规则，不把丢失的结算当成成功 | `TestSnoozeSurvivesProcessExit`、`TestStaleTokenCannotWrite`、`TestSnoozeFailedSettlementReclaimed` |
| 取消与并发 | pending 的普通 run / 工作流可取消；正在 Snooze 结算时取消命中不能重新排队；将 Snoozed 加入已有取消重排与工作流锁序回归，不改变候选冲突时跳过并由领取收敛的规则 | `TestSnoozePendingCanBeCancelled`、`TestCancelSeesRunReleasedMeanwhile`、`TestSettleCancelHit`、`TestRaceSnoozeVsWorkflowCancellation` |
| 两节点与期限 | submit 成功落库后，poll 多次 Snooze 不推进后继；重试 / 重启 / Resume 读取同一 task id 与原 deadline，submit 不重跑；只有 poll 成功才输出最终结果；用已过期 deadline 夹具验证超期失败，提交崩溃窗口由幂等模拟供应商验证 | `TestSnoozeWorkflowFixedDeadline`、`TestSnoozeWorkflowSubmitCrash` |

实现集中在 `errors.go`、`settle.go`、`internal/store/store.go` 与 `internal/store/queries/job_run.sql`，随后 `make sqlc`；复用现有 worker / claim / heartbeat 路径，不修改领取返回列、迁移或 schemaVersion。测试补入现有行为套件或必要的聚焦测试，README 与可运行示例展示两节点用法，不加入供应商依赖。

§4 / §5 的 attempt 与总期限说明、§6.5 的 Snoozed 分支、§6.11 的唤醒来源说明、§8 的函数 / Attempt 契约与指标标签，以及 §9 完成状态已同步。测试分布于 `snooze_test.go`、`reliability_test.go` 和 `race_test.go`；`example_test.go` 的 `ExampleSnooze` 提供可编译宿主示例。

2026-09-08 本地验收（Go 1.27.0，darwin/arm64）：

| PostgreSQL | `make test-ci` | `SKEIN_TEST_REQUIRE_DB=1 make race` |
|---|---|---|
| 13.23（临时 Docker，已清理） | 通过，25.757s | 通过，`-race -count=3`，83.368s |
| 18.4（Homebrew） | 通过，18.622s | 通过，`-race -count=3`，61.674s |

`make lint`（sqlc diff、gofmt、go vet）与 `git diff --check` 通过。PG 13 实测曾暴露大微秒数字面量越界，改为原生 interval 参数后，包含 24h 和最大 Duration 的精度测试均通过；未新增迁移或依赖。本次未运行 M7 显式负载或重测 M5 性能基线。
