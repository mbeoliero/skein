-- design §3; the schema is created and selected by skein.Migrate

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
