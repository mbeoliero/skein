# 性能基线（M5）

来源：`SKEIN_BENCH=1 go test -run TestPerformanceBaseline -v -timeout 20m`（`bench_test.go`）。不设 SLA，只留数字供后续对比。

场景：3 个 worker 进程（测试二进制的 bench 模式）、10 万条存量终态 `job_run`（约 1KB params）、三组任务并发提交：`short_1k`（5ms，1KB）4000 条、`short_256k`（5ms，250KB）400 条、`long_1k`（3s，1KB）240 条。queue = `started_at − run_at`，exec = `finished_at − started_at`，trigger = `Jobs().Trigger` 调用耗时。心跳间隔 1s、租约 4s，让长任务经历几次心跳以便观察 HOT 比例。

机器：Apple Silicon 笔记本，PostgreSQL 18.4（Homebrew，本机 socket），Go 1.27，测试与数据库同机。

兼容性：基线入口也支持 PG 13；该版本没有 `pg_stat_database.active_time`，报告标记 N/A，不填零，其余测量照常。以下历史 PG 18 数据保持原样。

计数口径：心跳次数 = `job_run` 总更新 − 2 × 启动次数（每次启动一条 claim、一条 settle，两者都改索引列，永不 HOT；启动次数 = Σ(attempt + 1)）；HOT 比例 = HOT 更新 / 心跳次数。此估算仅适用于无 released / 取消 / Resume 的普通任务，不能外推到混合负载或解释 CPU。`pg_stat_user_tables` 的快照在等过空闲后端的 10 s 刷新超时、再连续 3 s 不变之后才取。同日 10:27 / 10:28 的两次运行用执行时长估算心跳数、且快照取早了，HOT 一行不自洽（9696 − 9280 = 416 条非 claim/settle 更新却报 663 HOT），已由下面同参数的重跑替代。

## Baseline 2026-09-07 15:53

Workers 3 processes × Concurrency 16, PollInterval 1s, HeartbeatInterval 1s, LeaseTTL 4s; 100k seeded rows; PostgreSQL 18.4 (Homebrew).

| group | runs | payload | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |
|---|---|---|---|---|---|
| short_1k | 4000 | 1 KB | 0s / 4ms / 7ms | 1m7.9s / 1m45.712s / 1m48.804s | 11ms / 23ms / 34ms |
| short_256k | 400 | 250 KB | 5ms / 9ms / 37ms | 28.466s / 39.693s / 40.664s | 20ms / 32ms / 58ms |
| long_1k | 240 | 1 KB | 4ms / 7ms / 33ms | 11.558s / 23.576s / 24.563s | 3.012s / 3.036s / 3.055s |

| measure | value |
|---|---|
| submitted | 4640 runs in 556ms (8351 triggers/s) |
| completed | 4640 runs in 1m50.399s (42 runs/s end to end), 0 failed |
| job_run updates | 10007 total = 4640 claims + 4640 settles + 727 heartbeats; 676 HOT = 93% of heartbeats |
| idx_job_run_claim | 16 KB before, 168 KB peak, 168 KB after VACUUM; dead tuples 0 before / 0 after VACUUM |
| idx_job_run_running | 16 KB before, 16 KB after |
| DB active time | 0.32 s per wall second (pg_stat_database.active_time) |
| lock waits | max 0, mean 0.00 backends waiting on a lock over 1087 samples every 100 ms |

## Baseline 2026-09-07 15:54

Workers 3 processes × Concurrency 16, PollInterval 100ms, HeartbeatInterval 1s, LeaseTTL 4s; 100k seeded rows; PostgreSQL 18.4 (Homebrew).

| group | runs | payload | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |
|---|---|---|---|---|---|
| short_1k | 4000 | 1 KB | 1ms / 3ms / 6ms | 20.317s / 24.121s / 24.465s | 10ms / 20ms / 32ms |
| short_256k | 400 | 250 KB | 5ms / 10ms / 26ms | 16.295s / 17.419s / 17.535s | 14ms / 31ms / 47ms |
| long_1k | 240 | 1 KB | 3ms / 10ms / 19ms | 6.572s / 13.332s / 13.83s | 3.007s / 3.014s / 3.019s |

| measure | value |
|---|---|
| submitted | 4640 runs in 555ms (8355 triggers/s) |
| completed | 4640 runs in 25.228s (184 runs/s end to end), 0 failed |
| job_run updates | 9999 total = 4640 claims + 4640 settles + 719 heartbeats; 679 HOT = 94% of heartbeats |
| idx_job_run_claim | 16 KB before, 168 KB peak, 168 KB after VACUUM; dead tuples 9333 before / 0 after VACUUM |
| idx_job_run_running | 16 KB before, 16 KB after |
| DB active time | 0.55 s per wall second (pg_stat_database.active_time) |
| lock waits | max 0, mean 0.00 backends waiting on a lock over 249 samples every 100 ms |

## 解读

1. **短任务吞吐被轮询周期封顶。** 每实例每个 `PollInterval` 最多领 `Concurrency` 条，3 × 16 / 1s = 48/s 是理论上限，实测 42/s；轮询 100ms 时 184/s。这是当时采用 1s 轮询的取舍。事件唤醒（现设计 §2.7）之后槽位释放即唤醒领取，这个上限已解除：下面"M6 之后的复跑"在 5s 轮询下达到 286 runs/s，与轮询周期无关。
2. **提交很便宜。** `Trigger` 是一条 INSERT … SELECT：1KB p50 约 1ms，250KB p50 5ms；每组 8 个、共 24 个 goroutine 并发约 8300 条/s。250KB payload 只抬高触发 p99（26 到 37ms），本组的排队与执行延迟没有变化（jsonb 走 TOAST）。注意 payload 是 `strings.Repeat` 的重复字符，TOAST 压缩后落盘很小：这是可压缩 payload 的基线，不能推广到一般 250KB 文档的存储成本或延迟。
3. **心跳 HOT 比例 93% 到 94%**（676 / 727、679 / 719），按上面的口径直接从计数器算出，不再估算。这低于 M1 单测在安静库上的 > 95%（`TestHeartbeatIsHot`，仍通过，判据见 [README](../README.md#验证判据)）；负载下的差额来自并发快照与页内空间让页内剪枝并非每次都成立，未进一步定位。claim 与 settle 改的是索引列，永远不是 HOT，属预期。
4. **`idx_job_run_claim` 峰值 168KB**（4640 条 pending 同时入队），手动 VACUUM 后死元组归零但磁盘大小不回落（autovacuum 周期内的行为未测）：B-tree 页只标记可重用，不归还给文件系统。原验收所说"回落"在实践中的含义是页面被重用、大小由历史峰值积压决定而不随时间增长；要真正缩小只能 REINDEX。
5. **锁等待为零**：两次运行的 1300 多次采样里没有后端在等锁。数据库忙碌度 0.32 到 0.55 秒/秒（`pg_stat_database.active_time`，后端处于 active 状态的时间，不是 CPU 时间），测试进程与数据库同机，数值偏保守。
6. **长任务排队 p99 14 到 25s** 来自 240 条 3s 任务共享 48 个槽位（5 轮 × 3s），不是调度开销。

## 2026-09-07 并发修复后的复跑

本轮修改未启动节点取消的候选加锁、停机与 Executor 登记的互斥，以及失租移除时的结果丢弃标记。按上面相同配置分别复跑 1s / 100ms 轮询，保留原结果供比较；以下仍是普通任务混合负载，不衡量工作流取消的争用性能。

## Baseline 2026-09-07 16:47

Workers 3 processes × Concurrency 16, PollInterval 1s, HeartbeatInterval 1s, LeaseTTL 4s; 100k seeded rows; PostgreSQL 18.4 (Homebrew).

| group | runs | payload | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |
|---|---|---|---|---|---|
| short_1k | 4000 | 1 KB | 0s / 2ms / 5ms | 1m7.797s / 1m44.937s / 1m48.295s | 12ms / 24ms / 40ms |
| short_256k | 400 | 250 KB | 4ms / 8ms / 30ms | 29.625s / 41.595s / 43.101s | 17ms / 32ms / 91ms |
| long_1k | 240 | 1 KB | 3ms / 13ms / 20ms | 12.074s / 23.854s / 24.498s | 3.011s / 3.018s / 3.022s |

| measure | value |
|---|---|
| submitted | 4640 runs in 416ms (11154 triggers/s) |
| completed | 4640 runs in 1m49.726s (42 runs/s end to end), 0 failed |
| job_run updates | 10002 total = 4640 claims + 4640 settles + 722 heartbeats; 676 HOT = 94% of heartbeats |
| idx_job_run_claim | 16 KB before, 160 KB peak, 160 KB after VACUUM; dead tuples 0 before / 0 after VACUUM |
| idx_job_run_running | 16 KB before, 16 KB after |
| DB active time | 0.31 s per wall second (pg_stat_database.active_time) |
| lock waits | max 0, mean 0.00 backends waiting on a lock over 1080 samples every 100 ms |

## Baseline 2026-09-07 16:48

Workers 3 processes × Concurrency 16, PollInterval 100ms, HeartbeatInterval 1s, LeaseTTL 4s; 100k seeded rows; PostgreSQL 18.4 (Homebrew).

| group | runs | payload | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |
|---|---|---|---|---|---|
| short_1k | 4000 | 1 KB | 0s / 3ms / 5ms | 20.43s / 24.286s / 24.626s | 11ms / 21ms / 34ms |
| short_256k | 400 | 250 KB | 4ms / 9ms / 31ms | 16.511s / 17.788s / 17.884s | 15ms / 23ms / 26ms |
| long_1k | 240 | 1 KB | 3ms / 11ms / 24ms | 6.587s / 13.3s / 13.858s | 3.007s / 3.015s / 3.034s |

| measure | value |
|---|---|
| submitted | 4640 runs in 450ms (10311 triggers/s) |
| completed | 4640 runs in 25.323s (183 runs/s end to end), 0 failed |
| job_run updates | 10016 total = 4640 claims + 4640 settles + 736 heartbeats; 692 HOT = 94% of heartbeats |
| idx_job_run_claim | 16 KB before, 168 KB peak, 168 KB after VACUUM; dead tuples 9336 before / 0 after VACUUM |
| idx_job_run_running | 16 KB before, 16 KB after |
| DB active time | 0.52 s per wall second (pg_stat_database.active_time) |
| lock waits | max 8, mean 0.03 backends waiting on a lock over 249 samples every 100 ms |

## 2026-09-07 M6 精准触发后的复跑

同一台机器、同一场景，worker 的 `PollInterval` 改为 5s（M6 默认值），其余参数不变。报告由 `SKEIN_BENCH_POLL=5s SKEIN_BENCH_OUT=… go test -run TestPerformanceBaseline` 追加。

## Baseline 2026-09-07 23:45

Workers 3 processes × Concurrency 16, PollInterval 5s, HeartbeatInterval 1s, LeaseTTL 4s; 100k seeded rows; PostgreSQL 18.4 (Homebrew).

| group | runs | payload | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |
|---|---|---|---|---|---|
| short_1k | 4000 | 1 KB | 1ms / 4ms / 11ms | 15.127s / 15.325s / 15.331s | 7ms / 11ms / 17ms |
| short_256k | 400 | 250 KB | 5ms / 18ms / 43ms | 14.986s / 15.02s / 15.025s | 9ms / 14ms / 37ms |
| long_1k | 240 | 1 KB | 4ms / 17ms / 34ms | 5.987s / 12.008s / 12.035s | 3.002s / 3.009s / 3.027s |

| measure | value |
|---|---|
| submitted | 4640 runs in 700ms (6625 triggers/s) |
| completed | 4640 runs in 16.241s (286 runs/s end to end), 0 failed |
| job_run updates | 10026 total = 4640 claims + 4640 settles + 746 heartbeats; 728 HOT = 98% of heartbeats |
| idx_job_run_claim | 16 KB before, 160 KB peak, 160 KB after VACUUM; dead tuples 9316 before / 0 after VACUUM |
| idx_job_run_running | 16 KB before, 16 KB after |
| DB active time | 0.53 s per wall second (pg_stat_database.active_time) |
| lock waits | max 4, mean 0.09 backends waiting on a lock over 159 samples every 100 ms |

解读：

1. **吞吐不再被轮询封顶。** 5s 轮询下 286 runs/s，M6 之前 1s 轮询是 42/s、100ms 轮询是 184/s。槽位释放即再领，`PollInterval` 只剩兜底作用；评审方在 1s / 100ms / 5s 三档复跑得到 287 / 285 / 290，与此一致。
2. **queue 延迟是积压，不是调度延迟。** 4640 条在 0.7s 内提交、48 个槽位 16s 排空，短任务 queue p50 15s 就是排队时长；M6 的触发精度由 `wake_test.go` 以 Executor 入口时刻（DB 时钟）减 `run_at` 在 CI 断言（≤ 300ms），不由这个基线量。
3. **心跳 HOT 98%**（728 / 746），触发器不影响 HOT。
4. **锁等待出现了**：159 次采样里最多 4 个后端在等锁、平均 0.09，之前两次为 0。来源未定位，候选是同时被唤醒的三个实例在同一批到期行上的 `SKIP LOCKED` 竞争，或带通知的提交串行化（现设计 §3.4）；量级很小，先记录。
5. `idx_job_run_claim` 峰值 160KB、死元组 9316，与 M5 的 168KB / 同量级一致。

## 2026-09-09 审查修复后的复跑

本轮修复注册的生命周期互斥、工作流 Resume 的校验与本地唤醒、心跳已观察取消被先前 context 原因掩盖，以及停机后领取 / 扫描追加轮次。未改 SQL、迁移或存储参数。基于当前未提交工作区，沿用上次 M6 基线参数，独立执行（未与本轮其它测试并行）：

```sh
SKEIN_TEST_REQUIRE_DB=1 SKEIN_BENCH=1 SKEIN_BENCH_POLL=5s \
  SKEIN_BENCH_CONCURRENCY=16 \
  go test -count=1 -run '^TestPerformanceBaseline$' -v -timeout 20m
```

Go 1.27.0、PostgreSQL 18.4（Homebrew）；3 个进程 × 16 槽位、心跳 1s、租约 4s、10 万条存量行，任务组成同上。入口通过（63.49s），不等于性能无回退。

| 任务组 | 数量 | 参数 | trigger p50 / p95 / p99 | queue p50 / p95 / p99 | exec p50 / p95 / p99 |
|---|---|---|---|---|---|
| short_1k | 4000 | 1 KB | 1ms / 9ms / 23ms | 20.972s / 24.317s / 24.775s | 49ms / 124ms / 179ms |
| short_256k | 400 | 250 KB | 9ms / 28ms / 92ms | 15.572s / 17.218s / 17.406s | 63ms / 184ms / 327ms |
| long_1k | 240 | 1 KB | 8ms / 31ms / 87ms | 6.623s / 13.004s / 13.19s | 3.016s / 3.126s / 3.29s |

| 指标 | 实测 |
|---|---|
| 提交 | 4640 条 / 1.167s，3976 triggers/s |
| 完成 | 4640 条 / 26.602s，174 runs/s，0 failed |
| job_run 更新 | 10035 = 4640 claims + 4640 settles + 755 heartbeats；681 HOT，心跳估算比例 90% |
| idx_job_run_claim | 初始 16 KB、峰值及 VACUUM 后 160 KB；死元组 9386 → 0 |
| idx_job_run_running | 16 KB → 16 KB |
| DB active time | 3.40 s / 墙钟秒（不是 CPU） |
| 锁等待 | 242 次采样，最多 7、平均 0.30 个后端 |

**未解决的测量差异**：相比上次 286 runs/s、心跳 HOT 98%，本次吞吐降至 174、HOT 90%，短任务执行尾延迟与 DB active time 上升。未做同环境旧代码对照，也未定位锁等待或主机 / 数据库其它负载，不能归因于本轮代码或仅归因于环境；此结果不支持“性能无回退”。未调整阈值或存储参数。

本轮 `make lint`、required-DB 无缓存全集、默认并发三轮 race 均通过；M5 后单独无缓存执行 `TestHeartbeatIsHot` 也通过，其安静库 >95% 判据与上述负载估算不是同一指标。未运行 PG13 或 M7。
