# 性能基线（M5）

入口：`SKEIN_BENCH=1 go test -run TestPerformanceBaseline -v -timeout 20m`（`bench_test.go`）；用于同参数比较，不设 SLA。

共用配置：3 个 worker 进程 × Concurrency 16，心跳 1s、租约 4s，10 万条约 1KB params 的终态 `job_run`。并发提交 `short_1k`（5ms，1KB）4000 条、`short_256k`（5ms，250KB）400 条、`long_1k`（3s，1KB）240 条；PollInterval 见各轮。queue = `started_at − run_at`，exec = `finished_at − started_at`，trigger = `Jobs().Trigger` 耗时。

机器：Apple Silicon 笔记本，PostgreSQL 18.4（Homebrew，本机 socket），Go 1.27，测试与数据库同机。

PG13 缺少 `pg_stat_database.active_time`，报告记 N/A，其余测量照常。以下为 PG18 实测。

计数口径：启动次数 = Σ(attempt + 1)；心跳次数 = `job_run` 总更新 − 2 × 启动次数；HOT 比例 = HOT 更新 / 心跳次数。claim / settle 改索引列，不计 HOT。此估算仅适用于无 released / 取消 / Resume 的普通任务，不能外推其它混合负载或解释 CPU。统计先等空闲后端 10s 刷新，再确认连续 3s 不变。

09-07 10:27 / 10:28 的旧数据因按执行时长估算心跳数、提前取快照而失真（9696 − 9280 = 416 次非 claim/settle 更新，却报 663 HOT），已由同参数重跑替代。

## Baseline 2026-09-07 15:53

PollInterval = 1s；其余配置同上。

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

PollInterval = 100ms；其余配置同上。

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

1. **旧版吞吐受轮询限制。** 1s 轮询理论上限 3 × 16 / 1s = 48/s，实测 42/s；100ms 时为 184/s。M6 槽位释放唤醒后，5s 轮询达到 286 runs/s，见后续复跑。
2. **参数可压缩。** 24 个提交 goroutine（每组 8 个）约 8300 条/s；1KB / 250KB trigger p50 约 1ms / 5ms，250KB 的 p99 为 26–37ms，排队与执行延迟未变。payload 使用重复字符，TOAST 压缩率高，不能外推一般 250KB 文档。
3. **负载 HOT 与安静库判据分开。** 本轮 93%–94%（676/727、679/719）；安静库 `TestHeartbeatIsHot` >95% 仍通过（[判据](../README.md#验证判据)）。并发快照和页空间影响剪枝，差额未进一步定位。
4. **VACUUM 回收页内空间，不缩小索引文件。** 4640 条 pending 使领取索引达 168KB；手动 VACUUM 后死元组归零、页可复用，真正缩小需 REINDEX。autovacuum 周期行为未测。
5. **锁与 DB 忙碌度。** 1300 多次采样均无锁等待；active time 为 0.32–0.55 秒/墙钟秒，表示后端活跃时间，不是 CPU。同机测量偏保守。
6. **长任务排队。** p99 14–25s 来自 240 条 3s 任务共享 48 槽位（5 轮），不能作为调度开销。

## 2026-09-07 并发修复后的复跑

修复未启动节点取消加锁、停机与 Executor 登记互斥及失租结果丢弃后，以 1s / 100ms 轮询复跑。该普通任务负载不衡量工作流取消争用。

## Baseline 2026-09-07 16:47

PollInterval = 1s；其余配置同上。

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

PollInterval = 100ms；其余配置同上。

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

同机同场景，仅将 `PollInterval` 改为 M6 默认的 5s。通过 `SKEIN_BENCH_POLL=5s SKEIN_BENCH_OUT=… go test -run TestPerformanceBaseline` 追加报告。

## Baseline 2026-09-07 23:45

PollInterval = 5s；其余配置同上。

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

1. 槽位释放即再领，PollInterval 仅作兜底；评审方以 1s / 100ms / 5s 复跑得到 287 / 285 / 290 runs/s。
2. 4640 条在 0.7s 内提交，48 槽位约 16s 排空；短任务 queue p50 15s 属于排队。触发精度由 `wake_test.go` 以 DB 入口时刻减 run_at 验证 ≤300ms。
3. 心跳 HOT 98%（728/746）；领取索引峰值 160KB、死元组 9316，与旧基线同量级。
4. 159 次采样的锁等待最多 4、平均 0.09 个后端，来源未定位；候选为 SKIP LOCKED 竞争或 NOTIFY 提交串行化（设计 §3.4）。

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

## 2026-09-11 复跑

三轮沿用 09-09 的命令、机器与参数：Go 1.27.0、PostgreSQL 18.4（Homebrew），
3 进程 × 16 槽位、5s 轮询、1s 心跳、4s 租约、10 万条存量行，任务组成同上；均单独运行。
测量不含 Observer、提交 Trace 或高频 Health / Stats 读取，仅覆盖基础执行路径，
不能外推宿主追踪、回调队列或采集开销。心跳 SQL、索引及存储参数未变。

- extra / Trace：276 runs/s，心跳估算 HOT 97%；原始输出 `/tmp/skein-phase1-m5.log`。
- Observer：291 runs/s，心跳估算 HOT 97%；原始输出 `/tmp/skein-phase2-m5.log`。

最终版本（含健康快照与分类统计）入口通过（50.97s），原始输出 `/tmp/skein-phase3-m5.log`：

| 延迟 p50 / p95 / p99 | short_1k | short_256k | long_1k |
|---|---|---|---|
| trigger | 1ms / 5ms / 12ms | 5ms / 17ms / 36ms | 6ms / 15ms / 30ms |
| queue | 15.094s / 15.231s / 15.238s | 14.948s / 14.978s / 14.98s | 5.99s / 12.003s / 12.038s |
| exec | 7ms / 10ms / 15ms | 9ms / 21ms / 31ms | 3.002s / 3.015s / 3.023s |

| 指标 | 最终实测 |
|---|---|
| 提交 | 4640 条 / 738ms，6290 triggers/s |
| 完成 | 4640 条 / 16.1s，288 runs/s，0 failed |
| job_run 更新 | 10024 = 4640 claims + 4640 settles + 744 heartbeats；722 HOT，心跳估算比例 97% |
| idx_job_run_claim | 初始 16 KB、峰值及 VACUUM 后 160 KB；死元组 9319 → 0 |
| idx_job_run_running | 16 KB → 16 KB |
| DB active time | 0.59 s / 墙钟秒（不是 CPU） |
| 锁等待 | 155 次采样，最多 5、平均 0.12 个后端 |

三轮 M5 后均单独无缓存运行 `TestClaimUsesPartialIndexes` 与 `TestHeartbeatIsHot`，均通过；
负载估算 HOT 不替代安静库 >95% 的独立心跳判据。

未做同环境旧代码对照，不能把三轮差异归因于代码，也不能消除 09-09 记录的未解释差异；
这些结果不支持“性能无回退”。

## 2026-09-12 审查修复后的复跑

领取改为每第 8 个有效轮次优先回收过期租约，并在过期分支按 `lease_expires_at, id` 排序；索引、心跳 SQL 与存储参数未变。沿用上轮机器和参数，独立运行，没有与其它测试并行：Go 1.27.0、PostgreSQL 18.4（Homebrew），3 进程 × 16 槽位、5s 轮询、1s 心跳、4s 租约、10 万条存量行，任务组成同上。

```sh
SKEIN_TEST_REQUIRE_DB=1 SKEIN_BENCH=1 SKEIN_BENCH_POLL=5s \
  SKEIN_BENCH_CONCURRENCY=16 \
  go test -count=1 -parallel=1 -run '^TestPerformanceBaseline$' -v -timeout 20m
```

基于未提交工作区；Go / SQL / 构建配置及测试源码摘要为 `5c52d9b9741a28ce9e6eaa181966216799ec467f2bb741e47ac813f7d25997f1`。入口通过（48.93s），原始报告及日志保存在本地 `skein-fix-validation` 交付制品中，包含源码归档与逐文件摘要。

| 延迟 p50 / p95 / p99 | short_1k | short_256k | long_1k |
|---|---|---|---|
| trigger | 1ms / 4ms / 11ms | 5ms / 17ms / 64ms | 4ms / 19ms / 52ms |
| queue | 15.142s / 15.375s / 15.387s | 14.992s / 15.014s / 15.019s | 5.993s / 12.005s / 12.029s |
| exec | 7ms / 11ms / 20ms | 9ms / 17ms / 36ms | 3.002s / 3.012s / 3.022s |

| 指标 | 实测 |
|---|---|
| 提交 | 4640 条 / 695ms，6675 triggers/s |
| 完成 | 4640 条 / 16.254s，285 runs/s，0 failed |
| job_run 更新 | 10016 = 4640 claims + 4640 settles + 736 heartbeats；674 HOT，心跳估算比例 92% |
| idx_job_run_claim | 初始 16 KB、峰值及 VACUUM 后 160 KB；死元组 9619 → 0 |
| idx_job_run_running | 16 KB → 16 KB |
| DB active time | 0.56 s / 墙钟秒（不是 CPU） |
| 锁等待 | 160 次采样，最多 3、平均 0.07 个后端 |

随后分别在安静的 PG18.4 与 PG13.23 上独立、无缓存执行 `TestClaimUsesPartialIndexes` 和 `TestHeartbeatIsHot`，均通过。该判据不替代负载下的估算：与 09-11 的 288 runs/s、HOT 97% 相比，本次吞吐接近、HOT 降至 92%；未做同环境旧代码对照，差异原因未定位，不能宣称性能无回退。没有调整阈值或存储参数。

M5 不含计划负载，不能证明新增名称锁及大量历史拍的跨表在途检查成本；计划行为另由确定性交错测试及 M7 smoke 验证，不据此外推容量。

## 2026-09-12 全量 review 修复复跑

沿用上轮 M5 参数：Go 1.27.0、PG18.4（Homebrew）、3 进程 × 16 槽位、5s 轮询、1s 心跳、4s 租约、10 万条存量行。本任务的数据库测试与测量串行执行。当前 M5 显式开启后强制数据库可达并校验参数，保存报告后校验每组完成数量与零非预期失败。

基于未提交工作区，可执行源码、测试及构建配置清单摘要为 `b799805f5c9fa57025a8cd1df4b817d8b0df458d600669986fe1db020d10a53d`；源码归档、逐文件摘要及原始日志随本轮交付保存，尚无对应远程 CI 运行。

| 指标 | 实测 |
|---|---|
| 入口 / 完成 | 49.80s；4640 条全部成功，0 failed |
| 提交 / 完成吞吐 | 6148 triggers/s；287 runs/s，排空 16.162s |
| 更新 / 估算心跳 HOT | 10018 次更新；738 次心跳、726 HOT，98% |
| 领取索引 | 16KB → 峰值 160KB；VACUUM 后 160KB |
| 死元组 | 9296 → 0 |
| DB active time / 锁等待 | 0.57 s / 墙钟秒；最多 3、平均 0.06 个后端 |

之后独立、无缓存执行领取索引与心跳 HOT 检查，均通过。与上次 285 runs/s、估算 HOT 92% 的差异未做同环境旧代码对照，不能据此归因或宣称性能无回退。

工作流往返问题另以真实 PG 的 500 节点探针定位：每条语句注入 30ms 延迟，原逐节点插入在约 309 条语句、10.03s 时超时；同一探针改为批量插入后 6 条语句、约 215ms 完成。回归测试同时核对节点快照、依赖状态、插入失败整笔回滚与去重。该对照仅证明消除了逐节点往返；注入延迟不是实际网络测量，不证明 50 个大工作流同批或大量历史计划拍的容量。
