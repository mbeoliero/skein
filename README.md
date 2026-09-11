# Skein

嵌入 Go 宿主进程的 PostgreSQL 任务调度库：一次性与延迟任务、cron、DAG 工作流、重试、Snooze、取消、续跑和优雅停机。多个实例共享数据库，无需独立调度服务、选主、Redis 或消息队列。

## 安装

需要 [Go 1.27](https://go.dev/dl/) 和 PostgreSQL **≥ 13**；本地开发使用 18.4，CI 覆盖 13 / 18。在宿主模块目录执行（使用库无需 sqlc）：

```sh
go version # 应为 go1.27.x 或更新版本
go get github.com/mbeoliero/skein
```

## 快速开始

准备数据库并替换连接信息；`sslmode=disable` 仅供本地开发，远程连接应配置 TLS。

```sh
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable'
```

将代码保存为宿主项目的 `main.go`，执行 `go run .`，Ctrl+C 停机。示例内迁移便于首次运行；**生产环境将 `Migrate` 放在发布步骤串行执行，勿由每个副本启动时调用。**泛型解码与定时计划示例见 [example_test.go](example_test.go)。

```go
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("set DATABASE_URL before starting")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := skein.Migrate(ctx, pool, "skein"); err != nil {
		return err
	}
	engine, err := skein.New(pool, skein.Config{Concurrency: 8})
	if err != nil {
		return err
	}
	engine.Register("echo", func(ctx context.Context, req *skein.Request) (skein.RawJSON, error) {
		return req.Params, nil
	})
	if err := engine.Jobs().Declare(ctx, skein.JobSpec{Name: "hello", ExecutorType: "echo"}); err != nil {
		return err
	}
	if err := engine.Start(ctx); err != nil {
		return err
	}
	id, err := engine.Jobs().Trigger(ctx, "hello", skein.RawJSON(`{"message":"hello"}`))
	if err != nil {
		return errors.Join(err, engine.Shutdown(context.Background()))
	}
	log.Printf("created run %d", id)
	<-ctx.Done()
	return engine.Shutdown(context.Background())
}
```

### 执行契约

1. **至少一次投递。**外部副作用须用 `Request.IdempotencyKey` 去重；提交去重与防重叠见[去重键](#去重键)。
2. **数据库和连接池由宿主管理。**`Migrate` 创建 schema 与表，不创建数据库；`Start` 只校验版本。自定义 `Config.Schema` 时，迁移须使用同一名称；名称须为有效 UTF-8、无 NUL、最多 63 字节（空值使用默认）；连接池建议至少 5 个连接：监听独占 1 条，其余留给领取、心跳与结算。
3. **Executor 应响应 ctx 取消。**`Shutdown` 不关闭连接池；应先处理它的返回值，再关闭宿主资源。`ErrNotDrained` 表示仍有执行器未退出，其租约将由其他兼容实例回收。

### 观测

`Trigger` / `TriggerTx` 自动传递 ctx 中有效的 OTel 追踪上下文。宿主配置 SDK / exporter，在 Executor 内用 `tracer.Start(ctx, …)` 创建 span。排障可读 `Request.TraceId` / `ExecutionId`；元数据通过 `JobRun.Extra` / `WorkflowRun.Extra` 查询，由库管理。

`Config.Observer` 接收提交后事件；回调须并发安全、快速返回，慢操作交给宿主有界队列：

```go
events := make(chan skein.Event, 128)
cfg.Observer = skein.ObserverFunc(func(_ context.Context, event skein.Event) {
	select {
	case events <- event:
	default:
		slog.Warn("observer queue full", "event_id", event.EventId)
	}
})
```

`engine.Health()` 读取本进程进展与错误；`engine.Stats(ctx)` 返回共享队列总量与 `ByExecutor` 分类积压。事件字段见 [observer.go](observer.go)，观测口径见[设计 §3.5](docs/design.md#35-日志与观测)。

**schema 1 → 2：**先停旧实例，执行新版 `Migrate`，再启动新版实例；迁移将 `lease_owner` 搬入 `extra` 并删除原列，旧版 SQL 不兼容。已有状态、token 与租约到期时间保留。

**计划协议升级：**采用计划名称锁的版本仍使用 schema 2。升级时先暂停旧版计划写入端（Put / Delete、扫描、计划拍 Resume），统一升级后再恢复；仅检查 schema 版本不能阻止旧代码绕过名称锁。普通执行快照和幂等键保持不变。

### 去重键

`DedupKey(k)` 按 job / workflow 名各自隔离，在保留窗口内同键只对应一个 run，不看状态：

1. `ErrDuplicate` 的非零 id 指向已有 run，可能已终态；用 `Runs().Get` 查询，仅 id=0（持有者被清理）时重试。新 params / `At(t)` 不会覆盖首次提交。
2. failed / cancelled 用 `Runs().Resume(ctx, id)` 重跑，保留 id、快照与键；成功实例不能 Resume，再跑须换键。
3. 成功占键至 `RetentionSucceeded`（默认 7 天），失败 / 取消至 `RetentionFailed`（默认 30 天）；清理后同键可新建。更长幂等由 retention 或业务记录保证。
4. 键包含业务身份与周期，如 `invoice:2026-09`；固定 `"nightly"` 键会变成“7 天只跑一次”。防重叠用 `ScheduleSpec.Overlap = OverlapSkip`。

计划拍次由 `(schedule_name, scheduled_at)` 单独保证一拍一行，与手动 Trigger 的键互不影响。

### 执行器取消与手工续跑

Executor 返回 `nil, skein.Cancel(err)` 会取消本 run；节点会取消整个工作流，成功节点及其 output 保留。在跑兄弟通过心跳收到取消。原因保存在 run.Errors（kind=cancelled），不消耗 attempt，返回的 output 被忽略。支持 `%w` 包装，`Cancel(nil)` 返回 nil；上下文取消 / 停机 / 超时、失租及显式 `Permanent` 优先于 Cancel，Cancel 优先于 Snooze。

```go
return nil, skein.Cancel(fmt.Errorf("external task stopped: %w", err))
```

普通任务耗尽自动重试或取消后，调用 `e.Runs().Resume(ctx, runId)`：同一 id、输入快照、历史和幂等键，重置重试预算并立即排队。仅 failed/cancelled 可续跑；其它状态返回 `ErrNotResumable`；`overlap = skip` 的计划拍在下一拍仍在途时返回 `ErrDuplicate`，不改变旧 run。工作流节点不能单独续跑，使用 `e.Workflows().Resume(ctx, workflowRunId)`。

工作流若在取消与成功交错后成为“父 cancelled、全部节点 succeeded”，Resume 返回 `ErrNotResumable`，保留原终态与成功结果。计划拍 Resume 遵循当前计划的重叠规则：allow 改为 skip 后，历史 allow 拍同样参与在途检查；已有执行继续，新拍及恢复须等其它拍结束。同名删除重建或更换目标沿用这个范围，改名才是独立计划。

### 外部长任务：两节点 + Snooze

视频生成等“先提交、再查询”的任务使用一个两节点工作流：

```text
video.submit → 成功 output 保存 task_id 和固定 deadline
    ↓
video.poll   → 从 Request.Deps["video.submit"] 读取，每次只查询一次
               未完成：return nil, skein.Snooze(30 * time.Second)
               已完成：返回最终结果和 nil
```

Snooze 将**同一个 run** 延迟回 pending，释放槽位、租约与 goroutine，等待期间不需心跳。它不增加 attempt / errors、不保存 output，`Request.Attempt` 不能用作查询次数。delay 须为正，可超过 24h；查询错误仍按 RetryPolicy 处理，多个进程可接续查询。

**总等待 24h 由业务 deadline 约束，不是把 JobSpec.Timeout 设成 24h。** deadline 来自稳定业务输入或外部任务创建时间，随提交的成功 output 保存；重试找回同一任务时不能重置为“本轮时间 +24h”。查询使用同一个可信时间基准，限制本次请求及下一次 Snooze 不越过剩余预算，超期返回 Permanent 业务失败。Resume 保留成功的 submit，只续跑查询节点，不延长 deadline；重新生成应重新 Trigger。供应商调用仍需使用 `Request.IdempotencyKey` 做幂等；取消工作流不会自动 Abort 外部任务。

完整宿主写法见 [example_test.go 的 ExampleSnooze](example_test.go)（模拟供应商，Go 编译检查）；可运行的两节点、期限及恢复场景见 [snooze_test.go](snooze_test.go)：

```sh
SKEIN_TEST_REQUIRE_DB=1 go test -count=1 -run '^TestSnooze' -v
```

## 开发与测试

集成测试在真实 PostgreSQL 中创建并清理随机 schema；使用有相应创建 / 删除权限的专用测试账户与数据库。`SKEIN_TEST_DSN` 默认为本机 socket `postgres:///postgres?host=/tmp`，也可指定 TCP：

```sh
export SKEIN_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable'
go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
export PATH="$(go env GOPATH)/bin:$PATH"
make lint
make test-ci
make race # 默认整体超时 20m，可用 RACE_TIMEOUT 覆盖
```

| 命令 | 行为 |
|---|---|
| `make test` | 普通测试；数据库不可达时跳过集成测试，可能复用 Go 测试缓存 |
| `make test-ci` | 必须连接数据库；`-count=1` 禁止测试缓存，不可达则失败 |
| `make race` | race detector，重复 3 次；可加 `SKEIN_TEST_REQUIRE_DB=1` 强制数据库可达 |
| `make lint` | 检查 sqlc 生成结果、gofmt 和 go vet |
| `make sqlc` | 修改 SQL 后重新生成存储代码 |

[CI](.github/workflows/ci.yml) 在 push / PR / 手动触发时覆盖 Go 1.27、PG13/18：先查 TCP 健康与测试 DSN，再运行 `make lint && make test-ci && make race`，全程强制数据库可达。

性能基线不会随普通测试运行；显式开启后数据库不可达、参数非法、任务数量不符或非预期失败都会使测试失败，先保存报告再判定工作负载。需要时单独执行：

```sh
SKEIN_TEST_REQUIRE_DB=1 SKEIN_BENCH=1 go test -count=1 -run '^TestPerformanceBaseline$' -v -timeout 20m
```

M7 smoke 使用 3 个真实进程验证混合任务、工作流、精度、事务与 Snooze 槽位复用；与 M5 一样，须独立运行：

```sh
mkdir -p /tmp/skein-m7-smoke
SKEIN_TEST_REQUIRE_DB=1 SKEIN_SCENARIO=1 SKEIN_SCENARIO_PROFILE=smoke \
  go test -count=1 -parallel=1 -run '^TestScenarioAcceptance$' -timeout 15m \
  -artifacts -outputdir=/tmp/skein-m7-smoke -v
```

必须加 `-artifacts` 保留证据。配置、报告口径与实测见 [场景验收](docs/scenarios.md)；full / soak 尚未实现，M7 未完成。

手动 [Manual validation](.github/workflows/validation.yml) 可选 `both` / `m5` / `m7-smoke`。PG13/18 使用独立 runner、内部串行测量；失败也保留日志、版本、配置与原始制品 30 天。PR / 发布记录引用具体运行，长期证据另行归档，跨机器数据不作前后性能对照。

### 验证判据

下列判据对应现有行为测试，不是生产 SLA；M7 混合负载与未实现的 full / soak 仍按[场景验收](docs/scenarios.md)独立判断。

| 机制 | 必过条件与入口 |
|---|---|
| 领取与租约 | 两支领取及每类型到点读取使用各自索引；安静库心跳 HOT >95%；失败则评审 run_at 兼作租约到期、合并领取的单列退路，接受心跳写索引，不叠加参数。无持续积压、数据库可用且有空槽的独立窗口中，60s 租约配置下 kill 后 ≤75s 接管且 attempt +1；暂停超过 60s 恢复后旧 token 不可写；预算耗尽重领直接 failed。持续双类积压时，每 8 个有效领取轮次至少一次优先回收，单槽也须双方进展；这不构成满载时的秒数 SLA。见 [store_test.go](store_test.go)、[reliability_test.go](reliability_test.go)、[lease_process_test.go](lease_process_test.go)、[claim_fairness_test.go](claim_fairness_test.go)；真实 60s 租约验收单轮约 65–90s。 |
| 工作流与锁序 | 空定义拒绝；100 个并行前驱只激活一次汇合；fail-fast 收敛全部节点；Resume 不重跑成功节点且 started_at 不变；关键事务对并发 1000 轮无死锁，取消须在领取未提交时完成，并覆盖提交与回滚。清理不得删除在途工作流的已完成节点，或被 Resume 改回运行中的父行。见 [workflows_test.go](workflows_test.go)、[race_test.go](race_test.go)、[schedules_maintenance_test.go](schedules_maintenance_test.go)。 |
| 计划与唤醒 | 两实例同拍只建一份；反复 Put 不推迟；错过三个周期仅补一拍；在途重叠须跳过。5s 轮询、独立无积压窗口中，以 DB 入口时刻减 run_at 验证 Trigger / TriggerTx、700ms 延迟、退避、cron、released 与锁后到期行均不提前且 ≤300ms；监听中断仍须在 PollInterval 内领取，重连计数 =1；核查触发器规则、心跳 HOT 不变及短任务吞吐不再被轮询封顶（对照 [baseline](docs/baseline.md)）。见 [wake_test.go](wake_test.go)。 |
| 输出、去重与生命周期 | nil / 非 nil 空输出均成功；无效 jsonb 与非法错误文本可结算；同键返回原 id，含已终态且不改 params；持有者被清理时重试新建，三轮耗尽零 id；停机不能等待启动查库或补启动循环，不合作执行器须报 ErrNotDrained。见 `TestEmptyOutputSucceeds`、`TestDedupHoldsAfterFinish`、`TestDedupRoundsWhenWinnersVanish`、`TestShutdownCancelsStart` 及 [worker_test.go](worker_test.go)。 |
| 元数据与追踪 | schema 1 升级保留状态及租约，拒绝非法 owner / extra 组合；普通任务与工作流跨进程恢复 TraceContext，不继承请求取消与 deadline；重试、Snooze、Resume、去重保留提交关联，每次领取使用独立 ExecutionId。见 [migration_test.go](migration_test.go)、[tracing_test.go](tracing_test.go)。 |
| 提交后通知 | 回调时已提交且无事务行锁；Commit 失败、失租、幂等空操作不通知；取消守卫、下游取消、工作流聚合终态和 Resume 事件正确；回调 panic 不破坏已提交结果或后续通知；接管回调期间保持租约跟踪，失租后不进入 Executor。见 [observer_test.go](observer_test.go)、[observer_workflow_test.go](observer_workflow_test.go)、[observer_reclaim_test.go](observer_reclaim_test.go)。 |
| 健康与分类积压 | 区分启动中、运行、停机和残留槽位；Observer 内读取健康不得等待 Shutdown；领取部分失败仍派发已提交任务，空转保留前次结果，故障恢复清除错误，停机取消心跳不计失败；Stats 分类和总量同一快照，覆盖未来、blocked、终态与本实例注册口径。见 [health_test.go](health_test.go)、[health_loops_test.go](health_loops_test.go)、[stats_test.go](stats_test.go)。 |
| 控制组合与身份隔离 | 空恢复集合不得打开父流程或通知 resumed；执行器修改 Request 父 ID 不得改变内部结算与事件归属；allow/skip 切换、历史拍 Resume、删除重建及换目标均遵循同计划在途约束，扫描越过 50 个锁竞争候选且保留整批回滚。见 [request_isolation_test.go](request_isolation_test.go)、[resume_boundary_test.go](resume_boundary_test.go)、[schedule_protocol_test.go](schedule_protocol_test.go)、[race_test.go](race_test.go)。 |
| Snooze 与控制结果 | MaxAttempts=1 仍可多次等待成功；不占槽位、不改快照/attempt/errors、不保存中间 output；正值、微秒、24h 与最大 Duration 持久化准确。重启、旧 token、取消竞争及指标标签正确；submit 成功输出、task id 与原 deadline 经重试/恢复/Resume 保留；Cancel 原因与优先级、终态持键、并发守卫及 skip 拍 Resume 去重回滚均须覆盖。见 [snooze_test.go](snooze_test.go)、[cancel_resume_test.go](cancel_resume_test.go)、`TestResumeRejectsDuplicateKey`。 |

当前工作区验证：PG13 / PG18 无缓存全集、PG18 默认并发三轮 race、lint、独立索引 / HOT、M5 与 M7 smoke 通过，见[本轮记录](docs/scenarios.md#2026-09-12-全量-review-修复验证)。PG13 race 的通过记录属于上一轮。M5 不覆盖大规模计划容量，full / soak 尚未完成；普通测试通过不代表这些验收已完成。发布证据须关联固定源码版本和可访问制品。

## 源码导航

主包留在根目录；包内函数负责本进程的执行协调，`internal/store` 负责数据库事务、持久状态转换与工作流推进。职责约定见[设计 §1.1](docs/design.md#11-进程与职责)；贡献前先读 [AGENTS.md](AGENTS.md)。

| 阅读顺序 | 源码入口 | 关注内容 |
|---|---|---|
| 1. 使用入口 | [skein.go](skein.go)、[executor.go](executor.go)、[migrate.go](migrate.go) | 配置、注册、启动停机与显式迁移 |
| 2. 提交与查询 | [jobs.go](jobs.go)、[workflows.go](workflows.go)、[schedules.go](schedules.go)、[runs.go](runs.go) | 输入校验、同步操作、公开类型映射 |
| 3. 执行生命周期 | [claim.go](claim.go)、[worker.go](worker.go)、[heartbeat.go](heartbeat.go)、[settle.go](settle.go)、[extra.go](extra.go) | 领取、执行、续租、结果提交与追踪恢复；停机协调仍在 `skein.go` |
| 4. 后台循环与唤醒 | [scheduler.go](scheduler.go)、[listen.go](listen.go)、[maint.go](maint.go)、[health.go](health.go) | 定时扫描、LISTEN/NOTIFY 唤醒与到点定时器、保留清理、Stats 与本地健康 |
| 5. 数据库协议 | [internal/store](internal/store)、[queries](internal/store/queries)、[migrations](migrations) | 手写事务、sqlc 查询、嵌入 DDL；对照设计 §2 与 §2.9 |

测试与实现同包；按[验证判据](#验证判据)中的行为与文件入口定位。

## 文档与许可证

- [设计与 API 契约](docs/design.md)：状态、事务、锁序、配置与故障恢复。
- [性能基线](docs/baseline.md)：复跑配置、测量数据与限制。
- [场景验收](docs/scenarios.md)：M7 smoke 的运行入口、证据与边界。
- [MIT License](LICENSE)：Copyright © 2026 Jaken。

当前按预发布库维护，API 和存储协议尚未承诺跨版本兼容；宿主应固定依赖版本，升级前核对 schema 变化及旧版本共存要求。提交问题或改动时提供最小复现、Go / PG 版本、执行命令和验证结果；接口或协议调整先说明行为变化，并按 [AGENTS.md](AGENTS.md) 更新其单一契约来源。发布记录应列明 API / schema 兼容性、升级顺序和对应验证制品。
