# Skein

嵌入 Go 宿主进程的 PostgreSQL 任务调度库：一次性与延迟任务、cron、DAG 工作流、重试、Snooze、取消、续跑和优雅停机。多个实例共享数据库，无需独立调度服务、选主、Redis 或消息队列。

## 安装

需要 [Go 1.27](https://go.dev/dl/) 和 PostgreSQL **≥ 13**。当前开发环境为 PostgreSQL 18.4；CI 配置覆盖 13 与 18。仅使用库时不需要安装 sqlc。

在宿主项目的 Go 模块目录执行：

```sh
go version # 应为 go1.27.x 或更新版本
go get github.com/mbeoliero/skein
```

## 快速开始

准备一个已存在的数据库，通过 DSN 指定连接；下面的账户、密码和数据库名按实际环境替换。`sslmode=disable` 仅用于本地开发，远程连接应配置 TLS。

```sh
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable'
```

示例读取 `DATABASE_URL`，先迁移，再注册执行器、声明任务、启动与触发。**为展示首次运行顺序，迁移写在同一个程序中；生产环境应将 `Migrate` 放到发布步骤串行执行，而不是每个副本启动时调用。**

将下面代码放入宿主项目的 `main.go`，执行 `go run .`，按 Ctrl+C 停机。包含泛型参数解码与定时计划的完整示例见 [example_test.go](example_test.go)。

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

1. **至少一次投递，不是恰好一次。**有外部副作用的 Executor 必须使用 `Request.IdempotencyKey` 去重；`DedupKey` 让同键提交在保留窗口内落到同一个 run，不是永久去重记录，也不是防重叠锁。
2. **数据库和连接池由宿主管理。**`Migrate` 创建 schema 与表，不创建数据库；`Start` 只校验版本。自定义 `Config.Schema` 时，迁移须使用同一名称；连接池建议至少 5 个连接：监听独占 1 条，其余留给领取、心跳与结算。
3. **Executor 应响应 ctx 取消。**`Shutdown` 不关闭连接池；应先处理它的返回值，再关闭宿主资源。`ErrNotDrained` 表示仍有执行器未退出，其租约将由其他兼容实例回收。

### 去重键

`DedupKey(k)` 按 job / workflow 名各自隔离，在保留窗口内同键只对应一个 run，不看状态：

1. **`ErrDuplicate` 带回的 id 可能已经结束。**拿 id 调 `Runs().Get` 读状态和 output，不要当瞬时错误重试；只有 id 为 0（持有者恰好被清理）才重试。
2. **失败或取消的 run 占键到 `RetentionFailed`（默认 30 天）。**重跑用 `Runs().Resume(ctx, id)`，同 id、同快照、同键；换参数再 Trigger 得到的仍是旧 run。成功的 run 不能 Resume，想再跑只能换键。
3. **不是互斥锁。**固定键如 `"nightly"` 会变成"7 天只跑一次"；防重叠用 `ScheduleSpec.Overlap = OverlapSkip`。键应编进业务身份和周期，例如 `invoice:2026-09`。
4. **第一次提交的参数就是最终参数。**同键再 Trigger 时新 params 和 `At(t)` 全部忽略。
5. **幂等边界就是保留窗口。**成功 `RetentionSucceeded`（默认 7 天）、失败 30 天，窗口过后同键可再建；要更长就调大 retention 或业务自行记账。

计划拍次由 `(schedule_name, scheduled_at)` 单独保证一拍一行，与手动 Trigger 的键互不影响。

### 执行器取消与手工续跑

Executor 返回 `nil, skein.Cancel(err)` 会取消本 run；节点会取消整个工作流，成功节点及其 output 保留。在跑兄弟通过心跳收到取消。原因保存在 run.Errors（kind=cancelled），不消耗 attempt，返回的 output 被忽略。支持 `%w` 包装，`Cancel(nil)` 返回 nil；上下文取消 / 停机 / 超时、失租及显式 `Permanent` 优先于 Cancel，Cancel 优先于 Snooze。

```go
return nil, skein.Cancel(fmt.Errorf("external task stopped: %w", err))
```

普通任务耗尽自动重试或取消后，调用 `e.Runs().Resume(ctx, runId)`：同一 id、输入快照、历史和幂等键，重置重试预算并立即排队。仅 failed/cancelled 可续跑；其它状态返回 `ErrNotResumable`；`overlap = skip` 的计划拍在下一拍仍在途时返回 `ErrDuplicate`，不改变旧 run。工作流节点不能单独续跑，使用 `e.Workflows().Resume(ctx, workflowRunId)`。

### 外部长任务：两节点 + Snooze

视频生成等“先提交、再查询”的任务使用一个两节点工作流：

```text
video.submit → 成功 output 保存 task_id 和固定 deadline
    ↓
video.poll   → 从 Request.Deps["video.submit"] 读取，每次只查询一次
               未完成：return nil, skein.Snooze(30 * time.Second)
               已完成：返回最终结果和 nil
```

Snooze 将**同一个 run** 延迟回 pending，释放执行槽位与租约，不消耗失败次数、不追加 errors、不保存 output。delay 必须为正，支持 24h 及更长时长；真正的查询错误仍按 RetryPolicy 处理。`Request.Attempt` 不是查询次数，正常 Snooze 不增加它。Snooze 后无需继续占用 goroutine 或心跳，多个进程可接续查询。

**总等待 24h 由业务 deadline 约束，不是把 JobSpec.Timeout 设成 24h。** deadline 来自稳定业务输入或外部任务创建时间，随提交的成功 output 保存；重试找回同一任务时不能重置为“本轮时间 +24h”。查询使用同一个可信时间基准，限制本次请求及下一次 Snooze 不越过剩余预算，超期返回 Permanent 业务失败。Resume 保留成功的 submit，只续跑查询节点，不延长 deadline；重新生成应重新 Trigger。供应商调用仍需使用 `Request.IdempotencyKey` 做幂等；取消工作流不会自动 Abort 外部任务。

完整宿主写法见 [example_test.go 的 ExampleSnooze](example_test.go)（模拟供应商，Go 编译检查）；可运行的两节点、期限及恢复场景见 [snooze_test.go](snooze_test.go)：

```sh
SKEIN_TEST_REQUIRE_DB=1 go test -count=1 -run '^TestSnooze' -v
```

## 开发与测试

测试访问真实 PostgreSQL，每个集成测试创建并清理自己的随机 schema。测试账户需要创建 schema、表及删除这些测试对象的权限；请使用专用测试数据库，不要指向生产库。

未设置 `SKEIN_TEST_DSN` 时默认连接本机 Homebrew socket：`postgres:///postgres?host=/tmp`。也可以显式指定 TCP 连接：

```sh
export SKEIN_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable'
go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
export PATH="$(go env GOPATH)/bin:$PATH"
make lint
make test-ci
make race
```

| 命令 | 行为 |
|---|---|
| `make test` | 普通测试；数据库不可达时跳过集成测试，可能复用 Go 测试缓存 |
| `make test-ci` | 必须连接数据库；`-count=1` 禁止测试缓存，不可达则失败 |
| `make race` | race detector，重复 3 次；可加 `SKEIN_TEST_REQUIRE_DB=1` 强制数据库可达 |
| `make lint` | 检查 sqlc 生成结果、gofmt 和 go vet |
| `make sqlc` | 修改 SQL 后重新生成存储代码 |

[GitHub Actions](.github/workflows/ci.yml) 在 push、pull request 或手动触发时运行 Go 1.27 与 PostgreSQL 13/18 矩阵：服务 TCP 健康检查、测试 DSN 连接验证，然后执行 `make lint && make test-ci && make race`。整个 job 都要求数据库可达，包括 race 检查。

性能基线不会随普通测试运行；需要时单独执行：

```sh
SKEIN_BENCH=1 go test -count=1 -run '^TestPerformanceBaseline$' -v -timeout 20m
```

M7 smoke 同样显式运行：3 个真实进程、1,000 个普通任务、10 个工作流，另有独立精度与事务探针；追加普通任务 / DAG 的反复 Snooze、100 次再启动及全槽位复用检查。不要与其他测试同时测量。

```sh
mkdir -p /tmp/skein-m7-smoke
SKEIN_TEST_REQUIRE_DB=1 SKEIN_SCENARIO=1 SKEIN_SCENARIO_PROFILE=smoke \
  go test -count=1 -parallel=1 -run '^TestScenarioAcceptance$' -timeout 15m \
  -artifacts -outputdir=/tmp/skein-m7-smoke -v
```

必须加 `-artifacts` 保留证据。配置、报告口径与实测见 [场景验收](docs/scenarios.md)；full / soak 尚未实现，M7 未完成。

### 验证判据

下列判据对应现有行为测试，不是生产 SLA；M7 混合负载与未实现的 full / soak 仍按[场景验收](docs/scenarios.md)独立判断。

| 机制 | 必过条件与入口 |
|---|---|
| 领取与租约 | 两支领取及每类型到点读取使用各自索引；安静库心跳 HOT >95%；失败则评审 run_at 兼作租约到期、合并领取的单列退路，接受心跳写索引，不叠加参数。60s 租约配置下 kill 后 ≤75s 接管且 attempt +1；暂停超过 60s 恢复后旧 token 不可写；预算耗尽重领直接 failed。见 [store_test.go](store_test.go)、[reliability_test.go](reliability_test.go)、[lease_process_test.go](lease_process_test.go)；真实 60s 租约的 kill / 暂停恢复验收随普通测试运行，单轮约 65–90s。 |
| 工作流与锁序 | 空定义拒绝；100 个并行前驱只激活一次汇合；fail-fast 收敛全部节点；Resume 不重跑成功节点且 started_at 不变；关键事务对并发 1000 轮无死锁，取消须在领取未提交时完成，并覆盖提交与回滚。清理不得删除在途工作流的已完成节点，或被 Resume 改回运行中的父行。见 [workflows_test.go](workflows_test.go)、[race_test.go](race_test.go)、[schedules_maintenance_test.go](schedules_maintenance_test.go)。 |
| 计划与唤醒 | 两实例同拍只建一份；反复 Put 不推迟；错过三个周期仅补一拍；在途重叠须跳过。5s 轮询、独立无积压窗口中，以 DB 入口时刻减 run_at 验证 Trigger / TriggerTx、700ms 延迟、退避、cron、released 与锁后到期行均不提前且 ≤300ms；监听中断仍须在 PollInterval 内领取，重连计数 =1；核查触发器规则、心跳 HOT 不变及短任务吞吐不再被轮询封顶（对照 [baseline](docs/baseline.md)）。见 [wake_test.go](wake_test.go)。 |
| 输出、去重与生命周期 | nil / 非 nil 空输出均成功；无效 jsonb 与非法错误文本可结算；同键返回原 id，含已终态且不改 params；持有者被清理时重试新建，三轮耗尽零 id；停机不能等待启动查库或补启动循环，不合作执行器须报 ErrNotDrained。见 `TestEmptyOutputSucceeds`、`TestDedupHoldsAfterFinish`、`TestDedupRoundsWhenWinnersVanish`、`TestShutdownCancelsStart` 及 [worker_test.go](worker_test.go)。 |
| Snooze 与控制结果 | MaxAttempts=1 仍可多次等待成功；不占槽位、不改快照/attempt/errors、不保存中间 output；正值、微秒、24h 与最大 Duration 持久化准确。重启、旧 token、取消竞争及指标标签正确；submit 成功输出、task id 与原 deadline 经重试/恢复/Resume 保留；Cancel 原因与优先级、终态持键、并发守卫及 skip 拍 Resume 去重回滚均须覆盖。见 [snooze_test.go](snooze_test.go)、[cancel_resume_test.go](cancel_resume_test.go)、`TestResumeRejectsDuplicateKey`。 |

已知验证限制：2026-09-08 默认并发三轮 race 曾在既有 FanIn / EmptyOutput 测试出现租约超时；相关及新增测试单独三轮、全套 `-race -count=3 -parallel=4` 通过。该次未重跑 M5 / M7，不能表述为默认并发无条件通过，也未据此排除资源竞争或实现抖动；与 M7 的负载失败记录分开看。

2026-09-09 真实进程租约验收：Go 1.27 / PostgreSQL 18.4 下，`TestRealLeaseProcessRecovery` 使用 60s 租约，kill 后约 60s 接管；SIGSTOP 经内核确认后保持 61s，再 SIGCONT，验证旧执行结果丢弃、旧 token 心跳与结算拒绝、新持有者正常完成。非缓存全套及 lint 通过，新增场景三轮 race 均通过。全套 race 首轮既有 `TestHeartbeatIsHot` 为 91.0%（183/201），后两轮及单独复跑三轮通过；原因未确定，不能记为全套 race 通过。未调整存储参数；PG13 待 CI 验证，M5 / M7 未运行。

## 源码导航

主包留在根目录；包内函数负责本进程的执行协调，`internal/store` 负责数据库事务、持久状态转换与工作流推进。职责约定见[设计 §1.1](docs/design.md#11-进程与职责)；贡献前先读 [AGENTS.md](AGENTS.md)。

| 阅读顺序 | 源码入口 | 关注内容 |
|---|---|---|
| 1. 使用入口 | [skein.go](skein.go)、[executor.go](executor.go)、[migrate.go](migrate.go) | 配置、注册、启动停机与显式迁移 |
| 2. 提交与查询 | [jobs.go](jobs.go)、[workflows.go](workflows.go)、[schedules.go](schedules.go)、[runs.go](runs.go) | 输入校验、同步操作、公开类型映射 |
| 3. 执行生命周期 | [claim.go](claim.go)、[worker.go](worker.go)、[heartbeat.go](heartbeat.go)、[settle.go](settle.go) | 领取、执行、续租与结果提交；停机协调仍在 `skein.go` |
| 4. 后台循环与唤醒 | [scheduler.go](scheduler.go)、[listen.go](listen.go)、[maint.go](maint.go) | 定时扫描、LISTEN/NOTIFY 唤醒与到点定时器、保留清理与 Stats |
| 5. 数据库协议 | [internal/store](internal/store)、[queries](internal/store/queries)、[migrations](migrations) | 手写事务、sqlc 查询、嵌入 DDL；对照设计 §2 与 §2.9 |

测试与实现同包，按行为定位；重命名不改变测试函数或验收条件：

| 测试文件 | 覆盖范围 |
|---|---|
| [reliability_test.go](reliability_test.go)、[lease_process_test.go](lease_process_test.go) | 崩溃接管、真实 60s 租约及暂停恢复、租约校验、停机、心跳失败与普通任务取消（M2） |
| [workflows_test.go](workflows_test.go) | DAG、汇合、fail-fast、工作流取消与续跑（M3） |
| [schedules_maintenance_test.go](schedules_maintenance_test.go) | 定时、DST、补一拍、保留清理、Stats 与列表（M4） |
| [wake_test.go](wake_test.go) | 跨实例即时触发、延迟与退避到点、cron 到点、槽位释放再领、监听重连、触发器规则（M6） |
| [scenarios_test.go](scenarios_test.go) | 三进程混合负载、Snooze / 槽位复用、独立精度探针及审计核查器负例（M7 smoke，显式运行） |
| [cancel_resume_test.go](cancel_resume_test.go) | 执行器取消、原因与优先级、整流取消续跑、普通同 id 重试预算、终态持键与并发守卫（设计 §2.5 / §3.2） |
| [snooze_test.go](snooze_test.go) | 纯 Snooze、时长边界、到点与槽位、两节点输出传递与固定期限（M8） |
| [worker_test.go](worker_test.go)、[race_test.go](race_test.go) | 执行准入、失租与完成的交错，以及事务锁序回归 |
| [skein_test.go](skein_test.go)、[store_test.go](store_test.go) | 基础任务、共享数据库夹具、领取索引与心跳 HOT 比例 |

## 文档与许可证

- [设计与 API 契约](docs/design.md)：状态、事务、锁序、配置与故障恢复。
- [性能基线](docs/baseline.md)：复跑配置、测量数据与限制。
- [场景验收](docs/scenarios.md)：M7 smoke 的运行入口、证据与边界。
- [MIT License](LICENSE)：Copyright © 2026 Jaken。
