# Skein

嵌入 Go 宿主进程的 PostgreSQL 任务调度库：一次性与延迟任务、cron、DAG 工作流、重试、取消、续跑和优雅停机。多个实例共享数据库，无需独立调度服务、选主、Redis 或消息队列。

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

1. **至少一次投递，不是恰好一次。**有外部副作用的 Executor 必须使用 `Request.IdempotencyKey` 去重；`DedupKey` 只约束在途 run，不是永久去重记录。
2. **数据库和连接池由宿主管理。**`Migrate` 创建 schema 与表，不创建数据库；`Start` 只校验版本。自定义 `Config.Schema` 时，迁移须使用同一名称；连接池建议至少 5 个连接：监听独占 1 条，其余留给领取、心跳与结算。
3. **Executor 应响应 ctx 取消。**`Shutdown` 不关闭连接池；应先处理它的返回值，再关闭宿主资源。`ErrNotDrained` 表示仍有执行器未退出，其租约将由其他兼容实例回收。

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

## 源码导航

主包留在根目录；包内函数负责本进程的执行协调，`internal/store` 负责数据库事务、持久状态转换与工作流推进。目录与职责约定见设计 §8.3；贡献前先读 [AGENTS.md](AGENTS.md)。

| 阅读顺序 | 源码入口 | 关注内容 |
|---|---|---|
| 1. 使用入口 | [skein.go](skein.go)、[executor.go](executor.go)、[migrate.go](migrate.go) | 配置、注册、启动停机与显式迁移 |
| 2. 提交与查询 | [jobs.go](jobs.go)、[workflows.go](workflows.go)、[schedules.go](schedules.go)、[runs.go](runs.go) | 输入校验、同步操作、公开类型映射 |
| 3. 执行生命周期 | [claim.go](claim.go)、[worker.go](worker.go)、[heartbeat.go](heartbeat.go)、[settle.go](settle.go) | 领取、执行、续租与结果提交；停机协调仍在 `skein.go` |
| 4. 后台循环与唤醒 | [scheduler.go](scheduler.go)、[listen.go](listen.go)、[maint.go](maint.go) | 定时扫描、LISTEN/NOTIFY 唤醒与到点定时器、保留清理与 Stats |
| 5. 数据库协议 | [internal/store](internal/store)、[queries](internal/store/queries)、[migrations](migrations) | 手写事务、sqlc 查询、嵌入 DDL；对照设计 §6 与 §7.3 |

测试与实现同包，按行为定位；重命名不改变测试函数或验收条件：

| 测试文件 | 覆盖范围 |
|---|---|
| [reliability_test.go](reliability_test.go) | 崩溃接管、租约校验、停机、心跳失败与普通任务取消（M2） |
| [workflows_test.go](workflows_test.go) | DAG、汇合、fail-fast、工作流取消与续跑（M3） |
| [schedules_maintenance_test.go](schedules_maintenance_test.go) | 定时、DST、补一拍、保留清理、Stats 与列表（M4） |
| [wake_test.go](wake_test.go) | 跨实例即时触发、延迟与退避到点、cron 到点、槽位释放再领、监听重连、触发器规则（M6） |
| [worker_test.go](worker_test.go)、[race_test.go](race_test.go) | 执行准入、失租与完成的交错，以及事务锁序回归 |
| [skein_test.go](skein_test.go)、[store_test.go](store_test.go) | 基础任务、共享数据库夹具、领取索引与心跳 HOT 比例 |

## 文档与许可证

- [设计与 API 契约](docs/design.md)：状态、事务、锁序、配置与故障恢复。
- [性能基线](docs/baseline.md)：复跑配置、测量数据与限制。
- [MIT License](LICENSE)：Copyright © 2026 Jaken。
