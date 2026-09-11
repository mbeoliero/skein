package skein

import (
	"bufio"
	"context"
	"crypto/sha256"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

func scenarioCommand(parent context.Context, name string, args ...string) string {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "N/A"
	}
	return strings.TrimSpace(string(out))
}

func (h *scenarioHarness) dump(ctx context.Context, name, query string) (err error) {
	file, err := os.Create(filepath.Join(h.dir, name+".jsonl"))
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	rows, err := h.pool.Query(ctx, "SELECT row_to_json(x) FROM ("+query+") x")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		if _, err := file.Write(append(raw, '\n')); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (h *scenarioHarness) finish(cleanupCtx context.Context) {
	if !h.verified {
		h.t.Error("scenario verification did not complete")
	}
	ctx, cancel := context.WithTimeout(cleanupCtx, 10*time.Second)
	defer cancel()
	for name, table := range map[string]string{"invocations": "scenario_invocation", "effects": "scenario_effect", "workers": "scenario_worker", "workflows": "workflow_run", "controls": "scenario_control"} {
		if err := h.dump(ctx, name, "SELECT * FROM "+qualified(h.schema, table)); err != nil {
			h.t.Errorf("save %s evidence: %v", name, err)
		}
	}
	if err := h.dump(ctx, "runs", `SELECT id, job_name, workflow_run_id, state, attempt, run_at, started_at, finished_at, output, errors FROM `+qualified(h.schema, "job_run")); err != nil {
		h.t.Errorf("save run evidence: %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(h.dir, "worker-*.jsonl"))
	if err != nil {
		h.t.Error(err)
	}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			h.t.Error(err)
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var record struct {
				Level string `json:"level"`
				Msg   string `json:"msg"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				h.t.Errorf("invalid worker log: %v", err)
			} else if record.Level == "ERROR" {
				h.t.Errorf("worker error in %s: %s", filepath.Base(path), record.Msg)
			}
		}
		if err := scanner.Err(); err != nil {
			h.t.Error(err)
		}
		if err := file.Close(); err != nil {
			h.t.Error(err)
		}
	}
	var pg string
	if err := h.pool.QueryRow(ctx, "SHOW server_version").Scan(&pg); err != nil {
		h.t.Errorf("read PG version: %v", err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# M7 smoke %s\n\n结果：**%s**；核查完成：%t。种子：1。Go：%s；PostgreSQL：%s；系统：%s/%s。\n\n",
		time.Now().UTC().Format(time.RFC3339), map[bool]string{true: "FAIL", false: "PASS"}[h.t.Failed()], h.verified, runtime.Version(), pg, runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(&b, "提交：`%s`；工作区：`%s`；场景源码摘要：`%s`。\n\n", scenarioCommand(cleanupCtx, "git", "rev-parse", "HEAD"), strings.ReplaceAll(scenarioCommand(cleanupCtx, "git", "status", "--porcelain"), "\n", "; "), scenarioSourceHash())
	fmt.Fprintln(&b, "3 个进程 × Concurrency 16；Poll 5s；心跳 1s；租约 10s；取消超时 / 退出宽限 5s；重试退避 100–200ms ±20%。")
	fmt.Fprintf(&b, "\n连接预算：每个 worker 8 + LISTEN 1 + audit 2；提交端 %d；观察端 2。观察轮询 20ms，共执行 %d 条 SQL。\n\n", h.pool.Config().MaxConns, h.obsOps.Load())
	fmt.Fprintf(&b, "清单：%d 个 job_run，%d 个工作流，%d 次确认回滚。整轮耗时：%s；混合批次耗时：%s。\n\n", len(h.wants), len(h.workflowWants), len(h.rolledBack), time.Since(h.started).Round(time.Millisecond), h.mixedDuration.Round(time.Millisecond))
	if h.verified {
		fmt.Fprintf(&b, "逐次执行：%d；去重后副作用：%d；缺失返回：%d；缺失结算观察：%d。终态计数：`%v`。\n\n", h.callCount, h.effectCount, h.missingReturns, h.missingObservations, h.actualStates)
	} else {
		fmt.Fprintln(&b, "核查未完成，不能将缺失统计解释为零；请检查原始事件和测试日志。")
	}
	fmt.Fprintf(&b, "机器负载，开始：`%s`；结束：`%s`。\n\n", h.loadStart, scenarioCommand(cleanupCtx, "uptime"))
	fmt.Fprintln(&b, "混合批次：1,000 次普通提交 + 10 个四节点工作流。编码后参数：90% 1KB / 9% 32KB / 1% 250KB；固定种子的 base64 字符集随机数据，不用重复字符填充。精度探针与混合批次分开运行。")
	fmt.Fprintln(&b, "追加 Snooze 独立窗口：48 个普通任务各等待两次（3s），48 个屏障探针在最早到期前同时占满全部槽位；一个 submit → poll → verify 工作流，poll 等待四次（700ms）。共 100 次 Snooze 再启动，max_attempts=1；核对 pending 快照、独立请求时长、依赖输出和后继顺序。")
	fmt.Fprintln(&b, "\n| 场景 / 类型 / 参数大小 / 指标 | 样本数 | p50 | p95 | p99 | 最大值 |\n|---|---:|---|---|---|---|")
	for _, key := range slices.Sorted(maps.Keys(h.latencies)) {
		ds := h.latencies[key]
		p50, p95, p99 := percentiles(ds)
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s |\n", key, len(ds), p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond), slices.Max(ds).Round(time.Microsecond))
	}
	fmt.Fprintln(&b, "\n| 实例 | 调用数 | 并发峰值 | fixture SQL 次数 | SQL 累计耗时 | 非预期计数 |\n|---|---:|---:|---:|---|---:|")
	for _, worker := range h.workerRows {
		var stats map[string]int64
		if err := json.Unmarshal(worker.Stats, &stats); err != nil {
			h.t.Error(err)
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %s | %d |\n", worker.Owner, stats["starts"], stats["peak"], stats["audit_ops"], time.Duration(stats["audit_ns"]).Round(time.Millisecond), stats["unexpected"])
	}
	fmt.Fprintln(&b, "\nentry 使用数据库时钟；cron 以 scheduled_at 包含扫描晚点；混合负载 entry 包含排队。settle_confirm 是观察到结算的上界，不是精确提交延迟。fixture SQL 包含审计、幂等副作用与控制查询，不含 JSON / 摘要的 CPU 成本，不能整体扣除当作纯插桩开销。")
	fmt.Fprintln(&b, "\n范围仅 smoke。full / soak 尚未实现；不宣称持续容量、负载下故障接管、30 分钟稳定性、CPU 使用率或心跳 HOT 比例。生成器按固定批次提交，没有目标速率，generator lag 不适用。精度测量包含插桩成本；-race 结果不得用于性能对照。最终结果以 go test 退出码为准，制品写入失败同样使测试失败。")
	report := b.String()
	if err := os.WriteFile(filepath.Join(h.dir, "report.md"), []byte(report), 0o644); err != nil {
		h.t.Error(err)
	}
	if out := os.Getenv("SKEIN_SCENARIO_OUT"); out != "" {
		file, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err == nil {
			_, err = fmt.Fprintln(file, report)
			err = errors.Join(err, file.Close())
		}
		if err != nil {
			h.t.Errorf("save scenario report: %v", err)
		}
	}
	h.t.Logf("smoke report: %s", filepath.Join(h.dir, "report.md"))
}

func scenarioSourceHash() string {
	paths, err := filepath.Glob("scenario*_test.go")
	if err != nil || len(paths) == 0 {
		return "N/A"
	}
	slices.Sort(paths)
	hash := sha256.New()
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			return "N/A"
		}
		fmt.Fprintf(hash, "%s\x00%d\x00", path, len(b))
		hash.Write(b)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}
