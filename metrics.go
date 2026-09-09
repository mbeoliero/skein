package skein

// Metrics receives the §3.5 measurements; the host adapts it to Prometheus or OTel.
// Labels are alternating key, value pairs. Counters: lease_lost_total, reclaim_total,
// released_alert_total, schedule_skipped_total{reason}, retention_deleted_total{table},
// maintenance_failures_total{step}, listener_reconnect_total. Observations in seconds: claim_latency,
// exec_duration{executor_type,outcome}, schedule_lag. Gauge: stale_active_total.
// pending_due, pending_oldest_age and unregistered_due come from Stats();
// hot_update_ratio is pg_stat_user_tables.n_tup_hot_upd / n_tup_upd on job_run, a
// trend figure: claim, settle, cancel, resume and node activation change indexed
// columns and are never HOT, so the ratio follows the workload mix. The 95% check
// is the heartbeat alone, TestHeartbeatIsHot on a quiet database.
// Methods are called concurrently from executor goroutines and the loops; an
// implementation must be safe for that and must not block.
type Metrics interface {
	Count(name string, n int, labels ...string)
	Gauge(name string, v float64, labels ...string)
	Observe(name string, v float64, labels ...string)
}

type nopMetrics struct{}

func (nopMetrics) Count(string, int, ...string)       {}
func (nopMetrics) Gauge(string, float64, ...string)   {}
func (nopMetrics) Observe(string, float64, ...string) {}
