# skein

Go **1.27** job scheduler library on PostgreSQL **>= 13**. One module,
`github.com/mbeoliero/skein`, embedded in the host process; multiple hosts share
one database, with no leader or extra service.

## Before making changes

- [docs/design.md](docs/design.md) is the design source of truth; implement ordinary
  requirements within it. If code and design disagree, stop and ask.
- Before changing the database schema, system architecture (including protocol and
  lock order), or any part of `docs/design.md`, explain the necessity and impact and
  obtain explicit approval. Also ask before starting each milestone.
- Ask before adding dependencies or `internal/` packages; new packages need a real
  second import boundary. No DI frameworks or future scaffolding; no CLI unless requested.
  Runtime dependencies are pgx/v5, robfig/cron/v3 and the OpenTelemetry API for TraceContext propagation.
- Leave `AGENTS.md` unchanged unless long-term development rules or workflows change.
  Do not record individual requirements, implementation history or completion status.
- After approval, update the relevant design section before implementation, following
  the writing rules below. Docs are Chinese; code, comments and this file are English.

Before editing, read the relevant design sections below and the existing
implementation/tests.

| Change                                            | Required reading                                                                                                         |
| ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Public API, validation, configuration             | §3; for Cancel / Resume / Snooze also §2.4–§2.5 |
| Worker, leases, startup or shutdown               | §2.2–§2.4, §2.6, §2.9: fencing, local lease loss, admission and drain ordering |
| SQL, schema, cancellation or workflow propagation | §1.3, the affected §2 transaction, §2.9: zero-row meanings and lock-order proof |
| Schedules, wakeups or retention                   | §2.1, §2.7–§2.8: database clock, lost notifications and concurrent Resume |
| Acceptance or performance work                    | [README validation](README.md#验证判据); [scenarios](docs/scenarios.md) for M7, [baseline](docs/baseline.md) for M5 |

### Design writing

- **Minimum content:** explain architecture, required behavior and necessary rationale
  as concisely as possible. Delete content adding none of these. Release status,
  development operations, implementation records and editorial notes are not design.
- **Visual first:** keep the architecture overview. Use diagrams for relationships,
  states, sequences and lock interactions; short tables for comparisons. Text supplies
  only missing constraints or reasons, not a retelling of the diagram. No paragraph-sized nodes.
- **Local changes:** edit only the approved scope. No incidental rewrite, renumbering
  or diagram removal. Add a section only for an independent new topic, not each task.
- **One explanation:** explain each contract once; elsewhere add only local constraints
  or a reference. Keep full DDL, API declarations and defaults in source; usage in README;
  acceptance and evidence in existing validation docs or PR/CI records. Do not move
  redundant prose into new docs or copy operational warnings into design.
- **Preserve and verify:** retain conditions, transaction boundaries, fences, lock order,
  failure windows and unresolved risks in their owning documents. Compare before/after,
  render and visually inspect changed diagrams, and check references. Rendering alone
  does not prove semantic correctness; report unverified checks.

## Commands

```text
make test       go test ./...; may skip database tests and reuse cached results
make test-ci    require a reachable database and run without caching
make race       go test -race -count=3 ./...
make lint       sqlc diff, gofmt check and go vet
make sqlc       regenerate after changing queries or migrations
```

`SKEIN_TEST_DSN` defaults to `postgres:///postgres?host=/tmp`.
Use `SKEIN_TEST_REQUIRE_DB=1 make race` to fail rather than skip if PG is unreachable.
[CI](.github/workflows/ci.yml) requires PG 13/18 checks; cached/skipped tests are not
acceptance evidence. Setup and source/test navigation: [README.md](README.md).

## Safety and package boundaries

### Storage and execution

- Durable state and run snapshots live only in PostgreSQL; no hot-path definition joins.
  Dedup uses partial unique indexes, not a separate slot table.
- Recovery is expired-lease claim branch two; keep both indexed branches separate.
  No startup reset, zombie sweeper, timeout scanner or separate workflow advancer.
- Only settle leaves `running`. Heartbeat/settle require token **and** running-state
  fences; `ErrLeaseLost` means roll back and discard, never retry or report (§2.9).
- Executors hold no DB connection; the host owns the pool. At-least-once effects
  require `Request.IdempotencyKey`. Migrate explicitly, never in `Start`; check schema version.
- Imports: `skein` → `internal/store`, `migrations`, with no back-imports. Root code
  calls `store.Store`, not `store.Queries`. Store owns transactions; generated types
  stop at root mapping functions, never in the public API.

### Concurrency and lifecycle

- Lock order: **`schedule name advisory lock → schedule → workflow_run → job_run`**;
  operations start at the first lock they need. Scheduled Resume acquires the name
  lock before any run row; scan tries name locks without waiting before locking each
  schedule. Settle, heartbeat and retention never acquire schedule/name locks.
  Claim and unstarted-node cancellation use
  `FOR UPDATE SKIP LOCKED`; settle must not wait for additional cancellation candidates (§2.9).
- Cancel running workflow nodes through parent state, never a batch UPDATE.
  `cancel_requested` is plain-run only; preserve ordering and convergence in §2.4–§2.5.
- Preserve atomic lease-loss/admission and separate lease/executor tracking (§2.3/§2.6).
  Never wait for loops or executors under the tracking mutex.
- Use DB time for DB comparisons; notifications are optional wakeups (§2.7).
- Heartbeat/settle need independent bounded contexts. Follow shutdown/cleanup ordering
  in §2.6/§2.8; no loops may start after shutdown.

## SQL changes

- Write named queries only in `internal/store/queries/` and DDL in `migrations/`.
  Exceptions: dynamic `CREATE SCHEMA` in `Store.Migrate`, `LISTEN` in `Store.Listen`,
  and test fixtures / EXPLAIN / pg_stat reads in `_test.go`. Each query's `-- name:`
  line is followed by its design-section comment.
- Use `Store.tx` for library transactions, including reads; caller-owned
  transactions use `Store.inCallerTx`. Queries are unqualified. Set transaction-local
  `search_path` to the quoted schema followed by `pg_temp`; never set it at pool or
  connection scope. TriggerTx must restore the caller's path using a detached,
  bounded context; restore failure must not be masked by `ErrDuplicate`.
- Approved schema changes require a new `migrations/NNNNN_name.sql`, a
  `schemaVersion` bump, updated design §1.2–§1.3 and regenerated sqlc output in the same
  change. Migrations are embedded, versioned and advisory-locked; no down migrations
  or migration CLI. Full DDL lives in migrations; design records the model and constraints.
- Run `make sqlc` and include its output with query/migration changes. Never hand-edit
  generated `internal/store/*.sql.go` or `models.go`. Keep SQL compatible with PG 13;
  do not introduce `uuidv7()`, `MERGE` or `RETURNING old.*`.
- Use `sqlc.yaml` for generator options and type mappings. Nullable values are
  pointers; jsonb maps to `[]byte` internally and `RawJSON` at the public boundary.
  Map `pgx.ErrNoRows` according to query semantics in store, then public errors via
  `mapErr`; it is not uniformly `ErrNotFound`.

sqlc 1.31 workarounds when editing queries:

- Multi-argument `unnest(a, b)` is unsupported by its catalog; use
  `FROM (SELECT unnest(@ids::bigint[]) AS id, unnest(@tokens::uuid[]) AS token) v`.
- When SQL guarantees a non-null UUID/timestamp, add `::uuid` / `::timestamptz` so
  generated Go uses a value. Add `::boolean` / `::text` to expressions otherwise
  inferred as `interface{}`. Do not repair generated types by hand.

## Go changes

- Before writing Go, run the `use-modern-go` skill's `list` for the file and follow it.
  Prefer stdlib and existing helpers; use generics rather than `any`-based helpers.
- Imports: stdlib, third party, this module. Initialisms: `Id` / `Sql` / `Db` / `Dsn`
  / `Ttl`; leave third-party and generated names unchanged.
- Use `encoding/json/v2` and `RawJSON` (`jsontext.Value`), not legacy `json.RawMessage`.
- Comments explain reasons, lock/fence rules or deferrals, not merely names or code.
- No sentinels or Config fields outside the design; every Config field must be read.
  Follow validation and control-result precedence in §3.1–§3.2.

Use `log/slog` and existing loggers; `instance` is already attached by `New`.
Metrics use `Config.Metrics`, names in `metrics.go` and semantics in §3.5, not a new dependency.

## Tests and acceptance

- Use stdlib `testing`, `t.Context()` and same-package `_test.go` files beside code.
  Reuse existing fixtures; integration tests need real PG and isolated schemas.
- Poll conditions, not fixed sleeps or cron boundaries. Measure wakeup executor entry
  against `run_at` on the database clock, never host time against DB time.
- Exercise real deterministic interleavings with row locks, not SQL-string assertions;
  crashes need child processes. Cancellation must finish while claim is uncommitted;
  check commit and rollback. Keep §2.9 races in `race_test.go` (1000 rounds, 100 under `-short`).
- Use `t.Parallel()` with isolated schemas, except HOT, M5 and M7 measurements: run
  these separately on a quiet database; worker processes may still run concurrently.

Map each acceptance check in [README](README.md#验证判据) to a focused behavior test. Preserve claim-index
EXPLAIN and heartbeat HOT > 95% checks; on HOT failure, follow the README fallback and
approval process, not new storage parameters. After claim, heartbeat or storage-parameter
changes, rerun M5 with the same knobs and record [baseline](docs/baseline.md) results.
M5/M7 are opt-in; ordinary tests do not demonstrate their acceptance.
