# Data Engineering Review — `football_statistics`

> Reviewer: Data Engineering
> Date: 2026-06-04
> Scope: architecture, DAGs, technology choices, folder organization, data modeling, security, and **observability** (Medallion + per-technology, grounded in current vendor docs via Context7).
> Codebase reviewed: `dags/`, `dags/lib/`, `infra/`, `tests/`, notebooks, migrations.

---

## 1. Executive Summary

This is a **well-above-average personal/portfolio data platform**. It implements a genuine Medallion lakehouse (Bronze on MinIO → Silver/Gold on Iceberg), data-aware scheduling with Airflow 3.x `Dataset`s, a Postgres-backed state machine as a control plane, and a clean, unit-tested helper layer with dependency injection. The engineering instincts on display (idempotency, testability, separation of concerns) are good.

The gaps are the ones typical of a system that grew organically and has not yet been hardened for production:

| Theme | Verdict |
|---|---|
| Architecture & data flow | **Strong** — modern, coherent, idempotent |
| Orchestration (Airflow patterns) | **Strong** — datasets, factory, control table |
| Code quality / testability | **Good** — DI pattern, real unit tests |
| **Observability / monitoring** | **Weak — the #1 gap** (almost none today; OTel libs already installed but unused) |
| **Data quality** | **Misleading** — Silver "quality gates" are recorded as passed but never actually run |
| Compute execution model | **Fragile** — `docker exec jupyter nbconvert` on a shared container |
| Secrets management | **Weak** — hardcoded `minioadmin123`, plaintext `.env` |
| Repo hygiene | **Minor issues** — dead dirs, duplicates, stale README, 212 MB data in tree |

The rest of this document is organized as: strengths → weaknesses (prioritized) → a full **observability strategy** → per-technology notes → folder/repo notes → a prioritized roadmap.

---

## 2. What the Project Does Well (keep doing this)

1. **Clean Medallion separation.** Bronze (raw JSON in MinIO) → Silver (normalized Iceberg dims/facts) → Gold (season aggregates) is textbook and the boundaries are respected (the Silver notebook even warns "Do not add extraction cells here").
2. **Data-aware scheduling.** Bronze DAGs emit a *shared* `Dataset("minio://datalake-raw/espn/bronze")`; Silver is `schedule=[bronze_dataset]`; Gold is `schedule=[silver_dataset]`. This is the right Airflow 3.x idiom and decouples the stages cleanly.
3. **Control plane as a state machine.** `pipeline_season_control` (`pending → bronze_running → … → complete | failed`) with `last_error_stage` is an excellent pattern: it enables multi-season/multi-league processing without code changes and **incremental retry from the failed stage** (not from scratch).
4. **DAG factory for multi-league.** `_create_bronze_dag(league_key)` looped over `LEAGUE_CONFIGS` into `globals()` is the correct way to template one DAG per league.
5. **Testability via dependency injection.** Every DB helper takes a `get_conn_fn` callable instead of building connections itself. This makes `season_helpers`, `quality_helpers`, and `ingestion_helpers` Airflow-agnostic and unit-testable with mock connections — and the tests actually exercise this.
6. **Idempotent writes end-to-end.** Iceberg `writeTo(...).overwritePartitions()` keyed on `(season, league)`, Postgres `ON CONFLICT … DO UPDATE`, and `ON CONFLICT DO NOTHING` seeds. Re-runs are safe.
7. **Granular Bronze tasks** (`schedule → matchsheet → lineup → events`) each with an `on_failure_callback` that marks the season failed — good blast-radius control and good retry/backoff defaults (exponential backoff, `max_retry_delay`, `execution_timeout`).
8. **SQL-injection-safe dynamic ingestion.** `quote_ident()`/`normalize_identifier()` validate identifiers before interpolation — a real risk handled correctly.
9. **The athlete-ID enrichment** (`_build_enriched_lineup`) is a thoughtful fix to a genuine `soccerdata` limitation, with a graceful fallback and coverage logging.

---

## 3. Weak Points & Risks (prioritized)

### 🔴 Critical

**C1 — Silver "quality gates" are theater.**
`quality_helpers._STAGE_CHECKS["silver"]` declares 10 checks (`teams_not_empty`, `team_name_null_rate_ok`, `home_score_not_all_null`, …) and `record_stage_quality_passed()` writes them all to `pipeline_quality_checks` with `status='pass'` on every successful run. The module docstring asserts *"Quality gates (fail-fast assertions) live in the Spark notebooks and abort the notebook if data is bad. If the notebook succeeds, all gates passed."*

**This is false for Silver.** `spark_silver_processing.ipynb` contains **no `raise`/assertion** — its last cells only `print()` row counts. So those 10 "passes" certify checks that were never evaluated. (Gold is fine: `spark_gold_processing.ipynb` cell 4 has *real* `RuntimeError` gates for emptiness and null player/team.)
**Impact:** the audit trail is misleading; a null-flooded Silver table reports a clean bill of health.
**Fix:** either (a) add the real assertions to the Silver notebook so a failure actually aborts before `record_*` runs, or better (b) compute each check explicitly and call `record_quality_check(..., status, details="null_rate=0.03, threshold=0.05")` with the *measured* value and a true `pass/warn/fail`. See §4.B for the data-observability version of this.

**C2 — Secrets are hardcoded / in plaintext.**
- `minioadmin / minioadmin123` appears in `infra/minio/docker-compose.yaml`, `infra/spark/docker-compose.yaml`, `infra/spark/conf/spark-defaults.conf`, **and as code defaults** in the Bronze DAG (`MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")`).
- `.env` (correctly gitignored) still holds a real Postgres password and a third-party API key in cleartext on disk.
- No secrets backend is configured even though Airflow supports Connections/Variables + Vault/AWS SM/env backends.
**Fix:** move credentials to Airflow Connections (`db-pg-futebol-dados` already is one — extend the pattern to MinIO), inject via env at container runtime only, and remove real-looking secret *defaults* from code. Rotate the committed-to-disk values.

**C3 — No observability.** Beyond `logging` and two Postgres tables, there are no metrics, traces, dashboards, alerts, or lineage. This is the single biggest gap and the explicit focus of this review — covered in depth in **§4**. Notably, `opentelemetry-*` is **already in `requirements.txt`** and Airflow 3.x ships native OTel support, so most of the foundation is one config block away.

### 🟠 High

**H1 — Spark execution model is fragile.** Silver/Gold run via `BashOperator` → `docker start jupyter-spark` → `sleep 10` → `docker exec … jupyter nbconvert --execute notebook.ipynb`:
- **Host coupling:** Airflow (running as `root` via systemd) drives the Docker CLI on the host. Tight, privileged coupling.
- **Race condition:** the fixed `sleep 10` is not a readiness probe; a slow kernel start fails the run nondeterministically. Replace with a poll loop (`until docker exec … true; do sleep 2; done`) with a timeout.
- **Cross-DAG container contention:** Silver and Gold are *separate* DAGs, each `max_active_runs=1`, but they share **one** `jupyter-spark` container. There is no cross-DAG mutex, so an overlapping Gold run's `stop_spark` (`trigger_rule=ALL_DONE`) can kill a Silver session mid-flight (and vice-versa). Use an Airflow **Pool** of size 1 spanning both notebook tasks, or a dedicated container per stage.
- **Notebooks as production jobs** are hard to review/diff/test and carry hidden state. Prefer `.py` Spark jobs run with `spark-submit` (or at least `papermill`/`jupytext` for parameterization + review). The `_LEAGUE_SLUGS` map is even **duplicated** inside the notebook vs. `league_config.get_league_slug()` — a single-source-of-truth violation that will drift.

**H2 — Two overlapping ingestion lineages.** The Medallion path (Bronze→Silver→Gold in MinIO/Iceberg) coexists with a **separate** queue-based path: `brasileirao_teams_to_pg.py` (`get_brasileirao`) extracts to local CSV/JSON + `pending.jsonl`, and `consume_brasileirao_queue_to_pg.py` upserts into dynamic `raw_soccerdata_*` Postgres tables. These two lineages don't connect, partly duplicate extraction, and `brasileirao_teams_to_pg.py` still hardcodes `SEASON = 2026` (contradicting the control table). Decide: is Postgres a *serving* layer fed *from Gold*, or a parallel raw store? Consolidate or clearly document the split.

**H3 — Runtime DDL on every run.** `ensure_season_control_table()` and `ensure_control_tables()` run `CREATE TABLE / CREATE INDEX` (and `DROP TRIGGER`/`CREATE TRIGGER`!) on **every DAG run**. Migrations `001`/`002` already define these. Mixing schema management into the hot path is an anti-pattern (and the trigger drop/recreate is needless churn). Rely on the migrations; delete the runtime DDL or gate it behind an explicit one-time setup DAG.

### 🟡 Medium

| ID | Issue | Recommendation |
|---|---|---|
| M1 | **Per-row upserts** in `persist_rows()` (one `execute` per row) + `ALTER TABLE ADD COLUMN` per column per batch. | Batch with `psycopg2.extras.execute_values` or `COPY`; resolve schema once per batch. |
| M2 | **No row locking** in `get_pending_season` (read `SELECT … LIMIT 1`, then separate `UPDATE`). Safe only because `max_active_runs=1`. | Use `SELECT … FOR UPDATE SKIP LOCKED` so it stays correct if concurrency grows (e.g. per-league Silver). |
| M3 | **`game_map` travels through XCom** (game→id for a whole season, up to ~380 entries). XCom is backed by the metadata DB. | Fine now; at scale write the map to MinIO and pass a URI, or use a custom XCom backend (S3). |
| M4 | **No freshness / live-season refresh.** Once a season is `complete` it is terminal; an in-progress season whose fixtures change weekly is never re-pulled. | Add a "refresh" transition (e.g. `complete → pending` for the current season on a schedule) or a separate incremental DAG. |
| M5 | **`athlete_id` captured but unused in modeling.** Silver `players` dedups on player **name** (`partitionBy("player","team")`), risking collisions/transfers — the very thing the athlete-ID work solves. | Key the `players` dimension on `athlete_id` as a stable surrogate; carry it into `player_match_stats`/`player_season_stats`. |
| M6 | **`match_events` table is conditional** (skipped when `events.json` empty), so downstream schema is sometimes absent. | Always create the table (possibly empty) for schema stability, or document the optionality explicitly. |

### 🟢 Low / Hygiene

- **L1** Dead directories `database/` and `extraction/` (only `__pycache__`) — remove.
- **L2** Duplicate `drop_silver_gold_tables.py` in `infra/spark/` and `infra/spark/notebooks/` (latter untracked) — keep one.
- **L3** **Stale README**: references `brasileirao_lakehouse_pipeline.py` (deleted), shows the old structure, roadmap out of date.
- **L4** `infra/minio/minio-data/` (**212 MB**) lives in the working tree and is **not** in `.gitignore` — accidental `git add .` risk. Add it.
- **L5** Duplicated boilerplate across DAGs/notebooks: `_get_conn`, `DEFAULT_ARGS`, `_on_notebook_failure`, `_write_partitioned`. Factor into a shared module / notebook prelude.
- **L6** Duplicate verification cell in `spark_silver_processing.ipynb` (cells 9 & 10).
- **L7** `requirements.txt` is a raw `pip freeze` mixing runtime + unrelated deps (`selenium`, `seleniumbase`, `behave`, `mycdp`, `trio`…). Split into `requirements.txt` / `requirements-dev.txt`, install Airflow with its official constraints file, and pin intentionally.
- **L8** **No CI**: tests exist but nothing runs them automatically. Add GitHub Actions (`pytest`, `ruff`, notebook lint) + `pre-commit`.
- **L9** Compose lacks healthchecks/`depends_on: condition: service_healthy` (mc uses a shell wait loop — acceptable but inconsistent).

---

## 4. Observability Strategy (the core request)

Today, observability = `logging.info(...)` + `pipeline_season_control` + `pipeline_quality_checks`. That answers "did the DAG run?" but not "is the **data** correct, fresh, and complete?", "how is the **infrastructure** behaving?", or "where did this number come from?".

Below is a layered plan. It maps the classic observability pillars — **metrics, logs, traces** — plus **data observability** (freshness, volume, schema, distribution, lineage) onto each Medallion layer and each technology. Every code/config block is grounded in current vendor docs (fetched via Context7).

### The target stack (one diagram)

```
                         ┌────────────────────────────────────────┐
 Airflow 3.x  ──OTLP──►  │                                        │
 Spark 3.5    ──Prom──►  │   OpenTelemetry Collector  +  Prometheus│ ──► Grafana (dashboards)
 MinIO        ──Prom──►  │   (metrics)                 (TSDB)      │ ──► Alertmanager (alerts)
 Iceberg meta ──SQL──►   │   Tempo/Jaeger (traces)                 │
 Postgres ctrl──SQL──►   │                                        │
                         └────────────────────────────────────────┘
```

You already have the OTel client libraries installed. The missing pieces are a **collector + Prometheus + Grafana** (three small containers) and configuration.

---

### 4.A — Orchestration observability: Airflow 3.x (metrics + traces)

Airflow 3.x has **native OpenTelemetry** for both metrics and traces — and `apache-airflow[otel]` deps are already present. Turn it on.

**Metrics** (`airflow.cfg` / env `AIRFLOW__METRICS__*`):
```ini
[metrics]
otel_on = True
otel_host = otel-collector          # the OTel Collector container
otel_port = 8889
otel_prefix = airflow
otel_interval_milliseconds = 30000
otel_service = Airflow
otel_ssl_active = False
```
This emits DAG/task duration, success/failure counts, scheduler health, pool/queue saturation, executor lag, etc. — scrape into Prometheus, visualize in Grafana.

**Traces** (`airflow.cfg`):
```ini
[traces]
otel_on = True
otel_host = otel-collector
otel_port = 8889
otel_application = airflow
otel_ssl_active = False
otel_task_log_event = True
```
Distributed traces let you see a full pipeline run as one trace tree: `bronze_extraction → silver_processing → gold_processing`. You can add **custom spans** inside tasks to time the slow parts (e.g. each ESPN extraction):
```python
from airflow.sdk import task
from airflow.sdk.observability import trace

tracer = trace.get_tracer(__name__)

@task
def extract_schedule(season_info):
    with tracer.start_as_current_span("espn.read_schedule") as span:
        span.set_attribute("league_key", season_info["league_key"])
        span.set_attribute("season", season_info["season"])
        ...  # existing logic
```
Install: `pip install 'apache-airflow[otel]'`.

**Listeners & callbacks (lightweight, no new infra):** register a listener plugin (`@hookimpl on_task_instance_failed/_success`, `on_dag_run_failed`) to push a structured event to Slack/Discord/webhook or to write a metric. You already use `on_failure_callback` per task — generalize it: today a failure marks Postgres `failed` but **notifies no human**. Add a notifier (e.g. `SmtpNotifier` — `apache-airflow-providers-smtp` is already installed — or a Slack webhook) so failures page someone.

**SLAs / freshness:** define a max-runtime / deadline per stage and alert when a Bronze run for the current season hasn't completed within N hours (catches a silently-stuck `@hourly` league).

---

### 4.B — Data observability (Medallion: freshness, volume, schema, distribution, lineage)

This is the layer most pipelines miss and where C1 lives. For each table, track the **five data-quality dimensions** and persist them as time series so you can trend and alert — don't just record an unconditional `pass`.

**Make the checks real and measured.** Replace the blanket Silver "pass" with computed metrics written through the existing `record_quality_check(..., status, details)` API (which already supports `pass/warn/fail` + a `details` string):

| Dimension | Bronze (MinIO JSON) | Silver (Iceberg dims/facts) | Gold (aggregates) |
|---|---|---|---|
| **Freshness** | object `LastModified` of `schedule.json` per league/season | max `processed_at` per partition | max `processed_at`; lag vs Silver |
| **Volume** | row counts in each JSON; alert on −X% vs prior run | `teams/players/match_statistics/player_match_stats` counts vs expected (e.g. 380 matches) | `player_season_stats` count ≈ active roster |
| **Schema** | expected keys present in JSON | column set + types unchanged (schema drift) | column set stable |
| **Distribution/Nulls** | — | **the checks already named but never run**: `team_name_null_rate`, `player_name_null_rate`, `home_score_not_all_null`, `athlete_id` coverage % | no null player/team (already gated ✅) |
| **Uniqueness/Referential** | — | PK uniqueness (`game`, `player+team`); every `player_match_stats.game` ∈ `match_statistics` | one row per `(player,team,season)` |

Concretely, in the Silver notebook compute and **fail or warn**, then record the number:
```python
null_rate = df_players.filter(F.col("player").isNull()).count() / max(df_players.count(), 1)
status = "pass" if null_rate <= 0.05 else "fail"
# write status + measured value, not a blind 'pass'
record_quality_check(get_conn, season_id, "silver", "player_name_null_rate_ok",
                     status, details=f"null_rate={null_rate:.3f}, threshold=0.05")
if status == "fail":
    raise RuntimeError(f"player null_rate {null_rate:.3f} exceeds 0.05")
```

**Frameworks to graduate to** (the README roadmap already names these): **Great Expectations** or **Soda Core** for declarative expectations, or **dbt tests** if you add a dbt layer for Silver→Gold SQL. Any of them gives you a suite definition, a results store, and docs — far better than the hand-rolled catalogue. Wire their run as a real Airflow task that *gates* promotion (fail → don't emit the downstream `Dataset`).

**Lineage:** Airflow `Dataset`s already give you coarse lineage in the UI. For column-level lineage across the Spark transforms, add **OpenLineage** (`apache-airflow-providers-openlineage` + the Spark OpenLineage listener jar) → emit to **Marquez**. This makes "where did `goals_per_match` come from?" answerable and shows impact-analysis when a Bronze field changes.

---

### 4.C — Storage observability: MinIO (Prometheus)

MinIO publishes Prometheus metrics natively (cluster, node, bucket, API, scanner, replication). Scrape them.

- **Cluster metrics:** `GET http://minio:9000/minio/v2/metrics/cluster`
- **Bucket metrics:** `GET http://minio:9000/minio/v2/metrics/bucket` (v2 limit ≈100 buckets — fine here)
- **v3 / typed metrics & ad-hoc:** `mc admin prometheus metrics myminio cluster` (types: `api, audit, cluster, ilm, replication, scanner, system`)

Track per-bucket object counts/sizes for `datalake-raw`, `datalake-warehouse`, `datalake-artifacts` (Bronze growth and Iceberg warehouse growth), plus drive/node health. Example alert rules straight from the MinIO docs:
```yaml
groups:
- name: minio-alerts
  rules:
  - alert: NodesOffline
    expr: avg_over_time(minio_cluster_nodes_offline_total{job="minio-job"}[5m]) > 0
    for: 10m
    labels: {severity: warn}
    annotations:
      summary: "Node down in MinIO deployment"
  - alert: DisksOffline
    expr: avg_over_time(minio_cluster_drive_offline_total{job="minio-job"}[5m]) > 0
    for: 10m
    labels: {severity: warn}
    annotations:
      summary: "Disks down in MinIO deployment"
```
Also enable **MinIO audit logging** (webhook/Kafka target) to get a record of every `PutObject` the Bronze DAG performs — useful to reconcile "did the extraction actually land?" independent of Airflow's claim.

---

### 4.D — Compute observability: Spark 3.5 (metrics + event log)

Right now a Spark run is a black box that exits when the notebook ends. Spark 3.5 exposes a full metrics system; enable the **Prometheus sink** and **event logging**.

**`spark-defaults.conf` additions** (PrometheusServlet sink + richer app metrics):
```properties
# Expose Spark metrics in Prometheus format (scrape from the driver UI port)
spark.ui.prometheus.enabled                true
spark.metrics.appStatusSource.enabled      true
spark.metrics.staticSources.enabled        true

# Event log for the History Server (post-mortem of every run)
spark.eventLog.enabled                     true
spark.eventLog.dir                         s3a://datalake-artifacts/spark-events
spark.history.fs.logDirectory              s3a://datalake-artifacts/spark-events
```
Available **sinks**: `PrometheusServlet`, `MetricsServlet` (`/metrics/json`), `StatsdSink`, `GraphiteSink`, `JmxSink`, `Slf4jSink`, `CSVSink`. Useful **driver namespaces** to chart: `DAGScheduler.stage.failedStages`, `appStatus.tasks.failedTasks`, `appStatus.jobDuration`, `BlockManager.memory.*` (you run a memory-capped 3 GB container — watch `memUsed_MB`/spill to catch OOM before it kills the run).

**History Server:** point a small `spark-history-server` container at `s3a://datalake-artifacts/spark-events` so every Silver/Gold run is inspectable after the ephemeral `jupyter-spark` container is stopped (today, when Airflow stops the container, all UI/diagnostics vanish). Spark's **REST API** (`/api/v1/applications/...`) can also be polled by a final Airflow task to record job/stage metrics into `pipeline_quality_checks` as `details`.

---

### 4.E — Table-format observability: Iceberg metadata tables

Iceberg is self-observing — every table carries metadata tables you can query in the notebook (or from any engine) to record true row-deltas, file health, and snapshot lineage. Use these instead of bare `count()`:

```sql
-- What changed and when (per-commit operation, added/deleted rows live in summary)
SELECT * FROM lake.analytics.match_statistics.snapshots;
SELECT * FROM lake.analytics.match_statistics.history;

-- File/manifest health → detect small-file explosion from per-partition overwrites
SELECT * FROM lake.analytics.match_statistics.files;
SELECT * FROM lake.analytics.match_statistics.manifests;
SELECT * FROM lake.analytics.match_statistics.partitions;
```
The **snapshot `summary`** map carries `added-records`, `deleted-records`, `total-records`, `added-files-size` — record these per run as your authoritative volume metric (cheaper and more accurate than re-`count()`ing).

**Maintenance = observability hygiene.** Your `overwritePartitions()` per `(season, league)` on each run accumulates snapshots and small files. Schedule maintenance (a weekly Airflow DAG) so metadata stays small and queries stay fast:
```sql
-- prune old snapshots/metadata
ALTER TABLE lake.analytics.match_statistics EXECUTE expire_snapshots('2026-05-01 00:00:00');
-- (also: rewrite_data_files / rewrite_manifests via the Spark procedures)
CALL lake.system.rewrite_data_files('lake.analytics.match_statistics');
```
Iceberg also emits **`ScanReport`/`CommitReport`** metrics via a `MetricsReporter` — wire one to your OTel Collector for automatic scan/commit telemetry.

---

### 4.F — Control-plane / serving observability: Postgres

You already have the two best assets for a status dashboard — `pipeline_season_control` and `pipeline_quality_checks`. Surface them:
- A **Grafana Postgres datasource** with panels: seasons by `status`, time-in-stage, failure rate by `last_error_stage`, quality pass/warn/fail trend per check. This turns the control tables into an at-a-glance pipeline SLO board.
- Add an **`postgres_exporter`** for DB-level metrics (connections, locks, slow queries) since Postgres is also the Airflow metadata DB — its health is your scheduler's health.

---

### Observability rollout order (smallest effort → biggest signal)

1. **Wire failure notifications** (SMTP/Slack) — you already detect failures; just route them. *(hours)*
2. **Turn on Airflow OTel metrics+traces** + stand up `otel-collector + prometheus + grafana`. *(½ day)*
3. **Make Silver quality checks real** (C1) and record measured values. *(½ day)*
4. **Scrape MinIO + Spark Prometheus endpoints**; add the node/disk + task-failure alerts. *(½ day)*
5. **Record Iceberg snapshot-summary deltas** per run; add a weekly `expire_snapshots`/`rewrite_data_files` maintenance DAG. *(½ day)*
6. **Grafana board over the Postgres control/quality tables.** *(½ day)*
7. **OpenLineage → Marquez** for column-level lineage. *(stretch)*

---

## 5. Per-Technology Assessment

**Apache Airflow 3.x** — Idiomatic use (TaskFlow `@dag`/`@task`, `Dataset` scheduling, factory pattern, sensible retries). Improve: enable OTel (§4.A), add notifiers, replace `BashOperator(docker …)` with a proper Spark submission operator, use a **Pool** to serialize notebook tasks across Silver+Gold, and stop running DDL at runtime (H3).

**Apache Spark 3.5 + Iceberg 1.6** — Good lakehouse choices; AQE on, partitioned-by-`(season,league)` overwrites are correct and idempotent. Improve: don't run prod transforms through `nbconvert` (H1); enable metrics/event-log (§4.D); add table maintenance + metadata-table monitoring (§4.E); the pinned `hadoop-aws 3.3.4`/`aws-java-sdk-bundle 1.12.262` combo is brittle (a comment even notes the Hadoop-version dance) — consider a prebuilt image to pin the JVM dependency graph.

**MinIO** — Appropriate S3-compatible Bronze store; bucket bootstrap via `mc` is clean. Improve: real credentials/secret backend (C2), enable Prometheus metrics + audit log (§4.C), add object lifecycle/retention on `datalake-raw`, and **don't keep `minio-data/` in the repo tree** (L4).

**PostgreSQL** — Doubles as Airflow metadata DB, pipeline control plane, and a raw serving store — a lot of roles for one instance. Improve: clarify the `raw_soccerdata_*` lineage vs. the lakehouse (H2), batch the upserts (M1), add row-locking (M2), and add `postgres_exporter`.

**soccerdata / ESPN** — Smart use, with a real fix for the missing `athlete_id`. Watch: it scrapes an undocumented public API (fragile to upstream change) — the `read_events` non-fatal handling is the right instinct; extend that defensiveness and add a contract/schema check on the Bronze JSON so upstream drift is caught at the boundary, not three layers down.

---

## 6. DAG Design Review

- **Bronze** (`bronze_extraction__<league>`): clean linear `get_season → schedule → matchsheet → lineup → events → mark_done`. The `{}`/empty-dict short-circuit when no season is pending is a reasonable "skip" idiom, but it means a skipped run still shows **success** with no work — consider `AirflowSkipException` so the UI distinguishes "nothing to do" from "did work". `@hourly × 4 leagues` once all seasons are `complete` produces continuous no-op runs that just poll Postgres; a sensor or a lower cadence would cut noise.
- **Silver/Gold**: nearly identical (good candidate for a shared factory like Bronze has). The `start_spark → run_notebook → mark_done → quality → stop_spark` chain is sensible, but see H1 (container mutex, readiness, notebook-as-job) and C1 (quality).
- **Queue DAGs** (`get_brasileirao`, `consume_*`): solid queue mechanics (dedupe by hash + semantic key, done/failed sidecar files, idempotent upsert) but disconnected from the Medallion flow and partly stale (H2). Either promote to "serving from Gold" or retire.

---

## 7. Folder Organization Review

Overall layout is **clear and conventional** (`dags/` + `dags/lib/` + `infra/<component>/` + `tests/` + `Makefile`). Each `infra` component has its own README and compose — nice. Cleanups:

```
football_statistics/
├── dags/                  ✅ good; lib/ separation is excellent
├── database/   ❌ dead (only __pycache__) — delete
├── extraction/ ❌ dead (only __pycache__) — delete
├── infra/
│   ├── spark/
│   │   ├── drop_silver_gold_tables.py          ⚠️ duplicate of …
│   │   └── notebooks/drop_silver_gold_tables.py ⚠️ … this (untracked) — keep one
│   └── minio/minio-data/  ❌ 212 MB runtime data in tree, not gitignored — add to .gitignore
├── requirements.txt       ⚠️ raw pip freeze — split runtime/dev + use Airflow constraints
├── README.md              ⚠️ stale (references deleted DAG, old structure)
└── (no CI config)         ⚠️ add .github/workflows + pre-commit
```
Suggested additions: `requirements-dev.txt`, `.github/workflows/ci.yml`, `.pre-commit-config.yaml`, a top-level `docs/` (or expand `infra/README.md`) describing the two lineages, and a `spark_jobs/` package if/when notebooks become `.py` jobs.

---

## 8. Data Modeling Notes

- **Use `athlete_id` as the player surrogate key.** The Bronze layer goes to real effort to capture it, but Silver `players` dedups on `(player_name, team)` and Gold aggregates on the name — fragile across transfers/duplicate names. Promote `athlete_id` into `players`, `player_match_stats`, and `player_season_stats`.
- **No SCD / surrogate keys** for `teams`/`players` (acknowledged in the roadmap). For a portfolio piece, even SCD-2 on `players` (position/team changes over time) would be a strong showcase.
- **`match_statistics` keyed on `game` string** is acceptable as a natural key; consider a deterministic `game_id`-based surrogate for joins (you already carry `game_id`).
- The `mermaid` ER diagram in the README is a nice touch — keep it in sync with the `athlete_id` change.

---

## 9. Testing & CI

Good unit coverage on `extraction_helpers` (slug, lineup parsing, clock parsing, enrichment, profile fetch) with proper mocking. Gaps:
- No tests for `season_helpers` state transitions or `quality_helpers` (ironic given C1) — though those files exist as `tests/test_season_helpers.py` / `tests/test_quality_helpers.py`, verify they assert the *transition logic* and not just inserts.
- No integration test of a notebook (papermill can execute against a tiny fixture).
- **No CI** to run any of it. Add GitHub Actions: `ruff` + `pytest` + `nbqa`/notebook execution smoke test on PRs.

---

## 10. Prioritized Roadmap

**Now (correctness & safety):**
1. Fix **C1** — make Silver quality checks real (assert + record measured values).
2. Fix **C2** — credentials to Airflow Connections/secret backend; drop secret defaults from code; add `minio-data/` to `.gitignore`.
3. Route **failure notifications** (SMTP/Slack) from the existing callbacks.

**Next (observability foundation — the ask):**
4. Enable **Airflow OTel** metrics + traces; stand up `otel-collector + prometheus + grafana` (§4.A).
5. Scrape **MinIO** and **Spark** Prometheus endpoints; add node/disk + task-failure alerts (§4.C/§4.D).
6. Record **Iceberg snapshot-summary** deltas per run; add a weekly **maintenance DAG** (`expire_snapshots`, `rewrite_data_files`) (§4.E).
7. **Grafana board** over `pipeline_season_control` + `pipeline_quality_checks` (§4.F).

**Then (hardening):**
8. Replace `nbconvert`-on-shared-container with `.py` Spark jobs + `spark-submit`/operator; serialize Silver+Gold with a **Pool** (H1).
9. Remove runtime DDL (H3); batch ingestion upserts (M1); add `FOR UPDATE SKIP LOCKED` (M2).
10. Consolidate/clarify the **two ingestion lineages** (H2); remove dead dirs/duplicates/stale README (L1–L3).
11. Add **CI** + split requirements (L7/L8).

**Stretch (showcase):**
12. **OpenLineage → Marquez** column-level lineage; graduate data quality to **Great Expectations / Soda**; `athlete_id` surrogate keys + SCD-2.

---

### Appendix — sources

Observability guidance above was grounded in current vendor documentation retrieved via Context7:
- **Apache Airflow** (`/apache/airflow`) — `[metrics] otel_on`, `[traces] otel_on`, `airflow.sdk.observability` custom spans, `apache-airflow[otel]`.
- **Apache Iceberg** (`/apache/iceberg`) — metadata tables (`.snapshots/.history/.files/.manifests/.partitions`), `expire_snapshots`.
- **MinIO** (`/minio/docs`) — `/minio/v2/metrics/{cluster,bucket}`, `mc admin prometheus metrics`, Prometheus alert rules.
- **Apache Spark** (`/apache/spark`) — `PrometheusServlet`/`MetricsServlet` sinks, driver/appStatus metric namespaces, `spark.eventLog.*` + History Server, REST API `/api/v1`.
