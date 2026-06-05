# Football Statistics — Data Platform Report

A complete, end-to-end report on the `football_statistics` data platform: what it
is, the data it holds, the technologies and techniques in each layer, how every
piece works, and how to operate it and retrieve the data.

> Companion docs: [`DataEngineer.MD`](./DataEngineer.MD) (architecture review),
> [`DataEngineer-Progress.md`](./DataEngineer-Progress.md) (change tracker),
> [`DataEngineer-Testing.md`](./DataEngineer-Testing.md) (how to verify).

---

## 1. What this platform is

An automated **football (soccer) analytics data platform**. It extracts match
data from ESPN, lands it raw in object storage, transforms it into a curated
**Iceberg lakehouse** following the **Medallion architecture** (Bronze → Silver →
Gold), and exposes season-level analytics — all orchestrated by **Apache Airflow**
and observed with **Prometheus/Grafana + OpenTelemetry**.

### The data domain

| Dimension | Values |
|---|---|
| **Leagues** | `BRA-Brasileirao` (Brazil), `ITA-Serie A` (Italy), `ENG-Premier League` (England), `FRA-Ligue 1` (France) |
| **Seasons** | 2022 – 2026 (5 per league = **20 league-seasons** in the control plane) |
| **Teams** | ~20 per league-season. e.g. BRA 2024: Flamengo, Palmeiras, Corinthians, São Paulo, Grêmio, Internacional, Fluminense, Botafogo, Atlético-MG, Vasco da Gama, Bahia, Fortaleza, … |
| **Grain** | match → player-in-match → player-season |
| **Source** | ESPN public API via the `soccerdata` Python library |

### Entities (the analytical model)

```
teams (dim) ──< match_statistics (fact) >── teams
players (dim) ──< player_match_stats (fact) >── match_statistics
players ──aggregates──> player_season_stats (Gold)
match_events (fact: goals/cards/subs)
```

Players are keyed on a **stable surrogate** (`athlete_id` from ESPN, falling back
to name) so the same athlete is one identity across name spellings and seasons.

---

## 2. Architecture at a glance

```
 ESPN API ──(soccerdata)──►  BRONZE                 SILVER                   GOLD
                            MinIO (S3)            Iceberg tables           Iceberg table
   schedule/matchsheet/      raw JSON      ──►   teams, players,    ──►   player_season_stats
   lineup/events/game_map                        match_statistics,
                                                  player_match_stats,
                                                  match_events
        │                       │                      │                       │
        ▼                       ▼                      ▼                       ▼
  Bronze DAG (×4, @hourly) ─Dataset─► Silver DAG ──Dataset──► Gold DAG    (data-aware
   1 per league                       (ephemeral Spark)      (ephemeral)   scheduling)

  Control plane:  PostgreSQL  pipeline_season_control (state machine) + pipeline_quality_checks
  Quality:        in-job measured gates + Great Expectations suites → pipeline_quality_checks
  Lineage:        OpenLineage events (namespace football_pipeline)
  Observability:  Prometheus + Grafana + OTel Collector + Spark History Server + exporters
```

**Data-aware scheduling** (Airflow Assets/Datasets) ties the layers together: a
Bronze task declares `outlets=[Dataset("minio://datalake-raw/espn/bronze")]`; the
Silver DAG is `schedule=[that dataset]`. Airflow marks the dataset *updated* only
when the producing task **succeeds**, then schedules Silver — no cross-DAG polling.
Gold consumes the Silver dataset the same way. (This is also what OpenLineage
records as a `JobDependenciesRunFacet`.)

---

## 3. Technology stack

| Layer / concern | Technology | Role |
|---|---|---|
| Orchestration | **Apache Airflow 3.1.7** (native, systemd) | DAGs, data-aware scheduling, retries, pools |
| Extraction | **Python + soccerdata + boto3** | ESPN API → DataFrames → MinIO |
| Object storage (Bronze) | **MinIO** (S3-compatible) | raw JSON landing zone |
| Table format (Silver/Gold) | **Apache Iceberg 1.6.1** | ACID analytic tables, schema/partition evolution, time travel |
| Compute | **Apache Spark 3.5** (ephemeral `docker run` jobs) | transforms; custom `football-spark` image |
| Control plane / serving | **PostgreSQL 18** | season state machine + quality results |
| Data quality | **Great Expectations 1.3** + in-job measured gates | expectation suites gating promotion |
| Lineage | **OpenLineage** (Airflow provider) | run/job/dataset events |
| Metrics & dashboards | **Prometheus + Grafana** | infra + control-plane dashboards |
| Telemetry | **OpenTelemetry Collector** | Airflow metrics + traces (OTLP) |
| Spark observability | **Spark History Server** | post-hoc inspection of ephemeral jobs |
| Containers | **Docker / Docker Compose** | MinIO, Spark, observability stack |
| CI / quality | **GitHub Actions + ruff + pytest + pre-commit** | lint + unit tests |

---

## 4. The Medallion layers in detail

### 🥉 Bronze — raw landing zone (MinIO)

| Aspect | Detail |
|---|---|
| **Storage** | MinIO bucket `datalake-raw`, prefix `espn/{league_slug}/{season}/` |
| **Files** | `schedule.json`, `matchsheet.json`, `lineup.json`, `events.json`, `game_map.json` |
| **Format** | Raw JSON (records orient) — faithful, append-only copy of the source |
| **What's in it** | Per-season fixtures + scores (schedule), per-match team stats (matchsheet), per-player lineups **with ESPN `athlete_id`** (lineup), match events (events), and a `game→game_id` map |
| **Techniques** | `soccerdata.ESPN` reader; a custom **athlete-ID enrichment** that re-parses ESPN summary JSONs to recover `athlete_id` (which `read_lineup` discards) and left-joins it onto the soccerdata lineup; `game_map` persisted to MinIO instead of XCom to keep the Airflow metadata DB lean |
| **Producer** | `dags/brasileirao_bronze_extraction.py` — a **DAG factory** registering one `@hourly` DAG per league |

### 🥈 Silver — cleansed, normalized facts & dimensions (Iceberg)

| Aspect | Detail |
|---|---|
| **Storage** | Iceberg `lake.analytics.*` in `s3a://datalake-warehouse/iceberg` (HadoopCatalog) |
| **Tables** | `teams` (dim), `players` (dim), `match_statistics` (fact), `player_match_stats` (fact), `match_events` (fact) |
| **Partitioning** | `(season, league)` on every table — Iceberg **hidden partitioning** (queries filter on `season`/`league` without knowing the physical layout) |
| **What's in it** | Deduplicated team & player dimensions; one row per match (scores, winner, per-side stats); one row per player-per-match (goals, assists, cards, minutes…); one row per event |
| **Techniques** | **Surrogate keys** (`athlete_id` with name fallback) for the players dim; **season normalization** (soccerdata tags cross-year EU seasons e.g. `2627` → forced to the pipeline `SEASON`); **schema reconciliation** — different leagues expose different stat columns, so the write path `ALTER TABLE ADD COLUMN` for new columns and typed-NULL-fills missing ones before `overwritePartitions()`; **always-materialize** `match_events` (empty-with-schema if no data) for downstream stability |
| **Producer** | `spark_jobs/silver_job.py` run as an **ephemeral Spark container** |

### 🥇 Gold — curated season aggregates (Iceberg)

| Aspect | Detail |
|---|---|
| **Storage** | Iceberg `lake.analytics.player_season_stats`, partitioned `(season, league)` |
| **What's in it** | One row per (player, team, season): goals, assists, goal contributions, goals-per-match, matches played/started, cards… plus `athlete_id` |
| **Techniques** | `SUM`/`countDistinct` aggregation from `player_match_stats`; fail-fast quality gate (non-empty, non-null player/team); same idempotent partition-overwrite + schema-reconcile write |
| **Producer** | `spark_jobs/gold_job.py` (ephemeral) |

**Why Iceberg here** (grounded in the Iceberg spec): Iceberg tracks individual
data files (not directories) with **atomic commits** — each write creates a new
metadata file that atomically replaces the old one. Every write is a **snapshot**
(enables **time travel** & rollback); **schema evolution** adds/renames columns
with no table rewrite (used by our cross-league reconciliation); **hidden
partitioning** keeps queries simple; and **metadata tables** (`.snapshots`,
`.files`, `.history`, `.partitions`) make the lake self-observing.

---

## 5. The control plane (PostgreSQL)

`pipeline_season_control` is the single source of truth for *what to process and
where each season is*. State machine:

```
pending → bronze_running → bronze_done → silver_running → silver_done → gold_running → complete
                                                                                      → failed (any stage)
```

- **Atomic claiming:** `claim_next_season()` selects the next eligible row with
  `SELECT … ORDER BY season DESC LIMIT 1 FOR UPDATE SKIP LOCKED` and marks it
  running **in one transaction** — two concurrent runs can never grab the same season.
- **Incremental retry:** a failed season records `last_error_stage`; resetting it
  to `pending` re-runs only from where it failed.
- `pipeline_quality_checks` stores per-season, per-stage measured quality results
  (the system of record for data quality).

Live state at time of writing: 4 leagues × seasons 2022–2026 = 20 rows; the
scheduler is actively draining them (Bronze→Silver→Gold).

---

## 6. The pipelines (Airflow DAGs)

| DAG(s) | Schedule | What it does |
|---|---|---|
| `bronze_extraction__<league>` (×4) | `@hourly` | claim next pending season → extract schedule/matchsheet/lineup/events → upload to MinIO → emit Bronze dataset |
| `silver_processing` | `[bronze_dataset]` | claim next `bronze_done` → run ephemeral `silver_job.py` → record quality → mark `silver_done` |
| `gold_processing` | `[silver_dataset]` | claim next `silver_done` → run ephemeral `gold_job.py` → mark `complete` |
| `pipeline_season_refresh` | `@weekly` | re-queue the latest **complete** season per league (live seasons gain matches) |
| `iceberg_maintenance` | `@weekly` | `rewrite_data_files` + `expire_snapshots` + log file/snapshot counts |

Silver/Gold are generated by **one factory** (`dags/spark_stage_dag.py`).

### Execution model — ephemeral Spark containers

Each Silver/Gold run is a fresh **`docker run --rm football-spark spark-submit …`**
container (true isolation; no shared long-lived Spark). The custom image bakes the
Iceberg/S3A jars so jobs start in seconds (no per-run package download). A size-1
Airflow **pool** (`spark_notebook`) serializes the heavy jobs so two Spark
containers don't run at once on the memory-constrained host. Failures flow to a
DAG-level notifier (`ALERT_WEBHOOK_URL` if set) and mark the season failed.

---

## 7. Engineering techniques by layer

| Technique | Where | Why |
|---|---|---|
| Idempotent partition overwrite | Silver/Gold writes | re-runs are safe; `overwritePartitions()` replaces only the `(season, league)` partition |
| Cross-league schema evolution | `_align_df_to_table` | leagues expose different stat columns; tables evolve additively |
| Surrogate keys | players dim + Gold | stable identity via `athlete_id`, name fallback |
| Atomic work-claiming | `claim_next_season` | `FOR UPDATE SKIP LOCKED`, no double-processing |
| Data-aware scheduling | Bronze→Silver→Gold | decoupled, event-driven Medallion |
| Ephemeral compute | Spark jobs | isolation + reproducibility; baked jars for speed |
| Object-store handoff | `game_map.json` | keep large maps out of the Airflow metadata DB |
| Fail-fast quality gates | Silver/Gold jobs | bad data aborts **before** the write |
| Secrets via environment | `lib/minio_config` | no credentials in code; fail-fast if unset |

---

## 8. Data quality

Two complementary mechanisms, both writing to `pipeline_quality_checks`:

1. **In-job measured gates** — the Silver job computes null-rates, row counts,
   `home_score` presence, `athlete_id` coverage, writes a `report.json` to MinIO,
   and **raises** on a hard breach before any table is written. The Airflow task
   reads the report and records the measured `(status, details)` per check.
2. **Great Expectations** (`spark_jobs/ge_suites.py`) — declarative
   **Expectation Suites** validate the in-memory Spark DataFrames using GX's
   ephemeral **Data Context** → **Validation Definition** → results. Examples:
   `ExpectColumnValuesToNotBeNull(player/team/game)`,
   `ExpectColumnValuesToBeUnique(game)`, `ExpectTableRowCountToBeBetween`. Results
   merge into the same report → Postgres, and a GE `fail` aborts the run.

> GX concepts (grounded): a **Suite** holds Expectations; a **Validation
> Definition** binds a Suite to a data **Batch**; **Checkpoints** run validations
> and trigger actions; **Data Docs** render human-readable validation reports.
> (Data Docs → MinIO is a small remaining follow-up.)

---

## 9. Lineage

`apache-airflow-providers-openlineage` emits **OpenLineage** run/job/dataset
events (namespace `football_pipeline`) via a file transport (swap to an HTTP
transport to ship to Marquez). Because the pipeline uses Datasets, OpenLineage
records cross-DAG **asset dependencies** (a `JobDependenciesRunFacet`) — e.g. the
Silver run links back to the Bronze task/run that produced the dataset.

---

## 10. Observability — how to open each tool & what to look at

| Tool | URL | Login | What you'll see |
|---|---|---|---|
| **Airflow** | http://localhost:8080 | your Airflow user | DAGs, runs, datasets, task logs, pools |
| **Grafana** | http://localhost:3000 | `admin` / `admin` | "Football Pipeline" dashboard: seasons by status, quality pass/warn/fail, failed seasons; plus infra metrics |
| **Prometheus** | http://localhost:9090 | — | targets (`/targets`), ad-hoc queries (e.g. `minio_cluster_capacity_usable_total_bytes`, `pg_up`) |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin123` | buckets `datalake-raw` / `datalake-warehouse` / `datalake-artifacts`, browse the JSON + Iceberg files |
| **Spark UI** | http://localhost:4040 | — | live Spark job (only while one runs) |
| **Spark History** | http://localhost:18080 | — | past ephemeral jobs (event logs in MinIO) — start with `docker compose --profile history -f infra/observability/docker-compose.yaml up -d spark-history` |
| **Jupyter/PySpark** | http://localhost:8888 | token via `make logs-spark` | interactive Spark/Iceberg notebook |

**Telemetry flow:** Airflow exports metrics+traces (OTLP) to the **OTel
Collector** → Prometheus scrapes the collector + MinIO + Postgres-exporter →
Grafana renders. (Airflow metrics require enabling OTel + restarting Airflow; the
Grafana control-plane dashboard requires Postgres reachable from Docker.)

---

## 11. How to operate it

```bash
# Infra (MinIO + the persistent Spark/Jupyter container)
make infra-up            #  /  make infra-down
# Airflow (systemd)
make airflow-up          #  /  make airflow-down
make airflow-setup-pools # one-time: create the spark_notebook pool
# Observability stack (Prometheus + Grafana + OTel + exporters)
make obs-up              #  /  make obs-down  /  make logs-obs
# Migrations (idempotent — control plane + quality tables + season seeds)
psql -d futebol-dados -f infra/postgres/migrations/001_pipeline_season_control.sql
psql -d futebol-dados -f infra/postgres/migrations/002_pipeline_quality_checks.sql
psql -d futebol-dados -f infra/postgres/migrations/003_seed_seasons.sql
# Build the ephemeral Spark image (baked Iceberg jars + Great Expectations)
docker build -t football-spark:latest infra/spark
```

Credentials/config come from the environment (`.env.example`, `infra/airflow/airflow.env`) —
no secrets in code.

---

## 12. How to retrieve the data

**Gold/Silver via Spark SQL** (in Jupyter http://localhost:8888 or an ephemeral run):
```sql
-- Top scorers of a season
SELECT player, team, goals, assists, goals_per_match
FROM lake.analytics.player_season_stats
WHERE season = 2024 AND league = 'BRA-Brasileirao'
ORDER BY goals DESC LIMIT 10;

-- A league table (wins from match_statistics)
SELECT winner AS team, count(*) AS wins
FROM lake.analytics.match_statistics
WHERE season = 2024 AND league = 'BRA-Brasileirao' AND winner <> 'Draw'
GROUP BY winner ORDER BY wins DESC;
```

**Iceberg metadata / time travel** (self-observing tables):
```sql
SELECT * FROM lake.analytics.match_statistics.snapshots;   -- write history
SELECT * FROM lake.analytics.match_statistics.files;       -- file/size health
SELECT * FROM lake.analytics.player_season_stats VERSION AS OF <snapshot_id>;  -- time travel
```

**Control plane / quality (Postgres):**
```bash
psql -d futebol-dados -c "TABLE pipeline_season_control;"
psql -d futebol-dados -c "SELECT stage,check_name,status,details FROM pipeline_quality_checks ORDER BY checked_at DESC LIMIT 20;"
```

**Raw Bronze (MinIO):** the MinIO Console (:9001), or `mc ls/cat`/`boto3` against
`s3://datalake-raw/espn/<slug>/<season>/`.

---

## 13. Security & secrets

- **No secrets in application code** — MinIO/S3 credentials resolve from the
  environment (`lib/minio_config.get_minio_settings`, fail-fast if unset); Spark
  reads them via `EnvironmentVariableCredentialsProvider`.
- Local-dev defaults live only in marked infra files (`docker-compose` env,
  `infra/airflow/airflow.env`); `.env`/`.env.example` document the rest.
- Production should use an Airflow **secrets backend** (Vault / AWS SM) and rotate
  the local credentials.

---

## 14. Repository map

```
dags/                      Airflow DAGs
  spark_stage_dag.py         Silver+Gold factory (ephemeral containers)
  brasileirao_bronze_extraction.py   Bronze factory (1 DAG/league)
  pipeline_season_refresh.py / iceberg_maintenance.py
  lib/                       airflow_common, minio_config, league_config,
                             season_helpers, quality_helpers, extraction_helpers
spark_jobs/                silver_job.py, gold_job.py, ge_suites.py
infra/
  spark/Dockerfile           custom football-spark image (jars + GE)
  spark/conf/spark-defaults.conf
  minio/ postgres/ airflow/  compose, migrations, systemd units
  observability/             OTel + Prometheus + Grafana + exporters + dashboard
tests/                     unit tests (mock psycopg2; no infra needed)
DataEngineer*.md           review, progress tracker, testing notes
```

---

## 15. What's intentionally deferred

SCD-2 / full star schema; OpenLineage → Marquez UI; GE Data Docs → MinIO; per-run
Iceberg snapshot-delta recording. (See `DataEngineer-Progress.md`.)

---

*Generated as a complete platform report; data-engineering specifics grounded in
current Apache Iceberg, Apache Airflow, and Great Expectations documentation.*
