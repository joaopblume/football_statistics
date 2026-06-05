# Data Engineering — Resolution Progress Tracker

> Companion to [`DataEngineer.MD`](./DataEngineer.MD). One checkbox per issue raised in that review.
> Testing process + how to verify each change: [`DataEngineer-Testing.md`](./DataEngineer-Testing.md).
> Work happens on branch **`de-hardening`**, phased by tier, tests + commit after each wave.

**Legend:** `[ ]` todo · `[~]` in progress · `[x]` done · `[>]` tracked / deferred (follow-up) · `[-]` superseded / N-A

## Progress summary

| Wave | Scope | Done | Total |
|---|---|---|---|
| 0 | Setup | 3 | 3 |
| 1 | 🔴 Critical | 2 | 2 ✅ |
| 2 | 🟠 High | 3 | 3 ✅ |
| 3 | 🟡 Medium | 4 | 4 ✅ (M1 superseded, M5 deferred) |
| 4 | 🟢 Low / hygiene | 0 | 10 |
| 5 | 📈 Observability (§4 / C3) | 0 | 6 |
| 6 | Wrap-up | 0 | 1 |
| — | Deferred (tracked) | — | 6 |

_Last updated: Wave 3 (Medium) complete — M2, M3, M4, M6 done (M1 superseded, M5 deferred), each verified live. 78 unit tests passing._

## Live verification findings (running the actual pipelines)

Running the real Airflow DAGs end-to-end (per user request) surfaced bugs that unit tests + the notebook-in-isolation could not. All fixed and re-verified live.

- [x] **ENV — Airflow MinIO env gap** (consequence of C2). Live `/etc/default/airflow` lacked `MINIO_*`; applied `infra/airflow/airflow.env` + restarted services so scheduler/tasks have the creds. _commit: n/a (ops)._
- [x] **V1 — `docker exec` env unquoted** → league keys with spaces (`ENG-Premier League`, `ITA-Serie A`, `FRA-Ligue 1`) word-split; docker read `League` as the container name. This is why those leagues' 2026 seasons were stuck `failed/silver`. Quoted `SEASON`/`LEAGUE_KEY` in Silver+Gold. _commit: a7b5e16._
- [x] **V2 — Iceberg cross-league schema drift** → different ESPN leagues expose different stat columns; 2nd league into a shared table failed `INSERT_COLUMN_ARITY_MISMATCH`. Added `_align_df_to_table()` (ALTER ADD new cols, typed-NULL fill missing, project in table order) in both notebooks. _commit: a7b5e16._
- [x] **V3 — cross-year European season encoding** → soccerdata tags ENG/ITA/FRA seasons as e.g. `2627` for a `2026` request; Gold's `season==SEASON` filter then returned 0 rows. Normalize `season` to the pipeline `SEASON` in Silver. _commit: 9c40c4c._
- [x] **Notebook structure repair** — my iterative cell edits had duplicated the Silver write cell and dropped the quality-gate cell; restored from the C1 commit and re-applied the align fix cleanly.

**Verified live:** Silver DAG end-to-end for `ITA-Serie A 2026` (spaced key, into tables already holding BRA+ENG partitions) → `state=success`, 11 measured quality rows in Postgres. Gold for `ITA-Serie A 2026` → `player_season_stats` 766 rows. Cross-league `drop→BRA→ENG→BRA` all exit 0. Bronze → _(in progress)_.

---

## Wave 0 — Setup

- [x] **W0.1** Create feature branch `de-hardening`. — _done_
- [x] **W0.2** Capture green test baseline. — _`74 passed`_
- [x] **W0.3** Create this tracker (`DataEngineer-Progress.md`). — _done_

---

## Wave 1 — 🔴 Critical

- [x] **C1 — Silver quality gates are theater → make them real**
  - [x] Compute the 10 declared checks + `athlete_id_coverage` in `spark_silver_processing.ipynb` and `raise` on hard failure **before** the write cell; write a measured `report.json` to MinIO.
  - [x] Record **measured** results via new `record_quality_report(...)` (silver DAG reads the report) instead of blanket `record_stage_quality_passed`; fall back to a `warn` row if the report is missing.
  - [x] Add unit tests for `record_quality_report` (6 new tests, mock-conn pattern).
  - [x] Delete the duplicate verify cell (**L6**).
  - _Resolution:_ measured gates in notebook + `report.json` handoff to MinIO + `record_quality_report` helper. `record_stage_quality_passed` kept (still used by Gold for now). 80 tests pass. · _commit: (next)_
- [x] **C2 — Secrets out of code**
  - [x] New `lib/minio_config.get_minio_settings()` reads creds from env and **fail-fasts** if unset; Bronze + Silver DAGs use it. Removed `minioadmin123` literals + `minio*` secret defaults from `extraction_helpers`.
  - [x] Parameterized `infra/minio/docker-compose.yaml`, `infra/spark/docker-compose.yaml` (`${MINIO_*:-default}`); `spark-defaults.conf` now uses `EnvironmentVariableCredentialsProvider` (no keys in file).
  - [x] Added `.env.example`; local-dev creds moved to `infra/airflow/airflow.env` (systemd `EnvironmentFile`, clearly marked) — not in app code.
  - [x] Added `infra/minio/minio-data/` to `.gitignore` (**L4**).
  - _Note:_ real secrets still need rotation (out of scope here); prod should use a secrets backend. After deploy, run `make airflow-install-services` so the new env reaches Airflow.
  - _Resolution:_ env-driven creds, fail-fast, no secret literals in `*.py`. compose validates; 80 tests pass. · _commit: (next)_

> **C3 (no observability)** is the whole of §4 → tracked in **Wave 5**.

---

## Wave 2 — 🟠 High

- [x] **H1 — Fragile Spark execution (incremental fix)**
  - [x] Replaced `sleep 10` with a `pyspark`-import readiness poll (30×2s, fail at 60s) in `start_spark` (silver + gold).
  - [x] Added size-1 pool `spark_notebook` on **all three** container tasks (`start_spark`/`run_spark_*`/`stop_spark`) in both DAGs so neither run can start/stop the shared container while the other is mid-flight. Pool created live; `make airflow-setup-pools` provisions it.
  - [>] Full `.py` + `spark-submit` (ephemeral per-run containers) → **Deferred** (closes the residual start/stop window entirely).
  - _Resolution:_ readiness poll + cross-DAG size-1 pool. DAGs compile; pool created; 80 tests pass. · _commit: (next)_
- [x] **H2 — Retire the queue→Postgres path**
  - [x] Deleted `brasileirao_teams_to_pg.py`, `consume_brasileirao_queue_to_pg.py`, `lib/ingestion_helpers.py`.
  - [x] Removed dead helpers from `extraction_helpers.py` (`build_queue_message`, `fetch_player_profile`+`_extract_profile_url`, `write_csv`/`write_json`, `slug`, `ensure_brasileirao_mapping`, `ESPN_ATHLETE_API`, `DEFAULT_API_DELAY`) + now-unused imports (`Path`/`pendulum`/`requests`); kept all Bronze-used parsers.
  - [x] Removed orphan tests (`TestSlug`, `TestBuildQueueMessage`, `TestFetchPlayerProfile`) + a stale `ingestion_helpers` docstring ref.
  - [x] Updated `dags/README.md` (Medallion + "queue aposentada" note) + `infra/postgres/README.md` (003 row accuracy).
  - _Note:_ orphaned live tables (`raw_soccerdata_*`, `raw_ingestion_events`) can be dropped manually if desired — left untouched.
  - _Resolution:_ queue lineage gone; ruff clean; 80 → 70 tests (10 dead tests removed). · _commit: (next)_
- [x] **H3 — Remove runtime DDL**
  - [x] Dropped `ensure_season_control_table(...)` calls + imports from bronze/silver/gold DAGs; table now provisioned solely by migration `001`. (`ensure_control_tables` was removed with H2.)
  - [x] The helper is kept in `season_helpers.py` for explicit/manual setup (mirrors the migration); `infra/postgres/README.md` already documents "apply migrations first".
  - _Resolution:_ no DDL on the hot path; ruff clean; 70 tests pass. · _commit: (next)_

---

## Wave 3 — 🟡 Medium

- [x] **M2 — Atomic season claim** → new `claim_next_season()` does `SELECT … FOR UPDATE SKIP LOCKED` + mark-running in **one transaction**; Bronze/Silver/Gold use it. 5 tests. _commit e59fb06._
- [x] **M3 — `game_map` off XCom** → `extract_schedule_to_minio` writes `game_map.json` to MinIO; lineup/events read it via `_load_game_map()`. Verified live (380 games, 16KiB). _commit 0d02d4a._
- [x] **M4 — Live-season refresh** → `requeue_latest_complete_seasons()` + `@weekly` `pipeline_season_refresh` DAG (latest complete → pending per league). 3 tests; verified live (re-queued BRA 2022). _commit 4e58dca._
- [x] **M6 — Stable `match_events`** → always materialize the table (empty w/ stable schema if no events). Verified live (BRA 2022 → table exists, 0 rows). _commit 2836e79._
- [-] **M1 — Per-row upserts** → **superseded** by H2 (lived in `ingestion_helpers`).
- [>] **M5 — `athlete_id` surrogate key in modeling** → **deferred** (modeling change).

---

## Wave 4 — 🟢 Low / hygiene

- [ ] **L1** Delete dead `database/` + `extraction/` directories.
- [ ] **L2** De-dup `drop_silver_gold_tables.py` (keep the mounted `notebooks/` copy, track it).
- [ ] **L3** Fix stale top `README.md` (remove `brasileirao_lakehouse_pipeline.py`; add multi-league factory, season-control state machine, full notebook list, queue-path removal, observability pointer).
- [ ] **L5** Factor shared boilerplate (`_get_conn`, `DEFAULT_ARGS`, `_on_notebook_failure`, `_write_partitioned`) + unify the near-identical silver/gold DAGs (**§6**).
- [x] **L6** Remove duplicate notebook verify cell. — _done within C1_
- [ ] **L7** Split `requirements.txt` → runtime + `requirements-dev.txt` (file reorg only, no reinstall).
- [ ] **L8** Add CI (`.github/workflows/ci.yml`: ruff + pytest) + `.pre-commit-config.yaml`.
- [ ] **L9** Add compose healthchecks (minio/spark) + `depends_on: condition: service_healthy`.
- [ ] **§6a** Raise `AirflowSkipException` on no-op bronze runs (distinguish "nothing to do" from "did work").
- [x] **L4** `minio-data/` gitignored. — _(folded into C2)_

---

## Wave 5 — 📈 Observability (§4 / C3)

New `infra/observability/` stack: `docker-compose.yaml` (otel-collector + prometheus + grafana + postgres_exporter + spark-history-server) + scrape/alert configs + starter dashboard.

- [ ] **4.A — Airflow** OTel `[metrics]`/`[traces]` config + failure **notifier** (SMTP/Slack) wired to DAGs + one custom-span example.
- [ ] **4.C — MinIO** Prometheus scrape + node/disk alert rules; document audit-log webhook.
- [ ] **4.D — Spark** `PrometheusServlet` + `spark.eventLog.*` in `spark-defaults.conf`; History Server on `s3a://datalake-artifacts/spark-events`.
- [ ] **4.E — Iceberg** maintenance DAG (`expire_snapshots` + `rewrite_data_files`, weekly) + per-run snapshot-summary deltas → `pipeline_quality_checks`.
- [ ] **4.F — Postgres** Grafana board over `pipeline_season_control` + `pipeline_quality_checks` via `postgres_exporter`.
- [ ] **4.G — Stack compose** authored + `docker compose config` validates.
- [>] **4.LIVE — Bring stack up + verify dashboards/metrics** → **deferred** (needs Airflow + Spark running).

---

## Wave 6 — Wrap-up

- [ ] **W6.1** Final `pytest` green; finalize this tracker; leave commits on `de-hardening`; offer a PR.

---

## Deferred backlog (tracked, not in this pass)

- [>] **H1-full** Notebooks → parameterized `.py` jobs via `spark-submit`/operator.
- [>] **M5** Re-key `players` dimension on `athlete_id`; carry into facts.
- [>] **§8** SCD-2 on `players`/`teams`; surrogate keys.
- [>] **§4-live** Stand up + validate the monitoring stack against live Airflow/Spark.
- [>] **Stretch** OpenLineage → Marquez (column-level lineage).
- [>] **Stretch** Data-quality framework migration (Great Expectations / Soda Core).
