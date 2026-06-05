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
| 4 | 🟢 Low / hygiene | 10 | 10 ✅ |
| 5 | 📈 Observability (§4 / C3) | 6 | 6 ✅ (live bring-up deferred) |
| 6 | Wrap-up | 1 | 1 ✅ |
| — | Deferred (tracked) | — | 6 |

_Last updated: Wave 5 (Observability) complete — full stack + Airflow/Spark/MinIO/Postgres wiring + Iceberg maintenance DAG (verified live). Live stack bring-up deferred. All planned waves (0–5) done; only the explicitly-deferred backlog remains. 78 unit tests; ruff clean._

## Live verification findings (running the actual pipelines)

Running the real Airflow DAGs end-to-end (per user request) surfaced bugs that unit tests + the notebook-in-isolation could not. All fixed and re-verified live.

- [x] **ENV — Airflow MinIO env gap** (consequence of C2). Live `/etc/default/airflow` lacked `MINIO_*`; applied `infra/airflow/airflow.env` + restarted services so scheduler/tasks have the creds. _commit: n/a (ops)._
- [x] **V1 — `docker exec` env unquoted** → league keys with spaces (`ENG-Premier League`, `ITA-Serie A`, `FRA-Ligue 1`) word-split; docker read `League` as the container name. This is why those leagues' 2026 seasons were stuck `failed/silver`. Quoted `SEASON`/`LEAGUE_KEY` in Silver+Gold. _commit: a7b5e16._
- [x] **V2 — Iceberg cross-league schema drift** → different ESPN leagues expose different stat columns; 2nd league into a shared table failed `INSERT_COLUMN_ARITY_MISMATCH`. Added `_align_df_to_table()` (ALTER ADD new cols, typed-NULL fill missing, project in table order) in both notebooks. _commit: a7b5e16._
- [x] **V3 — cross-year European season encoding** → soccerdata tags ENG/ITA/FRA seasons as e.g. `2627` for a `2026` request; Gold's `season==SEASON` filter then returned 0 rows. Normalize `season` to the pipeline `SEASON` in Silver. _commit: 9c40c4c._
- [x] **Notebook structure repair** — my iterative cell edits had duplicated the Silver write cell and dropped the quality-gate cell; restored from the C1 commit and re-applied the align fix cleanly.

**Verified live (all three pipelines, both league types):** a clean deterministic run drove `ITA-Serie A 2026` (cross-year) **and** `BRA-Brasileirao 2024` (calendar-year) through Bronze→Silver→Gold — all 6 DAG runs `exit 0`; both leagues coexist in the shared Iceberg tables; 11 measured quality rows recorded per Silver run; Gold `player_season_stats` 766/879 rows. Cross-league `drop→BRA→ENG→BRA` writes all pass.

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

- [x] **L1** Deleted dead `database/` + `extraction/` directories. _commit d39db74._
- [x] **L2** De-duped `drop_silver_gold_tables.py` (kept + tracked the mounted `notebooks/` copy). _commit d39db74._
- [x] **L3** Refreshed stale top `README.md` (removed deleted-file refs; added multi-league factory, control plane, migrations/pool setup, secrets-via-env, updated roadmap). _commit 7812ebf._
- [x] **L5** Factored shared boilerplate into `lib/airflow_common.py` (`get_pg_conn`, Spark container/pool/readiness, `notebook_failure_callback`); 4 DAGs use it. Verified live (Silver ITA 2025). _commit d905a4c._ _(Full silver/gold factory unification left with the deferred spark-submit refactor.)_
- [x] **L6** Remove duplicate notebook verify cell. — _done within C1_
- [x] **L7** Split requirements → curated `requirements.txt` + `requirements-dev.txt` + `requirements.lock.txt`; **added missing `boto3`** runtime dep. _commit 028b374._
- [x] **L8** Added `ruff.toml`, `.github/workflows/ci.yml` (ruff + pytest), `.pre-commit-config.yaml`. _commit 028b374._
- [x] **L9** MinIO healthcheck (curl `/minio/health/live`); `mc` waits for `service_healthy`. _commit d39db74._
- [x] **§6a** `AirflowSkipException` on no-op runs (bronze/silver/gold). _commit d39db74._
- [x] **L4** `minio-data/` gitignored + untracked. — _(C2 / commit 565c40b)_

---

## Wave 5 — 📈 Observability (§4 / C3)

New `infra/observability/` stack: `docker-compose.yaml` (otel-collector + prometheus + grafana + postgres_exporter + spark-history-server) + scrape/alert configs + starter dashboard.

- [x] **4.A — Airflow** OTel `[metrics]`/`[traces]` config in `airflow.env` (OFF by default, one flag to enable) + `pipeline_failure_notifier` (webhook/Slack) wired to all DAGs; custom-span pattern documented. _commit 07ffcf0/5fc4777._
- [x] **4.C — MinIO** Prometheus scrape jobs + node/disk alert rules; `MINIO_PROMETHEUS_AUTH_TYPE=public`. _commit 07ffcf0._
- [x] **4.D — Spark** `spark.ui.prometheus.enabled` + `appStatusSource` live; `eventLog` + History Server authored as a documented **opt-in** (avoids breaking the validated Spark runs). _commit 07ffcf0._
- [x] **4.E — Iceberg** `iceberg_maintenance` DAG (@weekly) + script: `rewrite_data_files` + `expire_snapshots` + file/snapshot metrics. **Verified live** across all 6 tables. _(Per-run snapshot deltas → `pipeline_quality_checks` left as a minor follow-up.)_ _commit 5fc4777._
- [x] **4.F — Postgres** provisioned Grafana dashboard over `pipeline_season_control` + `pipeline_quality_checks` + `postgres-exporter`. _commit 07ffcf0._
- [x] **4.G — Stack compose** authored (`otel-collector + prometheus + grafana + postgres-exporter + spark-history`); all YAML/JSON + `docker compose config` validate. _commit 07ffcf0._
- [>] **4.LIVE — Bring stack up + verify dashboards/metrics** → **deferred** (needs the stack running + Airflow `[otel]` extra).

---

## Wave 6 — Wrap-up

- [x] **W6.1** Final `pytest` green (78); tracker finalized; commits on `de-hardening`; PR offered.

---

## Deferred backlog (tracked, not in this pass)

- [>] **H1-full** Notebooks → parameterized `.py` jobs via `spark-submit`/operator.
- [>] **M5** Re-key `players` dimension on `athlete_id`; carry into facts.
- [>] **§8** SCD-2 on `players`/`teams`; surrogate keys.
- [>] **§4-live** Stand up + validate the monitoring stack against live Airflow/Spark.
- [>] **Stretch** OpenLineage → Marquez (column-level lineage).
- [>] **Stretch** Data-quality framework migration (Great Expectations / Soda Core).
