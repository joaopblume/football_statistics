# Testing & Verification Notes

> Companion to [`DataEngineer.MD`](./DataEngineer.MD) (the review) and
> [`DataEngineer-Progress.md`](./DataEngineer-Progress.md) (the checklist).
> This file records **how each change was tested** during the hardening work and
> **how you can test further** — locally, per-component, and end-to-end.
> Updated as each wave lands.

---

## 1. How the work is organized (process)

- All work happens on branch **`de-hardening`** (never on `main`).
- Resolved **tier by tier** (Critical → High → Medium → Low → Observability), one commit per issue (or tight group), with the commit message naming the issue IDs (`C1`, `C2`, …).
- After every change: **run the test suite + static checks**, update `DataEngineer-Progress.md`, then commit.
- Reproducible baseline captured before touching anything: **74 tests passing**.

### Environment setup (run once per shell)
```bash
cd /root/football_statistics
source /root/airflow/venv/bin/activate     # Airflow + deps + pytest live here
```

---

## 2. The standing verification commands (what I run every change)

These are fast, deterministic, and need **no running infrastructure** (pure unit + static):

```bash
# 1. Full unit-test suite (DB helpers use mock psycopg2 — no real Postgres needed)
python -m pytest tests/ -q

# 2. Byte-compile changed Python (catches syntax/import-time errors)
python -m py_compile dags/*.py dags/lib/*.py

# 3. Notebooks are valid JSON and have the expected cells
python -c "import json; nb=json.load(open('infra/spark/notebooks/spark_silver_processing.ipynb')); print('cells:', len(nb['cells']))"

# 4. docker-compose files still parse + interpolate
docker compose -f infra/minio/docker-compose.yaml config  >/dev/null && echo OK
docker compose -f infra/spark/docker-compose.yaml config   >/dev/null && echo OK
```

A change is only committed when (1) and (2) are green.

---

## 3. What was tested per wave (audit trail)

### Wave 0 — baseline
- `pytest tests/ -q` → **74 passed** (recorded as the green baseline before any edit).

### Wave 1 — Critical

**C1 — real Silver quality gates** (commit `efe6528`)
- Added 6 unit tests for the new `record_quality_report()` helper (valid batch insert, measured status/details preserved, commit/close, empty-list raises, invalid-status raises *before* any write, missing-name raises). → suite **74 → 80 passed**.
- Validated the notebook is still valid JSON, now **12 cells**, contains the `QUALITY GATES` cell, and has exactly **1** verify cell (duplicate removed = L6).
- `py_compile` on the Silver DAG + helpers.
- ⚠️ **Not** runtime-verified here (needs a live Spark run) — see §4 for how to do that.

**C2 — secrets out of code** (commit `6a3125d`)
- `grep -rn minioadmin --include=*.py dags/ tests/` → only a docstring mention remains (no real secret in code).
- Both compose files validated with `docker compose … config` (the `${MINIO_*:-default}` interpolation resolves).
- `py_compile` on the Bronze DAG + `extraction_helpers`. Suite still **80 passed**.

### Wave 2 — High

**H1 — Spark readiness-poll + pool** (commit `cffe2f1`)
- `grep "sleep 10" dags/` → gone; `SPARK_POOL` referenced on all 3 container tasks in both DAGs.
- Created the pool live: `airflow pools set spark_notebook 1 …` → "Pool spark_notebook created".
- `py_compile` both DAGs; suite **80 passed**.
- ⚠️ Cross-DAG serialization is best-checked live (trigger Silver + Gold to overlap and confirm only one notebook runs); residual start/stop window closes with the deferred spark-submit refactor.

**H2 — retire queue→Postgres** (commit `31255f0`)
- `grep -rn "ingestion_helpers|build_queue_message|fetch_player_profile|get_brasileirao|consume_brasileirao" --include=*.py` → no references.
- `ruff check … --select F401,F811` → all clean (dead imports removed).
- Suite **80 → 70** (the 10 now-irrelevant tests were removed with the code they covered).

**H3 — remove runtime DDL** (commit `(this wave)`)
- `grep -rn ensure_season_control_table` → only the definition in `season_helpers.py` (no DAG call sites).
- ⚠️ Requires migrations to be applied first (`001`). On a fresh DB without migrations the DAGs now fail fast instead of silently creating tables — verify with: `psql -d futebol-dados -c "\d pipeline_season_control"`.

---

## 4. How YOU can test further

Layered from cheapest/most-isolated to full end-to-end.

### 4.1 Unit + static (seconds, no infra)
```bash
source /root/airflow/venv/bin/activate
python -m pytest tests/ -v          # all unit tests, verbose
python -m pytest tests/test_quality_helpers.py -v   # just the quality helper
python -m py_compile dags/*.py dags/lib/*.py
```
Expect: **all green**. These cover the pure logic (parsing, dedup, season-state transitions, quality recording).

### 4.2 Verify C1 — that the Silver quality gates actually *fire*
The gates run inside Spark, so this needs the Spark container. Two checks:

**(a) Happy path — measured rows land in Postgres**
```bash
make infra-up                                   # start MinIO + jupyter-spark
# trigger the pipeline (or run the silver DAG) so the notebook executes, then:
psql -d futebol-dados -c "SELECT stage, check_name, status, details
                          FROM pipeline_quality_checks
                          WHERE stage='silver' ORDER BY checked_at DESC LIMIT 15;"
```
Expect rows with **real `details`** (e.g. `player_null_rate=0.0123 …`), not just `pass` with empty details. Also confirm the report object exists:
```bash
docker exec jupyter-spark python - <<'PY'
# (or use mc) list the quality report written by the notebook
PY
# via mc:
docker run --rm --network datalake-network minio/mc sh -c \
  "mc alias set m http://minio:9000 $MINIO_ACCESS_KEY $MINIO_SECRET_KEY >/dev/null; \
   mc ls -r m/datalake-warehouse/quality/silver/"
```

**(b) Failure path — bad data must ABORT (the whole point of C1)**
Point the Silver notebook at an empty/garbage partition (e.g. a season with no Bronze data) and run it. Expect:
- the notebook **raises** `Silver quality gate FAILED: [...]` and the Airflow task fails;
- the season is marked `failed` in `pipeline_season_control`;
- **no** `pass` rows were written for that run.
This is the regression that proves the gates are no longer "theater."

### 4.3 Verify C2 — credentials are env-driven and fail-fast
```bash
# Unset → DAG helper must raise (fail-fast), not silently use a default:
python - <<'PY'
import os
for k in ("MINIO_ACCESS_KEY","MINIO_SECRET_KEY"): os.environ.pop(k, None)
import sys; sys.path.insert(0, "dags")
from lib.minio_config import get_minio_settings
try:
    get_minio_settings(); print("FAIL: should have raised")
except RuntimeError as e:
    print("OK fail-fast:", e)
PY

# Set → returns settings:
MINIO_ACCESS_KEY=x MINIO_SECRET_KEY=y python -c \
  "import sys; sys.path.insert(0,'dags'); from lib.minio_config import get_minio_settings; print(get_minio_settings())"

# No secret literals in application code:
grep -rn "minioadmin" --include=*.py dags/ tests/ || echo "none in python ✓"

# Override creds for the stack and confirm it still boots:
MINIO_ROOT_USER=admin MINIO_ROOT_PASSWORD=supersecret \
  docker compose -f infra/minio/docker-compose.yaml up -d
```

### 4.4 Component checks
```bash
# MinIO console: http://localhost:9001  (buckets: datalake-raw / -warehouse / -artifacts)
# Spark UI:      http://localhost:4040   (only while a SparkSession is running)
# Jupyter:       http://localhost:8888   (token via `make logs-spark`)

# Postgres control plane:
psql -d futebol-dados -c "SELECT league_key, season, status, last_error_stage
                          FROM pipeline_season_control ORDER BY season DESC;"
```

### 4.5 End-to-end (a full season through the Medallion)
1. `make infra-up` and ensure Airflow is running (`make airflow-up`).
2. Seed/!reset a season: `UPDATE pipeline_season_control SET status='pending' WHERE league_key='BRA-Brasileirao' AND season=2024;`
3. Let the Bronze DAG (`bronze_extraction__BRA-Brasileirao`, `@hourly`) pick it up, or trigger it.
4. Watch it flow: Bronze → (dataset) → Silver → (dataset) → Gold.
5. Assert outputs:
   - MinIO `datalake-raw/espn/brasileirao/2024/{schedule,matchsheet,lineup,events}.json`
   - Iceberg: `SELECT count(*) FROM lake.analytics.player_season_stats` (in the Gold notebook / Jupyter)
   - Postgres: `pipeline_season_control.status = 'complete'` and `pipeline_quality_checks` rows for silver+gold.

### 4.6 Iceberg table-level checks (in Jupyter / Spark SQL)
```sql
SELECT * FROM lake.analytics.match_statistics.snapshots;   -- history of writes
SELECT * FROM lake.analytics.match_statistics.files;       -- file/size health
```

---

## 5. Command cheat-sheet

| Goal | Command |
|---|---|
| Run all unit tests | `pytest tests/ -v` |
| One test file | `pytest tests/test_quality_helpers.py -v` |
| Syntax-check code | `python -m py_compile dags/*.py dags/lib/*.py` |
| Validate compose | `docker compose -f infra/<svc>/docker-compose.yaml config` |
| Start infra | `make infra-up` / stop: `make infra-down` |
| Spark/MinIO logs | `make logs-spark` / `make logs-minio` |
| Control-plane state | `psql -d futebol-dados -c "TABLE pipeline_season_control;"` |
| Quality results | `psql -d futebol-dados -c "TABLE pipeline_quality_checks;"` |
| Diff of a fix | `git show <commit>` (e.g. `git show efe6528`) |
| Full branch diff | `git diff main..de-hardening` |

---

_This file is updated at the end of each wave with the new verification steps._
