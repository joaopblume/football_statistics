"""Bronze extraction DAG factory — one DAG per configured league.

Each league gets its own independent Bronze DAG that:
- Runs hourly via ``@hourly`` schedule
- Picks the next pending season for that specific league (newest first)
- Extracts schedule → matchsheet → lineup → events in sequence
- Uploads JSON files to MinIO under ``espn/{slug}/{season}/``
- Emits a shared Dataset that triggers Silver processing

DAGs generated:
  bronze_extraction__BRA-Brasileirao
  bronze_extraction__ITA-Serie_A
  bronze_extraction__ENG-Premier_League
  bronze_extraction__FRA-Ligue_1

Produces dataset: minio://datalake-raw/espn/bronze  (shared across all leagues)
"""

import logging
import re
from datetime import timedelta
from typing import Any

import pendulum
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from lib.airflow_common import get_pg_conn
from lib.extraction_helpers import (
    extract_events_to_minio,
    extract_lineup_to_minio,
    extract_matchsheet_to_minio,
    extract_schedule_to_minio,
)
from lib.league_config import LEAGUE_CONFIGS
from lib.minio_config import get_minio_settings
from lib.season_helpers import (
    claim_next_season,
    mark_stage_completed,
    mark_stage_failed,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)

# MinIO credentials are resolved at task runtime from the environment via
# get_minio_settings() — there are no hardcoded secret defaults in code.

# Shared Dataset URI — all Bronze DAGs emit this same URI so Silver is
# triggered regardless of which league just finished.
BRONZE_DATASET = Dataset("minio://datalake-raw/espn/bronze")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),
}


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_get_conn = get_pg_conn


def _minio_kwargs() -> dict:
    # Resolved at task runtime; raises if MINIO_ACCESS_KEY/SECRET_KEY are unset.
    return get_minio_settings()


def _dag_id(league_key: str) -> str:
    """Convert a league key to a safe Airflow DAG ID."""
    safe = re.sub(r"[^a-zA-Z0-9]+", "_", league_key).strip("_")
    return f"bronze_extraction__{safe}"


# ---------------------------------------------------------------------------
# DAG factory
# ---------------------------------------------------------------------------

def _create_bronze_dag(league_key: str):
    """Create and return one Bronze extraction DAG for *league_key*."""

    dag_id = _dag_id(league_key)

    def _on_extraction_failure(context: dict) -> None:
        """Mark the season as failed when any extraction task fails."""
        season_info = context["ti"].xcom_pull(task_ids="get_season_and_mark_started")
        if not season_info or not season_info.get("season_id"):
            LOGGER.warning(
                "[%s] _on_extraction_failure: no season_id in XCom", dag_id
            )
            return
        error = str(context.get("exception", "Extraction task failed"))
        mark_stage_failed(_get_conn, season_info["season_id"], stage="bronze", error=error)
        LOGGER.error(
            "[%s] Bronze extraction failed for season=%s task=%s: %s",
            dag_id,
            season_info.get("season"),
            context["task_instance"].task_id,
            error[:200],
        )

    @dag(
        dag_id=dag_id,
        schedule="@hourly",
        start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["lakehouse", "bronze", "multi-liga", league_key.lower().replace(" ", "-")],
        doc_md=f"Bronze extraction for **{league_key}**.\n\n{__doc__}",
    )
    def bronze_extraction_dag():

        # ------------------------------------------------------------------
        # Task 1: Get next pending season for this league, mark as running
        # ------------------------------------------------------------------
        @task(task_id="get_season_and_mark_started")
        def get_season_and_mark_started() -> dict[str, Any]:
            # Atomically claim the next pending season (FOR UPDATE SKIP LOCKED).
            # Table is provisioned by migration 001 (no runtime DDL).
            season_row = claim_next_season(_get_conn, league_key, stage="bronze")
            if season_row is None:
                LOGGER.info(
                    "[%s] No pending season for stage=bronze. Nothing to do.", dag_id
                )
                raise AirflowSkipException("No pending season for bronze")
            LOGGER.info(
                "[%s] Bronze starting for season=%s (id=%s)",
                dag_id, season_row["season"], season_row["id"],
            )
            return {
                "season_id": season_row["id"],
                "season": season_row["season"],
                "league_key": season_row["league_key"],
            }

        # ------------------------------------------------------------------
        # Task 2: Extract schedule → writes game_map.json to MinIO
        # ------------------------------------------------------------------
        @task(task_id="extract_schedule", on_failure_callback=_on_extraction_failure)
        def extract_schedule(season_info: dict[str, Any]) -> dict[str, Any]:
            if not season_info or not season_info.get("season"):
                return {}
            result = extract_schedule_to_minio(
                league_key=league_key,
                season=season_info["season"],
                **_minio_kwargs(),
            )
            LOGGER.info(
                "[%s] Schedule: season=%s rows=%s games=%s (%.2fs)",
                dag_id, season_info["season"],
                result["schedule_rows"], result.get("game_count", 0),
                result["elapsed_seconds"],
            )
            return result

        # ------------------------------------------------------------------
        # Task 3: Extract matchsheet
        # ------------------------------------------------------------------
        @task(task_id="extract_matchsheet", on_failure_callback=_on_extraction_failure)
        def extract_matchsheet(
            season_info: dict[str, Any],
            sched: dict[str, Any],
        ) -> dict[str, Any]:
            if not season_info or not season_info.get("season"):
                return {}
            result = extract_matchsheet_to_minio(
                league_key=league_key,
                season=season_info["season"],
                **_minio_kwargs(),
            )
            LOGGER.info(
                "[%s] Matchsheet: season=%s rows=%s (%.2fs)",
                dag_id, season_info["season"],
                result["matchsheet_rows"], result["elapsed_seconds"],
            )
            return result

        # ------------------------------------------------------------------
        # Task 4: Extract lineup (reads game_map.json from MinIO)
        # ------------------------------------------------------------------
        @task(task_id="extract_lineup", on_failure_callback=_on_extraction_failure)
        def extract_lineup(
            season_info: dict[str, Any],
            sched: dict[str, Any],
            _ms: dict[str, Any],
        ) -> dict[str, Any]:
            if not season_info or not season_info.get("season") or not sched.get("game_count"):
                return {}
            result = extract_lineup_to_minio(
                league_key=league_key,
                season=season_info["season"],
                **_minio_kwargs(),
            )
            LOGGER.info(
                "[%s] Lineup: season=%s rows=%s (%.2fs)",
                dag_id, season_info["season"],
                result["lineup_rows"], result["elapsed_seconds"],
            )
            return result

        # ------------------------------------------------------------------
        # Task 5: Extract events (non-fatal; runs after lineup for cache warmth)
        # ------------------------------------------------------------------
        @task(task_id="extract_events", on_failure_callback=_on_extraction_failure)
        def extract_events(
            season_info: dict[str, Any],
            sched: dict[str, Any],
            _lu: dict[str, Any],
        ) -> dict[str, Any]:
            if not season_info or not season_info.get("season") or not sched.get("game_count"):
                return {}
            result = extract_events_to_minio(
                league_key=league_key,
                season=season_info["season"],
                **_minio_kwargs(),
            )
            LOGGER.info(
                "[%s] Events: season=%s rows=%s (%.2fs)",
                dag_id, season_info["season"],
                result["events_rows"], result["elapsed_seconds"],
            )
            return result

        # ------------------------------------------------------------------
        # Task 6: Mark bronze_done — emits shared bronze Dataset
        # ------------------------------------------------------------------
        @task(
            task_id="mark_bronze_done",
            outlets=[BRONZE_DATASET],
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        def mark_bronze_done(
            season_info: dict[str, Any],
            sched: dict[str, Any],
            ms: dict[str, Any],
            lu: dict[str, Any],
            ev: dict[str, Any],
        ) -> None:
            if not season_info or not season_info.get("season_id"):
                return
            mark_stage_completed(_get_conn, season_info["season_id"], stage="bronze")
            LOGGER.info(
                "[%s] Bronze complete: season=%s schedule=%s matchsheet=%s lineup=%s events=%s",
                dag_id,
                season_info.get("season"),
                sched.get("schedule_rows", 0),
                ms.get("matchsheet_rows", 0),
                lu.get("lineup_rows", 0),
                ev.get("events_rows", 0),
            )

        # Wire dependencies
        season_info = get_season_and_mark_started()
        sched = extract_schedule(season_info)
        ms = extract_matchsheet(season_info, sched)
        lu = extract_lineup(season_info, sched, ms)
        ev = extract_events(season_info, sched, lu)
        mark_bronze_done(season_info, sched, ms, lu, ev)

    return bronze_extraction_dag()


# ---------------------------------------------------------------------------
# Generate one DAG per configured league and register it in globals()
# ---------------------------------------------------------------------------

for _league_key in LEAGUE_CONFIGS:
    globals()[_dag_id(_league_key)] = _create_bronze_dag(_league_key)
