"""DAG: brasileirao_bronze_extraction -- Extract data and save to Bronze (MinIO).

Each entity (schedule, matchsheet, lineup, events) is extracted in a separate
sequential task so that soccerdata's disk cache is always warm for the next step.

The season to process is read from ``pipeline_season_control`` in PostgreSQL.
Seasons are processed in order (oldest pending first).  If no season is pending,
the DAG logs a skip message and exits cleanly.

Produces dataset: minio://datalake-raw/espn/brasileirao/bronze
"""

import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from lib.extraction_helpers import (
    extract_events_to_minio,
    extract_lineup_to_minio,
    extract_matchsheet_to_minio,
    extract_schedule_to_minio,
)
from lib.season_helpers import (
    ensure_season_control_table,
    get_pending_season,
    mark_stage_completed,
    mark_stage_failed,
    mark_stage_started,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEAGUE_KEY = "BRA-Brasileirao"
LOGGER = logging.getLogger(__name__)
POSTGRES_CONN_ID = os.getenv("PG_CONN_ID", "db-pg-futebol-dados")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# Dataset produced by this DAG (consumed by Silver)
bronze_dataset = Dataset("minio://datalake-raw/espn/brasileirao/bronze")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


def _minio_kwargs() -> dict:
    return {
        "minio_endpoint": MINIO_ENDPOINT,
        "minio_access_key": MINIO_ACCESS_KEY,
        "minio_secret_key": MINIO_SECRET_KEY,
    }


def _on_extraction_failure(context: dict) -> None:
    """Mark the season as failed when any extraction task fails.

    Reads the season_id from the get_season_and_mark_started XCom.
    """
    season_info = context["ti"].xcom_pull(task_ids="get_season_and_mark_started")
    if not season_info or not season_info.get("season_id"):
        LOGGER.warning("_on_extraction_failure: no season_id in XCom, cannot mark failed")
        return
    error = str(context.get("exception", "Extraction task failed"))
    mark_stage_failed(_get_conn, season_info["season_id"], stage="bronze", error=error)
    LOGGER.error(
        "Bronze extraction failed for season=%s task=%s: %s",
        season_info.get("season"),
        context["task_instance"].task_id,
        error[:200],
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "bronze", "brasileirao"],
    doc_md=__doc__,
)
def brasileirao_bronze_extraction():

    # ------------------------------------------------------------------
    # Task 1: Get the next pending season and mark it as running
    # ------------------------------------------------------------------
    @task(task_id="get_season_and_mark_started")
    def get_season_and_mark_started() -> dict[str, Any]:
        """Query pipeline_season_control for the next pending season.

        Returns ``{season_id, season}`` on success, or ``{}`` if nothing
        is ready.  Marks the row as bronze_running before returning.
        """
        ensure_season_control_table(_get_conn)

        season_row = get_pending_season(_get_conn, LEAGUE_KEY, stage="bronze")
        if season_row is None:
            LOGGER.info(
                "No pending season for league=%s stage=bronze. Nothing to do.", LEAGUE_KEY
            )
            return {}

        mark_stage_started(_get_conn, season_row["id"], stage="bronze")
        LOGGER.info(
            "Bronze extraction starting for league=%s season=%s (id=%s)",
            LEAGUE_KEY, season_row["season"], season_row["id"],
        )
        return {"season_id": season_row["id"], "season": season_row["season"]}

    # ------------------------------------------------------------------
    # Task 2: Extract schedule → uploads schedule.json, returns game_map
    # ------------------------------------------------------------------
    @task(task_id="extract_schedule", on_failure_callback=_on_extraction_failure)
    def extract_schedule(season_info: dict[str, Any]) -> dict[str, Any]:
        """Fetch the season schedule from ESPN and upload to MinIO."""
        if not season_info or not season_info.get("season"):
            return {}
        result = extract_schedule_to_minio(
            league_key=LEAGUE_KEY,
            season=season_info["season"],
            **_minio_kwargs(),
        )
        LOGGER.info(
            "Schedule extracted: season=%s rows=%s games=%s (%.2fs)",
            season_info["season"],
            result["schedule_rows"],
            len(result.get("game_map", {})),
            result["elapsed_seconds"],
        )
        return result  # {schedule_rows, game_map, bronze_base, elapsed_seconds}

    # ------------------------------------------------------------------
    # Task 3: Extract matchsheet → uploads matchsheet.json
    # ------------------------------------------------------------------
    @task(task_id="extract_matchsheet", on_failure_callback=_on_extraction_failure)
    def extract_matchsheet(
        season_info: dict[str, Any],
        sched: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch per-match team stats from ESPN and upload to MinIO."""
        if not season_info or not season_info.get("season"):
            return {}
        result = extract_matchsheet_to_minio(
            league_key=LEAGUE_KEY,
            season=season_info["season"],
            **_minio_kwargs(),
        )
        LOGGER.info(
            "Matchsheet extracted: season=%s rows=%s (%.2fs)",
            season_info["season"], result["matchsheet_rows"], result["elapsed_seconds"],
        )
        return result  # {matchsheet_rows, elapsed_seconds}

    # ------------------------------------------------------------------
    # Task 4: Extract lineup → uploads lineup.json (needs game_map)
    # ------------------------------------------------------------------
    @task(task_id="extract_lineup", on_failure_callback=_on_extraction_failure)
    def extract_lineup(
        season_info: dict[str, Any],
        sched: dict[str, Any],
        _ms: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch per-player lineup data from ESPN and upload to MinIO."""
        if not season_info or not season_info.get("season") or not sched.get("game_map"):
            return {}
        result = extract_lineup_to_minio(
            league_key=LEAGUE_KEY,
            season=season_info["season"],
            game_map=sched["game_map"],
            **_minio_kwargs(),
        )
        LOGGER.info(
            "Lineup extracted: season=%s rows=%s (%.2fs)",
            season_info["season"], result["lineup_rows"], result["elapsed_seconds"],
        )
        return result  # {lineup_rows, elapsed_seconds}

    # ------------------------------------------------------------------
    # Task 5: Extract events → uploads events.json (non-fatal if absent)
    # ------------------------------------------------------------------
    @task(task_id="extract_events", on_failure_callback=_on_extraction_failure)
    def extract_events(
        season_info: dict[str, Any],
        sched: dict[str, Any],
        _lu: dict[str, Any],
    ) -> dict[str, Any]:
        """Fetch match events from ESPN and upload to MinIO.

        Runs after lineup so the ESPN summary cache is fully populated.
        A missing events endpoint is treated as non-fatal.
        """
        if not season_info or not season_info.get("season") or not sched.get("game_map"):
            return {}
        result = extract_events_to_minio(
            league_key=LEAGUE_KEY,
            season=season_info["season"],
            game_map=sched["game_map"],
            **_minio_kwargs(),
        )
        LOGGER.info(
            "Events extracted: season=%s rows=%s (%.2fs)",
            season_info["season"], result["events_rows"], result["elapsed_seconds"],
        )
        return result  # {events_rows, elapsed_seconds}

    # ------------------------------------------------------------------
    # Task 6: Mark season as bronze_done — emits the bronze Dataset
    # ------------------------------------------------------------------
    @task(
        task_id="mark_bronze_done",
        outlets=[bronze_dataset],
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    def mark_bronze_done(
        season_info: dict[str, Any],
        sched: dict[str, Any],
        ms: dict[str, Any],
        lu: dict[str, Any],
        ev: dict[str, Any],
    ) -> None:
        """Mark season as bronze_done and emit the bronze Dataset trigger."""
        if not season_info or not season_info.get("season_id"):
            return
        mark_stage_completed(_get_conn, season_info["season_id"], stage="bronze")
        LOGGER.info(
            "Bronze complete: season=%s schedule=%s matchsheet=%s lineup=%s events=%s",
            season_info.get("season"),
            sched.get("schedule_rows", 0),
            ms.get("matchsheet_rows", 0),
            lu.get("lineup_rows", 0),
            ev.get("events_rows", 0),
        )

    # ------------------------------------------------------------------
    # Wire dependencies
    # ------------------------------------------------------------------
    season_info = get_season_and_mark_started()
    sched = extract_schedule(season_info)
    ms = extract_matchsheet(season_info, sched)
    lu = extract_lineup(season_info, sched, ms)
    ev = extract_events(season_info, sched, lu)
    mark_bronze_done(season_info, sched, ms, lu, ev)


# Instantiate the DAG
brasileirao_bronze_extraction()
