"""DAG: brasileirao_bronze_extraction -- Extract data and save to Bronze (MinIO).

This DAG extracts schedule, matchsheet, and lineup data from the ESPN API
via soccerdata, and uploads them as JSON files to MinIO.

The season to process is read from ``pipeline_season_control`` in PostgreSQL
(managed by dags/lib/season_helpers.py).  If no season is pending, the DAG
logs a skip message and exits cleanly without error.

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

from lib.extraction_helpers import extract_and_upload_to_minio
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

# MinIO connection settings
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
# Connection helper (same pattern as consume_brasileirao_queue_to_pg)
# ---------------------------------------------------------------------------

def _get_conn():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule=None,  # Manual trigger
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "bronze", "brasileirao"],
    doc_md=__doc__,
)
def brasileirao_bronze_extraction():

    @task(outlets=[bronze_dataset])
    def extract_and_upload_bronze() -> dict[str, Any]:
        """Fetch ESPN data and upload to MinIO Bronze layer.

        Reads the season to process from pipeline_season_control.
        Exits cleanly (returning an empty dict) if no season is pending.
        Updates the season control row with start/complete/failed status.
        """
        # Ensure the season control table exists (idempotent)
        ensure_season_control_table(_get_conn)

        # Find the next season eligible for Bronze extraction
        season_row = get_pending_season(_get_conn, LEAGUE_KEY, stage="bronze")

        if season_row is None:
            LOGGER.info(
                "No pending season found for league=%s stage=bronze. Nothing to do.",
                LEAGUE_KEY,
            )
            return {}

        season = season_row["season"]
        season_id = season_row["id"]
        LOGGER.info(
            "Starting Bronze extraction for league=%s season=%s (id=%s)",
            LEAGUE_KEY, season, season_id,
        )

        # Mark as running before any work begins
        mark_stage_started(_get_conn, season_id, stage="bronze")

        try:
            result = extract_and_upload_to_minio(
                league_key=LEAGUE_KEY,
                season=season,
                minio_endpoint=MINIO_ENDPOINT,
                minio_access_key=MINIO_ACCESS_KEY,
                minio_secret_key=MINIO_SECRET_KEY,
            )
        except Exception as exc:
            mark_stage_failed(_get_conn, season_id, stage="bronze", error=str(exc))
            raise

        LOGGER.info(
            "Bronze extraction complete: season=%s schedule=%s matchsheet=%s lineup=%s (%.2fs)",
            season,
            result["schedule_rows"],
            result["matchsheet_rows"],
            result["lineup_rows"],
            result["elapsed_seconds"],
        )

        # Mark as done — triggers Silver DAG via Dataset
        mark_stage_completed(_get_conn, season_id, stage="bronze")

        return {**result, "season": season, "season_id": season_id}

    extract_and_upload_bronze()


# Instantiate the DAG
brasileirao_bronze_extraction()