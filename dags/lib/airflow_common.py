"""Shared Airflow boilerplate reused across DAGs.

Centralizes the PostgreSQL connection factory, the Spark-container constants
(name, pool, readiness probe) and the notebook failure callback so the Bronze,
Silver, Gold and refresh DAGs don't each redefine them.
"""

import logging
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)

POSTGRES_CONN_ID = os.getenv("PG_CONN_ID", "db-pg-futebol-dados")

# The single Spark container Airflow drives, and the size-1 pool that serializes
# the Silver + Gold notebook tasks so they never operate it concurrently.
SPARK_CONTAINER = "jupyter-spark"
SPARK_POOL = "spark_notebook"

# Poll the Spark container until pyspark imports, instead of a fixed sleep.
SPARK_READINESS_CMD = (
    f"docker start {SPARK_CONTAINER} && "
    "echo 'Waiting for Spark container to become ready...' && "
    "for i in $(seq 1 30); do "
    f"  if docker exec {SPARK_CONTAINER} python -c 'import pyspark' 2>/dev/null; then "
    "    echo \"Spark container ready after ${i} attempt(s)\"; exit 0; "
    "  fi; "
    "  sleep 2; "
    "done; "
    "echo 'Spark container did not become ready within 60s' >&2; exit 1"
)


def get_pg_conn():
    """Return a psycopg2 connection (autocommit off) via Airflow's connection manager."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


def notebook_failure_callback(stage: str):
    """Build an ``on_failure_callback`` that marks the season failed for *stage*.

    Used by the Silver/Gold notebook BashOperators. Reads the season info that
    ``get_season_and_mark_started`` pushed to XCom and records the failure.
    """
    from lib.season_helpers import mark_stage_failed

    def _callback(context: dict) -> None:
        season_info = context["ti"].xcom_pull(task_ids="get_season_and_mark_started")
        if not season_info or not season_info.get("season_id"):
            LOGGER.warning("notebook_failure_callback[%s]: no season_id in XCom", stage)
            return
        error = str(context.get("exception", "Notebook execution failed"))
        mark_stage_failed(get_pg_conn, season_info["season_id"], stage=stage, error=error)
        LOGGER.error(
            "%s notebook failed for league=%s season=%s: %s",
            stage, season_info.get("league_key"), season_info.get("season"), error[:200],
        )

    return _callback
