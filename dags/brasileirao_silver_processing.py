"""DAG: brasileirao_silver_processing -- Bronze (MinIO) to Silver (Iceberg).

Triggered automatically via Dataset when the Bronze extraction finishes.
Starts the Spark container, executes notebook to build dimensions and facts,
and gracefully stops the container.

The season to process is read from ``pipeline_season_control`` (status = bronze_done).
Status transitions: bronze_done → silver_running → silver_done (or failed).

Produces dataset: iceberg://lake/analytics/silver
"""

import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.datasets import Dataset
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

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

LOGGER = logging.getLogger(__name__)
LEAGUE_KEY = "BRA-Brasileirao"
POSTGRES_CONN_ID = os.getenv("PG_CONN_ID", "db-pg-futebol-dados")

SPARK_CONTAINER = "jupyter-spark"
NOTEBOOK_PATH = "/home/jovyan/work/spark_silver_processing.ipynb"

# Datasets
bronze_dataset = Dataset("minio://datalake-raw/espn/brasileirao/bronze")
silver_dataset = Dataset("iceberg://lake/analytics/silver")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),
}


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_conn():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    return conn


# ---------------------------------------------------------------------------
# Failure callback for the notebook BashOperator
# ---------------------------------------------------------------------------

def _on_notebook_failure(context: dict) -> None:
    """Mark the season as failed in pipeline_season_control.

    Called automatically by Airflow when the run_spark_silver task fails.
    Reads the season_id from the upstream get_season task via XCom.
    """
    season_info = context["ti"].xcom_pull(task_ids="get_season_and_mark_started")
    if not season_info or not season_info.get("season_id"):
        LOGGER.warning("_on_notebook_failure: no season_id in XCom, cannot mark failed")
        return

    error = str(context.get("exception", "Notebook execution failed"))
    mark_stage_failed(_get_conn, season_info["season_id"], stage="silver", error=error)
    LOGGER.error(
        "Silver notebook failed for season=%s: %s",
        season_info.get("season"), error[:200],
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule=[bronze_dataset],  # Triggered when Bronze finishes
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "silver", "spark", "brasileirao"],
    doc_md=__doc__,
)
def brasileirao_silver_processing():

    # ------------------------------------------------------------------
    # Task 1: Get the season to process and mark it as running
    # ------------------------------------------------------------------
    @task(task_id="get_season_and_mark_started")
    def get_season_and_mark_started() -> dict[str, Any]:
        """Query pipeline_season_control for a bronze_done season.

        Returns {season_id, season} on success, or {} if nothing is ready.
        Marks the row as silver_running before returning.
        """
        ensure_season_control_table(_get_conn)

        season_row = get_pending_season(_get_conn, LEAGUE_KEY, stage="silver")
        if season_row is None:
            LOGGER.info("No bronze_done season found for league=%s. Skipping.", LEAGUE_KEY)
            return {}

        mark_stage_started(_get_conn, season_row["id"], stage="silver")
        LOGGER.info(
            "Silver processing starting for league=%s season=%s (id=%s)",
            LEAGUE_KEY, season_row["season"], season_row["id"],
        )
        return {"season_id": season_row["id"], "season": season_row["season"]}

    # ------------------------------------------------------------------
    # Task 2: Start Spark container
    # ------------------------------------------------------------------
    start_spark = BashOperator(
        task_id="start_spark",
        bash_command=(
            f"docker start {SPARK_CONTAINER} && "
            "echo 'Waiting for Spark container...' && "
            "sleep 10 && "
            f"docker exec {SPARK_CONTAINER} python -c 'print(\"Container ready\")'"
        ),
        execution_timeout=timedelta(minutes=2),
    )

    # ------------------------------------------------------------------
    # Task 3: Execute Silver notebook
    # ------------------------------------------------------------------
    run_spark_silver = BashOperator(
        task_id="run_spark_silver",
        env={
            "SEASON": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['season'] | string }}",
        },
        bash_command=(
            f"docker exec -e SEASON=$SEASON {SPARK_CONTAINER} "
            f"jupyter nbconvert --to notebook --execute {NOTEBOOK_PATH} "
            "--output-dir /tmp "
            "--ExecutePreprocessor.timeout=1800 "
            "--ExecutePreprocessor.kernel_name=python3"
        ),
        execution_timeout=timedelta(hours=1),
        outlets=[silver_dataset],
        on_failure_callback=_on_notebook_failure,
    )

    # ------------------------------------------------------------------
    # Task 4: Mark season as silver_done (only runs on notebook success)
    # ------------------------------------------------------------------
    @task(task_id="mark_silver_done", trigger_rule=TriggerRule.ALL_SUCCESS)
    def mark_silver_done(season_info: dict[str, Any]) -> None:
        """Mark the season as silver_done in pipeline_season_control."""
        if not season_info or not season_info.get("season_id"):
            return
        mark_stage_completed(_get_conn, season_info["season_id"], stage="silver")
        LOGGER.info("Silver processing complete for season=%s", season_info.get("season"))

    # ------------------------------------------------------------------
    # Task 5: Stop Spark container (always runs, even on failure)
    # ------------------------------------------------------------------
    stop_spark = BashOperator(
        task_id="stop_spark",
        bash_command=f"docker stop {SPARK_CONTAINER}",
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=2),
    )

    # ------------------------------------------------------------------
    # Wire dependencies
    # ------------------------------------------------------------------
    season_info = get_season_and_mark_started()
    season_info >> start_spark >> run_spark_silver >> mark_silver_done(season_info) >> stop_spark


# Instantiate the DAG
brasileirao_silver_processing()