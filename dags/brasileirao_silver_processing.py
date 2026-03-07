"""DAG: silver_processing -- Bronze (MinIO) to Silver (Iceberg).

Triggered automatically via Dataset when any Bronze extraction finishes.
Processes the next available ``bronze_done`` season across ALL leagues
(newest season first).

Starts the Spark container, executes the Silver notebook, and gracefully
stops the container.

The season and league are read from ``pipeline_season_control``
(status = bronze_done).  Both SEASON and LEAGUE_KEY are injected as env
vars into the notebook.

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

from lib.quality_helpers import record_stage_quality_passed
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
POSTGRES_CONN_ID = os.getenv("PG_CONN_ID", "db-pg-futebol-dados")

SPARK_CONTAINER = "jupyter-spark"
NOTEBOOK_PATH = "/home/jovyan/work/spark_silver_processing.ipynb"

# Shared Dataset — triggered by any Bronze DAG (any league)
bronze_dataset = Dataset("minio://datalake-raw/espn/bronze")
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
    """Mark the season as failed in pipeline_season_control."""
    season_info = context["ti"].xcom_pull(task_ids="get_season_and_mark_started")
    if not season_info or not season_info.get("season_id"):
        LOGGER.warning("_on_notebook_failure: no season_id in XCom, cannot mark failed")
        return

    error = str(context.get("exception", "Notebook execution failed"))
    mark_stage_failed(_get_conn, season_info["season_id"], stage="silver", error=error)
    LOGGER.error(
        "Silver notebook failed for league=%s season=%s: %s",
        season_info.get("league_key"), season_info.get("season"), error[:200],
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule=[bronze_dataset],  # Triggered when any Bronze DAG finishes
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "silver", "spark", "multi-liga"],
    doc_md=__doc__,
)
def silver_processing():

    # ------------------------------------------------------------------
    # Task 1: Get the next bronze_done season (any league) and mark started
    # ------------------------------------------------------------------
    @task(task_id="get_season_and_mark_started")
    def get_season_and_mark_started() -> dict[str, Any]:
        """Query pipeline_season_control for the next bronze_done season.

        Picks the highest season number across ALL leagues (newest first).
        Returns {season_id, season, league_key}, or {} if nothing is ready.
        """
        ensure_season_control_table(_get_conn)

        # league_key=None → search across all leagues
        season_row = get_pending_season(_get_conn, None, stage="silver")
        if season_row is None:
            LOGGER.info("No bronze_done season found across any league. Skipping.")
            return {}

        mark_stage_started(_get_conn, season_row["id"], stage="silver")
        LOGGER.info(
            "Silver starting: league=%s season=%s (id=%s)",
            season_row["league_key"], season_row["season"], season_row["id"],
        )
        return {
            "season_id": season_row["id"],
            "season": season_row["season"],
            "league_key": season_row["league_key"],
        }

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
    # Task 3: Execute Silver notebook (SEASON + LEAGUE_KEY injected)
    # ------------------------------------------------------------------
    run_spark_silver = BashOperator(
        task_id="run_spark_silver",
        env={
            "SEASON": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['season'] | string }}",
            "LEAGUE_KEY": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['league_key'] }}",
        },
        bash_command=(
            f"docker exec -e SEASON=$SEASON -e LEAGUE_KEY=$LEAGUE_KEY {SPARK_CONTAINER} "
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
    # Task 4: Mark season as silver_done
    # ------------------------------------------------------------------
    @task(task_id="mark_silver_done", trigger_rule=TriggerRule.ALL_SUCCESS)
    def mark_silver_done(season_info: dict[str, Any]) -> None:
        if not season_info or not season_info.get("season_id"):
            return
        mark_stage_completed(_get_conn, season_info["season_id"], stage="silver")
        LOGGER.info(
            "Silver complete: league=%s season=%s",
            season_info.get("league_key"), season_info.get("season"),
        )

    # ------------------------------------------------------------------
    # Task 5: Record quality checks
    # ------------------------------------------------------------------
    @task(task_id="record_silver_quality", trigger_rule=TriggerRule.ALL_SUCCESS)
    def record_silver_quality(season_info: dict[str, Any]) -> None:
        if not season_info or not season_info.get("season_id"):
            return
        record_stage_quality_passed(_get_conn, season_info["season_id"], stage="silver")
        LOGGER.info(
            "Silver quality recorded: league=%s season=%s",
            season_info.get("league_key"), season_info.get("season"),
        )

    # ------------------------------------------------------------------
    # Task 6: Stop Spark container (always runs)
    # ------------------------------------------------------------------
    stop_spark = BashOperator(
        task_id="stop_spark",
        bash_command=f"docker stop {SPARK_CONTAINER}",
        trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=2),
    )

    # Wire dependencies
    season_info = get_season_and_mark_started()
    s_done = mark_silver_done(season_info)
    s_quality = record_silver_quality(season_info)
    season_info >> start_spark >> run_spark_silver >> s_done >> s_quality >> stop_spark


# Instantiate the DAG
silver_processing()
