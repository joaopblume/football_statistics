"""DAG: gold_processing -- Silver (Iceberg) to Gold (Iceberg).

Triggered automatically via Dataset when the Silver processing finishes.
Processes the next available ``silver_done`` season across ALL leagues
(newest season first).

Starts the Spark container, executes the Gold notebook, and gracefully
stops the container.  Both SEASON and LEAGUE_KEY are injected as env vars.

Status transitions: silver_done → gold_running → complete (or failed).

Produces dataset: iceberg://lake/analytics/gold
"""

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from lib.airflow_common import (
    SPARK_CONTAINER,
    SPARK_POOL,
    SPARK_READINESS_CMD,
    get_pg_conn,
    notebook_failure_callback,
)
from lib.quality_helpers import record_stage_quality_passed
from lib.season_helpers import claim_next_season, mark_stage_completed

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)

NOTEBOOK_PATH = "/home/jovyan/work/spark_gold_processing.ipynb"

# Datasets
silver_dataset = Dataset("iceberg://lake/analytics/silver")
gold_dataset = Dataset("iceberg://lake/analytics/gold")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=3),
}

# Connection + notebook-failure callback come from lib.airflow_common.
_get_conn = get_pg_conn
_on_notebook_failure = notebook_failure_callback("gold")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule=[silver_dataset],  # Triggered when Silver finishes
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "gold", "spark", "multi-liga"],
    doc_md=__doc__,
)
def gold_processing():

    # ------------------------------------------------------------------
    # Task 1: Get the next silver_done season (any league) and mark started
    # ------------------------------------------------------------------
    @task(task_id="get_season_and_mark_started")
    def get_season_and_mark_started() -> dict[str, Any]:
        """Query pipeline_season_control for the next silver_done season.

        Picks the highest season number across ALL leagues (newest first).
        Returns {season_id, season, league_key}, or {} if nothing is ready.
        """
        # Atomically claim the next silver_done season across ALL leagues
        # (FOR UPDATE SKIP LOCKED). Table is provisioned by migration 001.
        season_row = claim_next_season(_get_conn, None, stage="gold")
        if season_row is None:
            LOGGER.info("No silver_done season found across any league. Skipping.")
            raise AirflowSkipException("No silver_done season to process")

        LOGGER.info(
            "Gold starting: league=%s season=%s (id=%s)",
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
        bash_command=SPARK_READINESS_CMD,
        pool=SPARK_POOL,
        execution_timeout=timedelta(minutes=2),
    )

    # ------------------------------------------------------------------
    # Task 3: Execute Gold notebook (SEASON + LEAGUE_KEY injected)
    # ------------------------------------------------------------------
    run_spark_gold = BashOperator(
        task_id="run_spark_gold",
        env={
            "SEASON": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['season'] | string }}",
            "LEAGUE_KEY": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['league_key'] }}",
        },
        bash_command=(
            f'docker exec -e SEASON="$SEASON" -e LEAGUE_KEY="$LEAGUE_KEY" {SPARK_CONTAINER} '
            f"jupyter nbconvert --to notebook --execute {NOTEBOOK_PATH} "
            "--output-dir /tmp "
            "--ExecutePreprocessor.timeout=1800 "
            "--ExecutePreprocessor.kernel_name=python3"
        ),
        execution_timeout=timedelta(hours=1),
        outlets=[gold_dataset],
        on_failure_callback=_on_notebook_failure,
        pool=SPARK_POOL,
    )

    # ------------------------------------------------------------------
    # Task 4: Mark season as complete
    # ------------------------------------------------------------------
    @task(task_id="mark_gold_done", trigger_rule=TriggerRule.ALL_SUCCESS)
    def mark_gold_done(season_info: dict[str, Any]) -> None:
        if not season_info or not season_info.get("season_id"):
            return
        mark_stage_completed(_get_conn, season_info["season_id"], stage="gold")
        LOGGER.info(
            "Pipeline complete: league=%s season=%s",
            season_info.get("league_key"), season_info.get("season"),
        )

    # ------------------------------------------------------------------
    # Task 5: Record quality checks
    # ------------------------------------------------------------------
    @task(task_id="record_gold_quality", trigger_rule=TriggerRule.ALL_SUCCESS)
    def record_gold_quality(season_info: dict[str, Any]) -> None:
        if not season_info or not season_info.get("season_id"):
            return
        record_stage_quality_passed(_get_conn, season_info["season_id"], stage="gold")
        LOGGER.info(
            "Gold quality recorded: league=%s season=%s",
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
        pool=SPARK_POOL,
    )

    # Wire dependencies
    season_info = get_season_and_mark_started()
    g_done = mark_gold_done(season_info)
    g_quality = record_gold_quality(season_info)
    season_info >> start_spark >> run_spark_gold >> g_done >> g_quality >> stop_spark


# Instantiate the DAG
gold_processing()
