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

import json
import logging
import os
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
    pipeline_failure_notifier,
)
from lib.league_config import get_league_slug
from lib.minio_config import get_minio_settings, make_s3_client
from lib.quality_helpers import record_quality_check, record_quality_report
from lib.season_helpers import claim_next_season, mark_stage_completed

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)

NOTEBOOK_PATH = "/home/jovyan/work/spark_silver_processing.ipynb"
# Bucket where the Silver notebook writes its quality report JSON
WAREHOUSE_BUCKET = os.getenv("MINIO_WAREHOUSE_BUCKET", "datalake-warehouse")

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

# Connection + notebook-failure callback come from lib.airflow_common.
_get_conn = get_pg_conn
_on_notebook_failure = notebook_failure_callback("silver")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    schedule=[bronze_dataset],  # Triggered when any Bronze DAG finishes
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=pipeline_failure_notifier,
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
        # Atomically claim the next bronze_done season across ALL leagues
        # (FOR UPDATE SKIP LOCKED). Table is provisioned by migration 001.
        season_row = claim_next_season(_get_conn, None, stage="silver")
        if season_row is None:
            LOGGER.info("No bronze_done season found across any league. Skipping.")
            raise AirflowSkipException("No bronze_done season to process")

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
        bash_command=SPARK_READINESS_CMD,
        pool=SPARK_POOL,
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
            f'docker exec -e SEASON="$SEASON" -e LEAGUE_KEY="$LEAGUE_KEY" {SPARK_CONTAINER} '
            f"jupyter nbconvert --to notebook --execute {NOTEBOOK_PATH} "
            "--output-dir /tmp "
            "--ExecutePreprocessor.timeout=1800 "
            "--ExecutePreprocessor.kernel_name=python3"
        ),
        execution_timeout=timedelta(hours=1),
        outlets=[silver_dataset],
        on_failure_callback=_on_notebook_failure,
        pool=SPARK_POOL,
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
        """Persist the MEASURED Silver quality checks the notebook computed.

        The notebook evaluates each check, writes a report to MinIO, and
        aborts on any hard failure — so reaching this task means nothing
        failed. We read that report and record each measured (status, details)
        instead of a blind 'pass'.
        """
        if not season_info or not season_info.get("season_id"):
            return

        league_key = season_info["league_key"]
        season = season_info["season"]
        key = f"quality/silver/{get_league_slug(league_key)}/{season}/report.json"

        try:
            s3 = make_s3_client(get_minio_settings())
            obj = s3.get_object(Bucket=WAREHOUSE_BUCKET, Key=key)
            checks = json.loads(obj["Body"].read()).get("checks", [])
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Silver quality report missing/unreadable at s3://%s/%s (%s) — "
                "recording a single 'warn' so the gap stays visible.",
                WAREHOUSE_BUCKET, key, exc,
            )
            record_quality_check(
                _get_conn, season_info["season_id"], "silver",
                "quality_report_available", "warn", details=f"missing: {key}",
            )
            return

        n = record_quality_report(_get_conn, season_info["season_id"], "silver", checks)
        LOGGER.info(
            "Silver quality recorded: league=%s season=%s checks=%d",
            league_key, season, n,
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
    s_done = mark_silver_done(season_info)
    s_quality = record_silver_quality(season_info)
    season_info >> start_spark >> run_spark_silver >> s_done >> s_quality >> stop_spark


# Instantiate the DAG
silver_processing()
