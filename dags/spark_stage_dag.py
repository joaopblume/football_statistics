"""Factory for the Silver and Gold Spark-stage DAGs (ephemeral containers).

Each stage claims the next eligible season from ``pipeline_season_control`` and
runs its `.py` Spark job as a fresh ``docker run --rm football-spark`` container
(true isolation — no shared jupyter-spark container, no start/stop dance). The
size-1 ``spark_notebook`` pool still serializes the heavy jobs so two Spark
containers don't run at once on the memory-constrained host.

Replaces brasileirao_silver_processing.py + brasileirao_gold_processing.py.

DAGs produced: ``silver_processing`` (Bronze→Silver), ``gold_processing`` (Silver→Gold).
"""

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from lib.airflow_common import (
    SPARK_POOL,
    get_pg_conn,
    notebook_failure_callback,
    pipeline_failure_notifier,
)
from lib.league_config import get_league_slug
from lib.minio_config import get_minio_settings, make_s3_client
from lib.quality_helpers import (
    record_quality_check,
    record_quality_report,
    record_stage_quality_passed,
)
from lib.season_helpers import claim_next_season, mark_stage_completed

# Repo root (this file is dags/spark_stage_dag.py) → used for the job + conf mounts.
REPO = Path(__file__).resolve().parent.parent
WAREHOUSE_BUCKET = os.getenv("MINIO_WAREHOUSE_BUCKET", "datalake-warehouse")

bronze_dataset = Dataset("minio://datalake-raw/espn/bronze")
silver_dataset = Dataset("iceberg://lake/analytics/silver")
gold_dataset = Dataset("iceberg://lake/analytics/gold")

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=2),
}

# Per-stage configuration.
STAGES = {
    "silver": {
        "dag_id": "silver_processing",
        "schedule": [bronze_dataset],
        "job": "silver_job.py",
        "outlet": silver_dataset,
        "tags": ["lakehouse", "silver", "spark", "multi-liga"],
    },
    "gold": {
        "dag_id": "gold_processing",
        "schedule": [silver_dataset],
        "job": "gold_job.py",
        "outlet": gold_dataset,
        "tags": ["lakehouse", "gold", "spark", "multi-liga"],
    },
}


def _run_cmd(job_file: str) -> str:
    """Ephemeral Spark run: a fresh container per job, baked Iceberg jars."""
    return (
        "docker run --rm --network datalake-network "
        '-e AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" -e AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" '
        '-e SEASON="$SEASON" -e LEAGUE_KEY="$LEAGUE_KEY" '
        f"-v {REPO}/spark_jobs:/jobs:ro "
        f"-v {REPO}/infra/spark/conf/spark-defaults.conf:/usr/local/spark/conf/spark-defaults.conf:ro "
        "football-spark:latest spark-submit "
        "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        f"/jobs/{job_file}"
    )


def _create_stage_dag(stage: str):
    cfg = STAGES[stage]

    @dag(
        dag_id=cfg["dag_id"],
        schedule=cfg["schedule"],
        start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        on_failure_callback=pipeline_failure_notifier,
        tags=cfg["tags"],
        doc_md=__doc__,
    )
    def stage_dag():

        @task(task_id="get_season_and_mark_started")
        def get_season_and_mark_started() -> dict[str, Any]:
            season_row = claim_next_season(get_pg_conn, None, stage=stage)
            if season_row is None:
                raise AirflowSkipException(f"No season eligible for {stage}")
            return {
                "season_id": season_row["id"],
                "season": season_row["season"],
                "league_key": season_row["league_key"],
            }

        run_spark = BashOperator(
            task_id=f"run_spark_{stage}",
            env={
                "SEASON": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['season'] | string }}",
                "LEAGUE_KEY": "{{ ti.xcom_pull(task_ids='get_season_and_mark_started')['league_key'] }}",
            },
            append_env=True,  # keep MINIO_ACCESS_KEY/SECRET from the Airflow env
            bash_command=_run_cmd(cfg["job"]),
            execution_timeout=timedelta(hours=1),
            outlets=[cfg["outlet"]],
            on_failure_callback=notebook_failure_callback(stage),
            pool=SPARK_POOL,
        )

        @task(task_id=f"mark_{stage}_done", trigger_rule=TriggerRule.ALL_SUCCESS)
        def mark_done(season_info: dict[str, Any]) -> None:
            if season_info and season_info.get("season_id"):
                mark_stage_completed(get_pg_conn, season_info["season_id"], stage=stage)

        @task(task_id=f"record_{stage}_quality", trigger_rule=TriggerRule.ALL_SUCCESS)
        def record_quality(season_info: dict[str, Any]) -> None:
            if not season_info or not season_info.get("season_id"):
                return
            if stage == "silver":
                key = f"quality/silver/{get_league_slug(season_info['league_key'])}/{season_info['season']}/report.json"
                try:
                    s3 = make_s3_client(get_minio_settings())
                    checks = json.loads(
                        s3.get_object(Bucket=WAREHOUSE_BUCKET, Key=key)["Body"].read()
                    ).get("checks", [])
                    record_quality_report(get_pg_conn, season_info["season_id"], "silver", checks)
                except Exception as exc:  # noqa: BLE001
                    record_quality_check(
                        get_pg_conn, season_info["season_id"], "silver",
                        "quality_report_available", "warn", details=f"missing: {key} ({exc})",
                    )
            else:
                record_stage_quality_passed(get_pg_conn, season_info["season_id"], stage="gold")

        season_info = get_season_and_mark_started()
        season_info >> run_spark
        run_spark >> mark_done(season_info) >> record_quality(season_info)

    return stage_dag()


for _stage in STAGES:
    globals()[STAGES[_stage]["dag_id"]] = _create_stage_dag(_stage)
