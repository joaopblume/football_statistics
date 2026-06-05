"""DAG: iceberg_maintenance -- weekly Iceberg table maintenance + metrics.

Runs ``rewrite_data_files`` (compaction) and ``expire_snapshots`` on every
Silver/Gold table and logs per-table file/snapshot counts — table-format
observability + hygiene so per-partition overwrites don't accumulate small
files and stale metadata.

Reuses the shared jupyter-spark container (and the size-1 ``spark_notebook``
pool, so it never overlaps Silver/Gold).
"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.task.trigger_rule import TriggerRule

from lib.airflow_common import (
    SPARK_CONTAINER,
    SPARK_POOL,
    SPARK_READINESS_CMD,
    pipeline_failure_notifier,
)

MAINT_SCRIPT = "/home/jovyan/work/iceberg_maintenance.py"

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    on_failure_callback=pipeline_failure_notifier,
    tags=["lakehouse", "maintenance", "iceberg"],
    doc_md=__doc__,
)
def iceberg_maintenance():
    start_spark = BashOperator(
        task_id="start_spark",
        bash_command=SPARK_READINESS_CMD,
        pool=SPARK_POOL,
        execution_timeout=timedelta(minutes=2),
    )

    run_maintenance = BashOperator(
        task_id="run_maintenance",
        bash_command=f"docker exec {SPARK_CONTAINER} python {MAINT_SCRIPT}",
        pool=SPARK_POOL,
        execution_timeout=timedelta(hours=1),
    )

    stop_spark = BashOperator(
        task_id="stop_spark",
        bash_command=f"docker stop {SPARK_CONTAINER}",
        trigger_rule=TriggerRule.ALL_DONE,
        pool=SPARK_POOL,
        execution_timeout=timedelta(minutes=2),
    )

    start_spark >> run_maintenance >> stop_spark


iceberg_maintenance()
