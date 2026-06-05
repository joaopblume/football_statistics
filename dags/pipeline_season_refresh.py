"""DAG: pipeline_season_refresh -- re-pull the latest (live) season per league.

A season is terminal once it reaches ``complete``, but a *live* season keeps
gaining matches week to week. This DAG runs weekly and resets the most recent
``complete`` season of each league back to ``pending``, so the Bronze DAGs
re-extract it and the fresh data flows through Silver/Gold again.

It only touches the highest ``complete`` season per league — never one that is
currently mid-pipeline — so it is safe to run alongside the extraction DAGs.
"""

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from lib.airflow_common import get_pg_conn
from lib.season_helpers import requeue_latest_complete_seasons

LOGGER = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

_get_conn = get_pg_conn


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "refresh", "multi-liga"],
    doc_md=__doc__,
)
def pipeline_season_refresh():

    @task(task_id="requeue_latest_complete_seasons")
    def requeue() -> str:
        requeued = requeue_latest_complete_seasons(_get_conn)
        LOGGER.info(
            "Season refresh: re-queued %d season(s): %s", len(requeued), requeued
        )
        return f"re-queued {len(requeued)} season(s): {requeued}"

    requeue()


pipeline_season_refresh()
