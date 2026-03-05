"""DAG: brasileirao_lakehouse_pipeline -- Full Medallion Architecture orchestration.

Orchestrates the complete data pipeline:
    1. Start the Spark container (Docker)
    2. Extract data from ESPN via soccerdata and upload to MinIO Bronze layer
    3. Run Spark Silver processing (Bronze -> Iceberg tables)
    4. Stop the Spark container to free RAM

DAG structure (4 tasks):
    start_spark >> extract_and_upload_bronze >> run_spark_silver >> stop_spark
"""

import logging
import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.operators.bash import BashOperator
from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

from lib.extraction_helpers import extract_and_upload_to_minio

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEAGUE_KEY = "BRA-Brasileirao"
SEASON = 2024
LOGGER = logging.getLogger(__name__)

# Docker container name for the Spark/Jupyter instance
SPARK_CONTAINER = "jupyter-spark"

# Path to the Silver processing notebook inside the container
NOTEBOOK_PATH = "/home/jovyan/work/spark_silver_processing.ipynb"

# MinIO connection settings (accessible from the Airflow host via port mapping)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

# Retry / timeout defaults
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=3),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs (single Spark container)
    default_args=DEFAULT_ARGS,
    tags=["lakehouse", "bronze", "silver", "spark", "brasileirao"],
    doc_md=__doc__,
)
def brasileirao_lakehouse_pipeline():
    """Full Medallion Architecture pipeline: Extract -> Bronze -> Silver."""

    # ------------------------------------------------------------------
    # Task 1: Start the Spark container
    # ------------------------------------------------------------------
    start_spark = BashOperator(
        task_id="start_spark",
        bash_command=(
            # Start the container (no-op if already running)
            f"docker start {SPARK_CONTAINER} && "
            # Wait for the Jupyter server to be ready (up to 30 seconds)
            "echo 'Waiting for Spark container...' && "
            "sleep 10 && "
            f"docker exec {SPARK_CONTAINER} python -c 'print(\"Container ready\")'"
        ),
        # Allow 2 minutes for container startup
        execution_timeout=timedelta(minutes=2),
    )

    # ------------------------------------------------------------------
    # Task 2: Extract from ESPN and upload to MinIO Bronze
    # ------------------------------------------------------------------
    @task()
    def extract_and_upload_bronze() -> dict[str, Any]:
        """Fetch ESPN data (schedule, matchsheet, lineup) and upload to MinIO.

        Runs on the Airflow worker (Python-only, no Spark required).
        Uses the helper function from lib/extraction_helpers.py.
        """
        LOGGER.info(
            "Starting extraction for league=%s season=%s", LEAGUE_KEY, SEASON
        )

        # Call the shared helper function
        result = extract_and_upload_to_minio(
            league_key=LEAGUE_KEY,
            season=SEASON,
            minio_endpoint=MINIO_ENDPOINT,
            minio_access_key=MINIO_ACCESS_KEY,
            minio_secret_key=MINIO_SECRET_KEY,
        )

        LOGGER.info(
            "Extraction complete: schedule=%s matchsheet=%s lineup=%s (%.2fs)",
            result["schedule_rows"],
            result["matchsheet_rows"],
            result["lineup_rows"],
            result["elapsed_seconds"],
        )

        return result

    # ------------------------------------------------------------------
    # Task 3: Run Spark Silver processing (nbconvert --execute)
    # ------------------------------------------------------------------
    run_spark_silver = BashOperator(
        task_id="run_spark_silver",
        bash_command=(
            # Convert notebook to .py script, then run with Python directly.
            # This avoids Jupyter kernel environment issues where pyspark
            # may not be found by the nbconvert kernel process.
            f"docker exec {SPARK_CONTAINER} bash -lc '"
            f"jupyter nbconvert --to script {NOTEBOOK_PATH} --stdout "
            "| python"
            "'"
        ),
        # Allow 1 hour for the full Spark processing
        execution_timeout=timedelta(hours=1),
    )

    # ------------------------------------------------------------------
    # Task 4: Stop the Spark container to free RAM
    # ------------------------------------------------------------------
    stop_spark = BashOperator(
        task_id="stop_spark",
        bash_command=f"docker stop {SPARK_CONTAINER}",
        # ALL_DONE ensures we stop Spark even if Silver processing fails
        trigger_rule=TriggerRule.ALL_DONE,
        # Short timeout for a simple docker stop
        execution_timeout=timedelta(minutes=2),
    )

    # ------------------------------------------------------------------
    # Wire up the DAG dependency graph
    # ------------------------------------------------------------------
    bronze_result = extract_and_upload_bronze()

    # start_spark -> extract_bronze -> run_silver -> stop_spark
    start_spark >> bronze_result >> run_spark_silver >> stop_spark


# Instantiate the DAG
brasileirao_lakehouse_pipeline()
