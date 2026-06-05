"""Iceberg table maintenance + metrics (run inside the jupyter-spark container).

For each Silver/Gold table:
  - rewrite_data_files  → compact small files (per-partition overwrites create many)
  - expire_snapshots    → drop old snapshots/metadata (keep the last 5)
  - log files/snapshots counts as a cheap table-format observability signal

Invoked by the `iceberg_maintenance` Airflow DAG:
  docker exec jupyter-spark python /home/jovyan/work/iceberg_maintenance.py
"""

from pyspark.sql import SparkSession

TABLES = [
    "teams",
    "players",
    "match_statistics",
    "player_match_stats",
    "match_events",
    "player_season_stats",
]

spark = SparkSession.builder.appName("IcebergMaintenance").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for table in TABLES:
    fq = f"lake.analytics.{table}"
    if not spark.catalog.tableExists(fq):
        print(f"[skip] {fq} does not exist")
        continue

    try:
        res = spark.sql(f"CALL lake.system.rewrite_data_files(table => '{fq}')").collect()
        print(f"[rewrite_data_files] {fq}: {res}")
    except Exception as exc:  # noqa: BLE001
        print(f"[rewrite_data_files] {fq} ERROR: {exc}")

    try:
        res = spark.sql(
            f"CALL lake.system.expire_snapshots(table => '{fq}', retain_last => 5)"
        ).collect()
        print(f"[expire_snapshots] {fq}: {res}")
    except Exception as exc:  # noqa: BLE001
        print(f"[expire_snapshots] {fq} ERROR: {exc}")

    try:
        files = spark.sql(f"SELECT count(*) c FROM {fq}.files").collect()[0]["c"]
        snaps = spark.sql(f"SELECT count(*) c FROM {fq}.snapshots").collect()[0]["c"]
        print(f"[metrics] {fq}: files={files} snapshots={snaps}")
    except Exception as exc:  # noqa: BLE001
        print(f"[metrics] {fq} ERROR: {exc}")

print("Iceberg maintenance complete.")
spark.stop()
