"""
Drop all Silver and Gold Iceberg tables so they can be recreated
with season partitioning on the next pipeline run.

Run inside the Spark container:
    docker exec jupyter-spark python /home/jovyan/work/drop_silver_gold_tables.py
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DropSilverGoldTables")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

TABLES = [
    "lake.analytics.teams",
    "lake.analytics.players",
    "lake.analytics.match_statistics",
    "lake.analytics.player_match_stats",
    "lake.analytics.player_season_stats",
]

for table in TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

print("\nDone. All tables will be recreated (partitioned by season) on the next pipeline run.")
spark.stop()
