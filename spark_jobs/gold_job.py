"""Gold Spark job — Silver (Iceberg) → Gold (Iceberg).

Ported from spark_gold_processing.ipynb to run as an ephemeral per-run Spark
container. Reads the (season, league) partition of player_match_stats and
writes player_season_stats. SEASON/LEAGUE_KEY come from the environment.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SEASON = int(os.getenv("SEASON", "2024"))
LEAGUE_KEY = os.getenv("LEAGUE_KEY", "BRA-Brasileirao")

spark = (
    SparkSession.builder.appName("BrasileiraoGoldProcessing")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df_player_match = spark.read.table("lake.analytics.player_match_stats").filter(
    (F.col("season") == SEASON) & (F.col("league") == LEAGUE_KEY)
)
numeric_cols = [
    "total_goals", "goal_assists", "shots_on_target", "total_shots", "fouls_committed",
    "fouls_suffered", "yellow_cards", "red_cards", "own_goals", "offsides", "appearances",
    "saves", "shots_faced", "goals_conceded",
]
print(f"League={LEAGUE_KEY} season={SEASON} | silver rows={df_player_match.count()}")

agg_exprs = [F.sum(c).alias(c) for c in numeric_cols if c in df_player_match.columns]
id_exprs = (
    [F.first("athlete_id", ignorenulls=True).alias("athlete_id")]
    if "athlete_id" in df_player_match.columns else []
)
df_season_stats = (
    df_player_match.groupBy("player", "team", "league", "season")
    .agg(
        F.countDistinct("game").alias("matches_played"),
        F.sum(F.when(F.col("starter") == True, 1).otherwise(0)).alias("matches_started"),
        *id_exprs,
        *agg_exprs,
    )
    .withColumnRenamed("total_goals", "goals")
    .withColumnRenamed("goal_assists", "assists")
    .withColumn("goal_contributions", F.col("goals") + F.col("assists"))
    .withColumn("goals_per_match", F.round(F.col("goals") / F.col("matches_played"), 2))
    .withColumn("processed_at", F.current_timestamp())
)

# Quality gate: non-empty + non-null player/team
cnt = df_season_stats.count()
if cnt == 0:
    raise RuntimeError(f"Gold quality gate FAILED: player_season_stats empty for season={SEASON}")
for col_chk in ("player", "team"):
    nulls = df_season_stats.filter(F.col(col_chk).isNull() | (F.col(col_chk) == "")).count()
    if nulls > 0:
        raise RuntimeError(f"Gold quality gate FAILED: '{col_chk}' has {nulls} nulls")
print(f"Gold quality gates PASSED: {cnt} rows")

# Write — idempotent (season, league) partition overwrite with schema reconcile
spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.analytics")


def _align_df_to_table(df, fq):
    tbl_names = [f.name for f in spark.table(fq).schema.fields]
    for c in df.columns:
        if c not in tbl_names:
            spark.sql(f"ALTER TABLE {fq} ADD COLUMN `{c}` {df.schema[c].dataType.simpleString()}")
    df_cols = set(df.columns)
    exprs = [F.col(f.name) if f.name in df_cols else F.lit(None).cast(f.dataType).alias(f.name)
             for f in spark.table(fq).schema.fields]
    return df.select(*exprs)


fq = "lake.analytics.player_season_stats"
if spark.catalog.tableExists(fq):
    _align_df_to_table(df_season_stats, fq).writeTo(fq).overwritePartitions()
    print(f"  {fq}: overwrote (season={SEASON}, league={LEAGUE_KEY})")
else:
    df_season_stats.writeTo(fq).partitionedBy("season", "league").create()
    print(f"  {fq}: created")
print("Gold table written successfully.")
spark.stop()
