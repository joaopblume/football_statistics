"""Silver Spark job — Bronze (MinIO) → Silver (Iceberg).

Ported from spark_silver_processing.ipynb to run as an ephemeral per-run Spark
container (``docker run --rm football-spark spark-submit … silver_job.py``).
SEASON and LEAGUE_KEY come from the environment (injected by the Airflow DAG).

Tables: teams, players, match_statistics, player_match_stats, match_events.
The measured quality gate writes report.json to MinIO and fails fast; the
Airflow DAG records the measured results to pipeline_quality_checks.
"""

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

SEASON = int(os.getenv("SEASON", "2024"))
LEAGUE_KEY = os.getenv("LEAGUE_KEY", "BRA-Brasileirao")
RAW_BUCKET = "datalake-raw"

_LEAGUE_SLUGS = {
    "BRA-Brasileirao": "brasileirao",
    "ITA-Serie A": "serie_a",
    "ENG-Premier League": "premier_league",
    "FRA-Ligue 1": "ligue_1",
}
LEAGUE_SLUG = _LEAGUE_SLUGS.get(
    LEAGUE_KEY, LEAGUE_KEY.lower().replace(" ", "_").replace("-", "_").strip("_")
)
BRONZE_URI = f"s3a://{RAW_BUCKET}/espn/{LEAGUE_SLUG}/{SEASON}"

spark = (
    SparkSession.builder.appName("BrasileiraoSilverProcessing")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Read Bronze + normalize season (soccerdata encodes cross-year EU seasons
# differently; align to the pipeline SEASON so partitions/filters match).
# ---------------------------------------------------------------------------
df_schedule = spark.read.option("multiline", "true").json(f"{BRONZE_URI}/schedule.json")
df_matchsheet = spark.read.option("multiline", "true").json(f"{BRONZE_URI}/matchsheet.json")
df_lineup = spark.read.option("multiline", "true").json(f"{BRONZE_URI}/lineup.json")
df_schedule = df_schedule.withColumn("season", F.lit(SEASON))
df_matchsheet = df_matchsheet.withColumn("season", F.lit(SEASON))
df_lineup = df_lineup.withColumn("season", F.lit(SEASON))
print(f"League={LEAGUE_KEY} season={SEASON} | schedule={df_schedule.count()} "
      f"matchsheet={df_matchsheet.count()} lineup={df_lineup.count()}")

# ---------------------------------------------------------------------------
# DIMENSION: teams
# ---------------------------------------------------------------------------
df_home_teams = (
    df_matchsheet.filter(F.col("is_home") == True).select(
        F.col("team").alias("team_name"), F.col("league"), F.col("season"),
        F.col("venue"), F.col("attendance").cast("int"), F.col("capacity").cast("int"),
    )
)
venue_window = Window.partitionBy("team_name", "league", "season").orderBy(
    F.desc("venue_count"), "venue"
)
df_most_common_venue = (
    df_home_teams.groupBy("team_name", "league", "season", "venue")
    .agg(F.count("*").alias("venue_count"))
    .withColumn("rn", F.row_number().over(venue_window))
    .filter(F.col("rn") == 1)
    .select("team_name", "league", "season", F.col("venue").alias("home_venue"))
)
df_team_stats = df_home_teams.groupBy("team_name", "league", "season").agg(
    F.max("capacity").cast("int").alias("stadium_capacity"),
    F.round(F.avg("attendance"), 0).cast("int").alias("avg_attendance"),
    F.count("*").alias("home_matches"),
)
df_teams = df_team_stats.join(
    df_most_common_venue, on=["team_name", "league", "season"], how="left"
).withColumn("processed_at", F.current_timestamp())

# ---------------------------------------------------------------------------
# DIMENSION: players — keyed on a stable surrogate (athlete_id, name fallback)
# ---------------------------------------------------------------------------
if "athlete_id" in df_lineup.columns:
    df_lineup_keyed = df_lineup.withColumn(
        "_player_uid", F.coalesce(F.col("athlete_id").cast("string"), F.col("player"))
    )
else:
    df_lineup_keyed = df_lineup.withColumn("athlete_id", F.lit(None).cast("long")).withColumn(
        "_player_uid", F.col("player")
    )
player_window = Window.partitionBy("_player_uid", "team").orderBy(F.desc("game"))
df_players = (
    df_lineup_keyed.withColumn("rn", F.row_number().over(player_window))
    .filter(F.col("rn") == 1).drop("rn")
    .select("athlete_id", "player", "team", "position", "league", "season", "_player_uid")
)
df_player_match_count = df_lineup_keyed.groupBy("_player_uid", "team").agg(
    F.countDistinct("game").alias("matches_played")
)
df_players = (
    df_players.join(df_player_match_count, on=["_player_uid", "team"], how="left")
    .drop("_player_uid").withColumn("processed_at", F.current_timestamp())
)

# ---------------------------------------------------------------------------
# FACT: match_statistics
# ---------------------------------------------------------------------------
if "home_score" in set(df_schedule.columns) and "away_score" in set(df_schedule.columns):
    df_score_base = df_schedule.select(
        "game", F.col("home_score").cast("int").alias("home_score"),
        F.col("away_score").cast("int").alias("away_score"),
    )
    _schedule_for_join = df_schedule.drop("home_score", "away_score")
else:
    df_scores = (
        df_lineup.withColumn("total_goals", F.coalesce(F.col("total_goals").cast("int"), F.lit(0)))
        .groupBy("game", "is_home").agg(F.sum("total_goals").alias("team_goals"))
    )
    df_home_sc = df_scores.filter(F.col("is_home") == True).select(
        "game", F.col("team_goals").alias("home_score"))
    df_away_sc = df_scores.filter(F.col("is_home") == False).select(
        "game", F.col("team_goals").alias("away_score"))
    df_score_base = df_home_sc.join(df_away_sc, on="game", how="outer")
    _schedule_for_join = df_schedule

meta_cols = {"league", "season", "game", "team", "is_home", "venue", "attendance", "capacity"}
stat_cols = [c for c in df_matchsheet.columns if c not in meta_cols]
df_home_stats = df_matchsheet.filter(F.col("is_home") == True)
for c in stat_cols:
    df_home_stats = df_home_stats.withColumnRenamed(c, f"home_{c}")
df_home_stats = df_home_stats.select("game", "venue", "attendance", "capacity",
                                     *[f"home_{c}" for c in stat_cols])
df_away_stats = df_matchsheet.filter(F.col("is_home") == False)
for c in stat_cols:
    df_away_stats = df_away_stats.withColumnRenamed(c, f"away_{c}")
df_away_stats = df_away_stats.select("game", *[f"away_{c}" for c in stat_cols])

df_match_stats = (
    _schedule_for_join.join(df_score_base, on="game", how="left")
    .join(df_home_stats, on="game", how="left").join(df_away_stats, on="game", how="left")
    .withColumn("home_score", F.col("home_score").cast("int"))
    .withColumn("away_score", F.col("away_score").cast("int"))
    .withColumn("total_goals", F.col("home_score") + F.col("away_score"))
    .withColumn("goal_diff", F.abs(F.col("home_score") - F.col("away_score")))
    .withColumn("is_draw", F.col("home_score") == F.col("away_score"))
    .withColumn("winner", F.when(F.col("home_score") > F.col("away_score"), F.col("home_team"))
                .when(F.col("away_score") > F.col("home_score"), F.col("away_team"))
                .otherwise(F.lit("Draw")))
    .withColumn("processed_at", F.current_timestamp())
)

# ---------------------------------------------------------------------------
# FACT: player_match_stats
# ---------------------------------------------------------------------------
numeric_cols = [
    "total_goals", "goal_assists", "shots_on_target", "total_shots", "fouls_committed",
    "fouls_suffered", "yellow_cards", "red_cards", "own_goals", "offsides", "appearances",
    "saves", "shots_faced", "goals_conceded",
]
df_player_match = df_lineup
for c in numeric_cols:
    if c in df_player_match.columns:
        df_player_match = df_player_match.withColumn(c, F.coalesce(F.col(c).cast("int"), F.lit(0)))
if "starter" not in df_player_match.columns and "is_starter" in df_player_match.columns:
    df_player_match = df_player_match.withColumn("starter", F.col("is_starter") == True)
elif "starter" not in df_player_match.columns and "formation_place" in df_player_match.columns:
    df_player_match = df_player_match.withColumn("starter", F.col("formation_place").isNotNull())
elif "starter" not in df_player_match.columns:
    df_player_match = df_player_match.withColumn("starter", F.lit(True))
df_player_match = df_player_match.withColumn("processed_at", F.current_timestamp())

# ---------------------------------------------------------------------------
# FACT: match_events (always materialized, empty with stable schema if no data)
# ---------------------------------------------------------------------------
try:
    df_events_raw = spark.read.option("multiline", "true").json(f"{BRONZE_URI}/events.json")
    _events_available = df_events_raw.count() > 0
except Exception:
    _events_available = False
if _events_available:
    df_match_events = df_events_raw
    if "game_id" in df_match_events.columns:
        df_match_events = df_match_events.withColumn("game_id", F.col("game_id").cast("long"))
    if "season" not in df_match_events.columns:
        df_match_events = df_match_events.withColumn("season", F.lit(SEASON).cast("int"))
    if "league" not in df_match_events.columns:
        df_match_events = df_match_events.withColumn("league", F.lit(LEAGUE_KEY))
    df_match_events = df_match_events.withColumn("processed_at", F.current_timestamp())
else:
    df_match_events = spark.createDataFrame([], StructType([
        StructField("game", StringType(), True), StructField("game_id", LongType(), True),
        StructField("season", IntegerType(), True), StructField("league", StringType(), True),
        StructField("processed_at", TimestampType(), True),
    ]))

# ---------------------------------------------------------------------------
# QUALITY GATE — measured checks → report.json (MinIO); fail fast on hard breach.
# (Great Expectations validation runs as a separate Airflow task — Phase 3.)
# ---------------------------------------------------------------------------
_NULL_WARN, _NULL_FAIL = 0.01, 0.05


def _null_rate(df, col):
    total = df.count()
    if total == 0:
        return 1.0
    return df.filter(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")).count() / total


def _empty_check(name, df):
    n = df.count()
    return {"check_name": name, "status": "pass" if n > 0 else "fail", "details": f"row_count={n}"}


def _null_check(name, df, col):
    r = _null_rate(df, col)
    status = "pass" if r <= _NULL_WARN else ("warn" if r <= _NULL_FAIL else "fail")
    return {"check_name": name, "status": status,
            "details": f"{col}_null_rate={r:.4f} (warn>{_NULL_WARN}, fail>{_NULL_FAIL})"}


checks = [
    _empty_check("teams_not_empty", df_teams),
    _empty_check("players_not_empty", df_players),
    _empty_check("match_statistics_not_empty", df_match_stats),
    _empty_check("player_match_stats_not_empty", df_player_match),
    _null_check("team_name_null_rate_ok", df_teams, "team_name"),
    _null_check("player_name_null_rate_ok", df_players, "player"),
    _null_check("player_team_null_rate_ok", df_players, "team"),
    _null_check("match_game_null_rate_ok", df_match_stats, "game"),
    _null_check("player_match_game_null_rate_ok", df_player_match, "game"),
]
_ms_total = df_match_stats.count()
_home_nonnull = df_match_stats.filter(F.col("home_score").isNotNull()).count()
_hs = "fail" if (_ms_total == 0 or _home_nonnull == 0) else (
    "warn" if _home_nonnull < _ms_total * 0.5 else "pass")
checks.append({"check_name": "home_score_not_all_null", "status": _hs,
               "details": f"home_score_nonnull={_home_nonnull}/{_ms_total}"})
if "athlete_id" in df_player_match.columns:
    _pm = df_player_match.count()
    _aid = df_player_match.filter(F.col("athlete_id").isNotNull()).count()
    _cov = (_aid / _pm) if _pm else 0.0
    checks.append({"check_name": "athlete_id_coverage", "status": "pass" if _cov >= 0.8 else "warn",
                   "details": f"coverage={_cov:.3f}"})

# Great Expectations — the declarative DQ engine (system of record). Its results
# merge into the same report → pipeline_quality_checks, and a GE 'fail' aborts.
from ge_suites import run_ge_validation  # noqa: E402

checks.extend(run_ge_validation(spark, {
    "teams": df_teams, "players": df_players,
    "match_statistics": df_match_stats, "player_match_stats": df_player_match,
}))

for _c in checks:
    print(f"  [{_c['status'].upper():4}] {_c['check_name']:40} {_c['details']}")

_report = {"league": LEAGUE_KEY, "season": SEASON, "stage": "silver", "checks": checks}
_uri = f"s3a://datalake-warehouse/quality/silver/{LEAGUE_SLUG}/{SEASON}/report.json"
_hconf = spark._jsc.hadoopConfiguration()
_HPath = spark._jvm.org.apache.hadoop.fs.Path
_out = _HPath(_uri).getFileSystem(_hconf).create(_HPath(_uri), True)
_out.write(bytearray(json.dumps(_report), "utf-8"))
_out.close()
_failed = [c["check_name"] for c in checks if c["status"] == "fail"]
if _failed:
    raise RuntimeError(f"Silver quality gate FAILED: {_failed} (report at {_uri})")

# ---------------------------------------------------------------------------
# WRITE — idempotent (season, league) partition overwrite with schema reconcile
# ---------------------------------------------------------------------------
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


def _write_partitioned(df, table):
    fq = f"lake.analytics.{table}"
    if spark.catalog.tableExists(fq):
        _align_df_to_table(df, fq).writeTo(fq).overwritePartitions()
        print(f"  {fq}: overwrote (season={SEASON}, league={LEAGUE_KEY})")
    else:
        df.writeTo(fq).partitionedBy("season", "league").create()
        print(f"  {fq}: created")


_write_partitioned(df_teams, "teams")
_write_partitioned(df_players, "players")
_write_partitioned(df_match_stats, "match_statistics")
_write_partitioned(df_player_match, "player_match_stats")
_write_partitioned(df_match_events, "match_events")
print("All Silver Iceberg tables written successfully.")
spark.stop()
