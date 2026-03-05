"""DAG: get_brasileirao -- Extract football data from ESPN via soccerdata.

Extracts match schedules, lineups (with ESPN athlete IDs), and player
profiles for the Brasileirao season.  Results are saved as raw files
and enqueued into ``pending.jsonl`` for downstream ingestion.

DAG structure (4 tasks):
    fetch_schedule >> extract_matches >> enrich_player_profiles >> enqueue_results
"""

import json
import logging
import os
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
import soccerdata as sd
from airflow.sdk import dag, task

from lib.extraction_helpers import (
    build_queue_message,
    ensure_brasileirao_mapping,
    fetch_player_profile,
    parse_lineup_with_ids,
    slug,
    write_csv,
    write_json,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEAGUE_KEY = "BRA-Brasileirao"
# ESPN internal league key used for athlete API calls
ESPN_LEAGUE_KEY = "bra.1"
SEASON = 2026
LOGGER = logging.getLogger(__name__)

# Retry / timeout defaults applied to every task in this DAG
DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=6),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs corrupting the queue file
    default_args=DEFAULT_ARGS,
    tags=["soccerdata", "raw", "queue", "brasileirao"],
)
def get_brasileirao():
    """Extract Brasileirao data from ESPN and enqueue for ingestion."""

    # ------------------------------------------------------------------
    # Task 1: Fetch the full season schedule
    # ------------------------------------------------------------------
    @task()
    def fetch_schedule() -> dict[str, Any]:
        """Fetch the full season schedule from ESPN via soccerdata.

        Returns a dict containing the schedule rows (as list of dicts)
        and run metadata (output root, timestamps, etc.).
        """
        started = time.perf_counter()
        LOGGER.info("Starting fetch_schedule for league=%s season=%s", LEAGUE_KEY, SEASON)

        # Register the Brasileirao league in soccerdata's config
        ensure_brasileirao_mapping(LEAGUE_KEY)
        LOGGER.info("League mapping ensured at runtime")

        # Create the ESPN reader instance
        reader = sd.ESPN(leagues=LEAGUE_KEY, seasons=SEASON)
        LOGGER.info("ESPN reader initialized")

        # Read the full season schedule
        t0 = time.perf_counter()
        schedule = reader.read_schedule().reset_index()
        LOGGER.info("read_schedule completed in %.2fs (%s rows)", time.perf_counter() - t0, len(schedule))

        # Set up output directory structure
        output_root = Path(os.getenv("OUTPUT_DIR", "output")).resolve()
        max_matches = int(os.getenv("MAX_MATCHES", "0"))
        run_ts = pendulum.now("UTC").format("YYYYMMDDTHHmmss")
        run_root = output_root / "raw" / "soccerdata" / slug(LEAGUE_KEY) / str(SEASON) / run_ts

        # Optionally limit the number of matches for testing
        if max_matches > 0:
            schedule = schedule.head(max_matches)
            LOGGER.info("Schedule truncated to %s matches (MAX_MATCHES)", len(schedule))

        # Save the full schedule CSV
        write_csv(run_root / "schedule.csv", schedule)
        LOGGER.info("Schedule saved to CSV")

        elapsed = time.perf_counter() - started
        LOGGER.info("fetch_schedule completed in %.2fs", elapsed)

        return {
            "schedule": schedule.to_dict(orient="records"),
            "output_root": str(output_root),
            "run_root": str(run_root),
            "run_ts": run_ts,
        }

    # ------------------------------------------------------------------
    # Task 2: Extract matches and lineups (with athlete IDs)
    # ------------------------------------------------------------------
    @task()
    def extract_matches(schedule_data: dict[str, Any]) -> dict[str, Any]:
        """For each match: read raw ESPN summary JSON and parse lineups WITH athlete IDs.

        Instead of using soccerdata's read_lineup() (which discards athlete IDs),
        we read the cached summary JSON directly and parse the roster ourselves.

        Returns a dict with queue messages, collected lineups, and unique athlete IDs.
        """
        started = time.perf_counter()
        schedule = pd.DataFrame(schedule_data["schedule"])
        run_root = Path(schedule_data["run_root"])
        log_every = int(os.getenv("LOG_EVERY", "10"))

        LOGGER.info("Starting extract_matches for %s matches", len(schedule))

        # Re-initialize reader so we can use its .get() method for cached data
        ensure_brasileirao_mapping(LEAGUE_KEY)
        reader = sd.ESPN(leagues=LEAGUE_KEY, seasons=SEASON)

        # ESPN API URL and file cache patterns (from soccerdata internals)
        url_mask = "http://site.api.espn.com/apis/site/v2/sports/soccer/{}/{}"
        file_mask = "Summary_{}.json"

        queue_messages: list[dict[str, Any]] = []
        collected_lineups: list[pd.DataFrame] = []
        all_athlete_ids: set[int] = set()
        total_matches = len(schedule)
        lineup_success = 0
        lineup_fail = 0

        for idx, row in enumerate(schedule.itertuples(index=False), start=1):
            loop_started = time.perf_counter()
            game_id = int(getattr(row, "game_id"))
            game_slug_str = slug(getattr(row, "game"))
            match_dir = run_root / "matches" / f"{game_id}_{game_slug_str}"

            if idx == 1 or idx % max(1, log_every) == 0:
                LOGGER.info("Processing match %s/%s game_id=%s", idx, total_matches, game_id)

            # Save the schedule row as JSON (match entity)
            schedule_row = {k: getattr(row, k) for k in schedule.columns}
            schedule_json = match_dir / "schedule_row.json"
            write_json(schedule_json, schedule_row)

            # Enqueue the match entity
            queue_messages.append(
                build_queue_message(
                    entity_type="match",
                    entity_id=game_id,
                    payload_format="json",
                    path=str(schedule_json),
                    provider="espn",
                    league=LEAGUE_KEY,
                    season=SEASON,
                )
            )

            # Read the raw ESPN summary JSON (uses soccerdata's cache)
            try:
                t_summary = time.perf_counter()
                league_id = schedule_row.get("league_id", ESPN_LEAGUE_KEY)
                url = url_mask.format(league_id, f"summary?event={game_id}")
                filepath = reader.data_dir / file_mask.format(game_id)
                summary_reader = reader.get(url, filepath)
                data = json.load(summary_reader)
                LOGGER.info(
                    "Summary loaded for game_id=%s in %.2fs",
                    game_id,
                    time.perf_counter() - t_summary,
                )
            except Exception as exc:  # noqa: BLE001
                lineup_fail += 1
                LOGGER.exception("Failed to load summary for game_id=%s: %s", game_id, exc)
                continue

            # Parse rosters from both teams
            game_info = {
                "game": getattr(row, "game"),
                "game_id": game_id,
                "league": LEAGUE_KEY,
                "season": SEASON,
            }

            # Iterate over both teams (index 0 = home, index 1 = away)
            match_lineups: list[pd.DataFrame] = []
            for team_idx in range(2):
                # Check if roster data exists for this team
                if "roster" not in data.get("rosters", [{}])[team_idx]:
                    LOGGER.info(
                        "No roster data for team %d in game_id=%s", team_idx + 1, game_id
                    )
                    continue

                # Get team name from boxscore form data
                team_name = data["boxscore"]["form"][team_idx]["team"]["displayName"]
                roster_data = data["rosters"][team_idx]["roster"]

                # Parse the lineup using our custom parser (captures athlete IDs)
                lineup_df = parse_lineup_with_ids(
                    roster_data=roster_data,
                    game_info=game_info,
                    team_display_name=team_name,
                    is_home=(team_idx == 0),
                )

                if not lineup_df.empty:
                    match_lineups.append(lineup_df)
                    # Collect unique athlete IDs for profile enrichment
                    valid_ids = lineup_df["athlete_id"].dropna().astype(int)
                    all_athlete_ids.update(valid_ids.tolist())

            if match_lineups:
                lineup_success += 1
                # Concatenate both teams' lineups for this match
                full_lineup = pd.concat(match_lineups, ignore_index=True)
                collected_lineups.append(full_lineup)

                # Save lineup CSV
                lineup_csv = match_dir / "lineup.csv"
                write_csv(lineup_csv, full_lineup)

                # Enqueue the match_lineup entity
                queue_messages.append(
                    build_queue_message(
                        entity_type="match_lineup",
                        entity_id=game_id,
                        payload_format="csv",
                        path=str(lineup_csv),
                        provider="espn",
                        league=LEAGUE_KEY,
                        season=SEASON,
                    )
                )
            else:
                lineup_fail += 1

            if idx == 1 or idx % max(1, log_every) == 0:
                LOGGER.info(
                    "Match game_id=%s done in %.2fs (success=%s fail=%s athletes=%s)",
                    game_id,
                    time.perf_counter() - loop_started,
                    lineup_success,
                    lineup_fail,
                    len(all_athlete_ids),
                )

        LOGGER.info(
            "Match loop finished. total=%s success=%s fail=%s unique_athletes=%s",
            total_matches,
            lineup_success,
            lineup_fail,
            len(all_athlete_ids),
        )

        # Aggregate lineups by team and player (preserving backward compat)
        if collected_lineups:
            all_lineups = pd.concat(collected_lineups, ignore_index=True)
        else:
            all_lineups = pd.DataFrame()

        # Team aggregations
        if not all_lineups.empty:
            for team_name, team_df in all_lineups.groupby("team", dropna=True):
                team_path = run_root / "teams" / f"{slug(team_name)}.csv"
                write_csv(team_path, team_df)
                queue_messages.append(
                    build_queue_message(
                        entity_type="team",
                        entity_id=str(team_name),
                        payload_format="csv",
                        path=str(team_path),
                        provider="espn",
                        league=LEAGUE_KEY,
                        season=SEASON,
                    )
                )

        # Player aggregations (grouped by player name -- legacy, kept for compat)
        if not all_lineups.empty:
            for player_name, player_df in all_lineups.groupby("player", dropna=True):
                player_path = run_root / "players" / f"{slug(player_name)}.csv"
                write_csv(player_path, player_df)
                queue_messages.append(
                    build_queue_message(
                        entity_type="player",
                        entity_id=str(player_name),
                        payload_format="csv",
                        path=str(player_path),
                        provider="espn",
                        league=LEAGUE_KEY,
                        season=SEASON,
                    )
                )

        elapsed = time.perf_counter() - started
        LOGGER.info("extract_matches completed in %.2fs", elapsed)

        return {
            "queue_messages": queue_messages,
            "athlete_ids": list(all_athlete_ids),
            "run_root": str(run_root),
            "output_root": schedule_data["output_root"],
        }

    # ------------------------------------------------------------------
    # Task 3: Enrich unique players with full ESPN profile data
    # ------------------------------------------------------------------
    @task()
    def enrich_player_profiles(extraction_result: dict[str, Any]) -> list[dict[str, Any]]:
        """For each unique athlete ID: fetch the full ESPN player profile.

        Calls the ESPN common API to get biographical data (name, DOB,
        position, team, jersey, etc.) and saves profile JSONs to disk.

        Returns a list of queue messages for the player_profile entities.
        """
        started = time.perf_counter()
        athlete_ids = extraction_result["athlete_ids"]
        run_root = Path(extraction_result["run_root"])
        api_delay = float(os.getenv("ESPN_API_DELAY", "0.5"))

        LOGGER.info(
            "Starting enrich_player_profiles for %s unique athletes (delay=%.2fs)",
            len(athlete_ids),
            api_delay,
        )

        profile_messages: list[dict[str, Any]] = []
        success_count = 0
        fail_count = 0
        log_every = int(os.getenv("LOG_EVERY", "10"))

        for idx, athlete_id in enumerate(athlete_ids, start=1):
            # Fetch the profile from ESPN's athlete API
            profile = fetch_player_profile(
                athlete_id=athlete_id,
                league_espn_key=ESPN_LEAGUE_KEY,
                delay=api_delay,
            )

            if profile is None:
                fail_count += 1
                LOGGER.warning("Profile fetch failed for athlete_id=%s", athlete_id)
                continue

            success_count += 1

            # Save the profile JSON (without the _raw key in the file)
            profile_path = run_root / "player_profiles" / f"{athlete_id}.json"
            # Keep _raw in a separate variable, save clean profile
            raw_data = profile.pop("_raw", None)
            write_json(profile_path, profile)

            # Enqueue the player_profile entity
            profile_messages.append(
                build_queue_message(
                    entity_type="player_profile",
                    entity_id=athlete_id,
                    payload_format="json",
                    path=str(profile_path),
                    provider="espn",
                    league=LEAGUE_KEY,
                    season=SEASON,
                )
            )

            if idx == 1 or idx % max(1, log_every) == 0:
                LOGGER.info(
                    "Profile progress: %s/%s (success=%s fail=%s)",
                    idx,
                    len(athlete_ids),
                    success_count,
                    fail_count,
                )

        elapsed = time.perf_counter() - started
        LOGGER.info(
            "enrich_player_profiles completed in %.2fs (success=%s fail=%s)",
            elapsed,
            success_count,
            fail_count,
        )

        return profile_messages

    # ------------------------------------------------------------------
    # Task 4: Merge all messages and write to the pending queue
    # ------------------------------------------------------------------
    @task()
    def enqueue_results(
        extraction_result: dict[str, Any],
        profile_messages: list[dict[str, Any]],
    ) -> str:
        """Merge match/lineup/team/player messages with profile messages and enqueue.

        Writes all messages to ``output/queue/pending.jsonl`` for the
        ingestion DAG to consume.
        """
        started = time.perf_counter()
        output_root = Path(extraction_result["output_root"])
        queue_file = output_root / "queue" / "pending.jsonl"
        queue_file.parent.mkdir(parents=True, exist_ok=True)

        # Combine extraction messages with profile messages
        all_messages = extraction_result["queue_messages"] + profile_messages
        LOGGER.info("Enqueuing %s total messages to %s", len(all_messages), queue_file)

        # Append all messages to the queue file
        with queue_file.open("a", encoding="utf-8") as f:
            for msg in all_messages:
                f.write(json.dumps(msg, ensure_ascii=False) + "\n")

        elapsed = time.perf_counter() - started
        LOGGER.info("enqueue_results completed in %.2fs", elapsed)

        return (
            f"Enqueued {len(all_messages)} messages to {queue_file}. "
            f"(extraction={len(extraction_result['queue_messages'])}, "
            f"profiles={len(profile_messages)})"
        )

    # ------------------------------------------------------------------
    # Wire up the DAG dependency graph
    # ------------------------------------------------------------------

    # Task 1 -> Task 2 -> Task 3 -> Task 4
    schedule_data = fetch_schedule()
    extraction_result = extract_matches(schedule_data)
    profile_messages = enrich_player_profiles(extraction_result)
    enqueue_results(extraction_result, profile_messages)


# Instantiate the DAG
get_brasileirao()
