"""Shared helpers for football data extraction (soccerdata / ESPN)."""

import json
import logging
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
import requests

# Module-level logger
LOGGER = logging.getLogger(__name__)

# Base URL for the ESPN public API (athletes endpoint)
ESPN_ATHLETE_API = (
    "http://site.api.espn.com/apis/common/v3/sports/soccer/{league}/athletes/{athlete_id}"
)

# Default delay between ESPN API calls to avoid rate limiting (seconds)
DEFAULT_API_DELAY = 0.5


# ---------------------------------------------------------------------------
# General-purpose helpers (moved from DAG files)
# ---------------------------------------------------------------------------

def ensure_brasileirao_mapping(league_key: str = "BRA-Brasileirao") -> None:
    """Register the Brasileirao league in soccerdata's runtime config if absent.

    This is needed because soccerdata does not ship with a built-in
    mapping for the Brasileirao.  We inject it at runtime before
    creating the ESPN reader.
    """
    from soccerdata import _config  # late import to avoid module-level side-effects

    # Only add the mapping if it does not already exist
    if league_key not in _config.LEAGUE_DICT:
        _config.LEAGUE_DICT[league_key] = {
            "ESPN": "bra.1",
            "season_start": "Apr",
            "season_end": "Dec",
        }


def slug(value: str) -> str:
    """Convert an arbitrary string into a filesystem-safe slug.

    Replaces non-alphanumeric characters with underscores, strips leading/
    trailing underscores, and lowercases everything.  Returns ``"unknown"``
    for empty results.
    """
    # Replace any non-alphanumeric character with underscore
    result = re.sub(r"[^a-zA-Z0-9]+", "_", str(value)).strip("_").lower()
    # Fallback if the result is empty
    return result or "unknown"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Serialize *payload* as formatted JSON to *path*, creating parent dirs."""
    # Ensure parent directories exist
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write the JSON with readable indentation
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str, indent=2)


def write_csv(path: Path, df: pd.DataFrame) -> None:
    """Write a DataFrame to CSV at *path*, creating parent dirs."""
    # Ensure parent directories exist
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write CSV without the pandas index column
    df.to_csv(path, index=False)


def _to_json_str(df: pd.DataFrame) -> str:
    """Serialize a DataFrame to a compact JSON string (records orient, ISO dates)."""
    if df.empty:
        return "[]"
    return json.dumps(
        json.loads(df.to_json(orient="records", date_format="iso")),
        indent=2, ensure_ascii=False,
    )


def _make_s3_client(endpoint: str, access_key: str, secret_key: str):
    """Create a boto3 S3 client configured for MinIO."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def build_queue_message(
    entity_type: str,
    entity_id: str | int,
    payload_format: str,
    path: str,
    provider: str,
    league: str,
    season: int,
) -> dict[str, Any]:
    """Build a standardized queue message dict for pending.jsonl.

    All queue messages share the same shape so downstream ingestion
    can process them uniformly.
    """
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "payload_format": payload_format,
        "path": str(path),
        "provider": provider,
        "league": league,
        "season": season,
        "created_at": pendulum.now("UTC").to_iso8601_string(),
    }


# ---------------------------------------------------------------------------
# Lineup parsing with athlete IDs
# ---------------------------------------------------------------------------

def parse_lineup_with_ids(
    roster_data: list[dict[str, Any]],
    game_info: dict[str, Any],
    team_display_name: str,
    is_home: bool,
) -> pd.DataFrame:
    """Parse a raw ESPN roster array into a DataFrame that includes athlete IDs.

    Unlike ``soccerdata``'s ``read_lineup()``, this function preserves the
    ESPN ``athlete.id`` field, which is essential for stable player identity.

    Parameters
    ----------
    roster_data : list[dict]
        The ``data["rosters"][i]["roster"]`` array from the ESPN summary JSON.
    game_info : dict
        Dict with keys ``game``, ``league``, ``season``, ``game_id``.
    team_display_name : str
        Display name for the team (from ``boxscore.form[i].team.displayName``).
    is_home : bool
        Whether this roster corresponds to the home team.

    Returns
    -------
    pd.DataFrame
        Lineup DataFrame with columns including ``athlete_id``.
    """
    rows: list[dict[str, Any]] = []

    for p in roster_data:
        # Extract the athlete object from the roster entry
        athlete = p.get("athlete", {})

        # Build base row with identity and match context
        row: dict[str, Any] = {
            "game": game_info.get("game"),
            "game_id": game_info.get("game_id"),
            "league": game_info.get("league"),
            "season": game_info.get("season"),
            "team": team_display_name,
            "is_home": is_home,
            # ESPN athlete ID -- the critical field we were missing
            "athlete_id": int(athlete.get("id", 0)) if athlete.get("id") else None,
            "player": athlete.get("displayName", "Unknown"),
            "position": p["position"]["name"] if "position" in p else None,
            "formation_place": p.get("formationPlace"),
            "starter": p.get("starter", False),
        }

        # Parse substitution data (same logic as soccerdata's read_lineup)
        _parse_substitution_data(p, row)

        # Parse per-match stats if available (e.g. minutes, tackles, passes)
        if "stats" in p:
            for stat in p["stats"]:
                row[stat["name"]] = stat["value"]

        rows.append(row)

    # Return an empty DataFrame with expected columns if no roster entries
    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def _parse_substitution_data(player_entry: dict[str, Any], row: dict[str, Any]) -> None:
    """Parse substitution in/out timing from an ESPN roster player entry.

    Mutates *row* in place, adding ``sub_in`` and ``sub_out`` keys.
    This replicates the logic from ``soccerdata``'s ``espn.py`` lines 270-322.
    """
    # Determine whether the player was subbed in or out
    subbed_in_raw = player_entry.get("subbedIn", False)
    subbed_out_raw = player_entry.get("subbedOut", False)

    # subbedIn/subbedOut can be a bool or an object with a didSub flag
    subbed_in = (
        subbed_in_raw
        if isinstance(subbed_in_raw, bool)
        else subbed_in_raw.get("didSub", False)
    )
    subbed_out = (
        subbed_out_raw
        if isinstance(subbed_out_raw, bool)
        else subbed_out_raw.get("didSub", False)
    )

    # Collect substitution event objects for clock parsing
    subbed_events: list[dict] = []
    if isinstance(subbed_in_raw, bool) and (subbed_in or subbed_out):
        # When subbedIn is a bool, sub events live in the plays array
        subbed_events = [
            e for e in player_entry.get("plays", [])
            if e.get("substitution")
        ]
    else:
        # When subbedIn/Out are objects, they ARE the sub events
        if subbed_in:
            subbed_events.append(subbed_in_raw)
        if subbed_out:
            subbed_events.append(subbed_out_raw)

    # Determine sub_in value
    if player_entry.get("starter"):
        row["sub_in"] = "start"
    elif subbed_in and subbed_events:
        # Parse the minute from the clock display (e.g. "65'" -> 65)
        row["sub_in"] = _parse_clock_minute(subbed_events[0])
    else:
        row["sub_in"] = None

    # Determine sub_out value
    if (player_entry.get("starter") or subbed_in) and not subbed_out:
        row["sub_out"] = "end"
    elif subbed_out and subbed_events:
        # If player was also subbed in, the out event is the second one
        event_index = 0 if not subbed_in else 1
        if event_index < len(subbed_events):
            row["sub_out"] = _parse_clock_minute(subbed_events[event_index])
        else:
            row["sub_out"] = None
    else:
        row["sub_out"] = None


def _parse_clock_minute(event: dict[str, Any]) -> int | None:
    """Extract the match minute from a substitution event's clock display.

    The ESPN API returns clock values like ``"65'"`` or ``"90+3'"``.
    This function sums all numeric groups (e.g. 90+3 = 93).
    """
    try:
        clock_display = event.get("clock", {}).get("displayValue", "")
        # Find all digit groups and sum them (handles "90+3" -> 93)
        digits = re.findall(r"(\d{1,3})", clock_display)
        return sum(int(d) for d in digits) if digits else None
    except (AttributeError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# ESPN athlete profile fetching
# ---------------------------------------------------------------------------

def fetch_player_profile(
    athlete_id: int,
    league_espn_key: str,
    delay: float = DEFAULT_API_DELAY,
) -> dict[str, Any] | None:
    """Fetch a player profile from the ESPN athlete API.

    Parameters
    ----------
    athlete_id : int
        ESPN athlete numeric ID.
    league_espn_key : str
        ESPN league key, e.g. ``"bra.1"``.
    delay : float
        Seconds to sleep after the request (rate limiting).

    Returns
    -------
    dict or None
        The parsed profile dict, or None if the request failed.
    """
    url = ESPN_ATHLETE_API.format(league=league_espn_key, athlete_id=athlete_id)
    LOGGER.info("Fetching player profile: athlete_id=%s url=%s", athlete_id, url)

    try:
        # Make the HTTP GET request to ESPN's public API
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        raw = response.json()

        # Extract the nested athlete object
        athlete = raw.get("athlete", {})

        # Build a flattened profile dict with the fields we care about
        profile: dict[str, Any] = {
            "espn_athlete_id": int(athlete.get("id", athlete_id)),
            "espn_guid": athlete.get("guid"),
            "first_name": athlete.get("firstName"),
            "last_name": athlete.get("lastName"),
            "display_name": athlete.get("displayName"),
            "full_name": athlete.get("fullName"),
            "jersey": athlete.get("jersey"),
            "position_id": athlete.get("position", {}).get("id"),
            "position_name": athlete.get("position", {}).get("name"),
            "team_id": athlete.get("team", {}).get("id"),
            "team_name": athlete.get("team", {}).get("displayName"),
            "date_of_birth": athlete.get("displayDOB"),
            "age": athlete.get("age"),
            "gender": athlete.get("gender"),
            "is_active": athlete.get("active"),
            "profile_url": _extract_profile_url(athlete),
            # Keep the full raw response for future use
            "_raw": raw,
        }

        LOGGER.info(
            "Profile fetched: athlete_id=%s name=%s",
            athlete_id,
            profile.get("display_name"),
        )
        return profile

    except requests.RequestException as exc:
        LOGGER.warning(
            "Failed to fetch profile for athlete_id=%s: %s", athlete_id, exc
        )
        return None
    finally:
        # Rate-limit: sleep after each call regardless of success/failure
        if delay > 0:
            time.sleep(delay)


def _extract_profile_url(athlete: dict[str, Any]) -> str | None:
    """Extract the ESPN player card URL from the athlete links array."""
    for link in athlete.get("links", []):
        # Look for the playercard / overview link
        rels = link.get("rel", [])
        if "playercard" in rels or "overview" in rels:
            return link.get("href")
    return None


# ---------------------------------------------------------------------------
# ESPN summary JSON cache constants (mirrors brasileirao_teams_to_pg.py)
# ---------------------------------------------------------------------------

# URL template for ESPN match summary API
_ESPN_SUMMARY_URL_MASK = "http://site.api.espn.com/apis/site/v2/sports/soccer/{}/{}"
# Filename template used by soccerdata when caching summary JSONs locally
_ESPN_SUMMARY_FILE_MASK = "Summary_{}.json"


# ---------------------------------------------------------------------------
# Lineup enrichment: merge soccerdata stats with ESPN athlete IDs
# ---------------------------------------------------------------------------

def _build_enriched_lineup(
    schedule: "pd.DataFrame",
    sd_lineup: "pd.DataFrame",
    reader,
    league_key: str,
    season: int,
) -> "pd.DataFrame":
    """Merge soccerdata lineup stats with ESPN athlete IDs from cached summary JSONs.

    ``soccerdata``'s ``read_lineup()`` returns reliable numeric stats (goals,
    assists, cards) but discards ESPN's ``athlete_id`` and ``game_id``.

    This function reads the ESPN summary JSONs that were already cached to disk
    by the preceding ``read_lineup()`` call, extracts ``athlete_id`` via
    ``parse_lineup_with_ids()``, and left-joins the result onto the soccerdata
    lineup using ``(game, player, team)`` as the merge key.

    Rows from the soccerdata lineup that have no matching summary JSON are kept
    with ``athlete_id = None`` and ``game_id = None`` (graceful fallback).

    Parameters
    ----------
    schedule : pd.DataFrame
        Schedule DataFrame (output of ``reader.read_schedule().reset_index()``).
        Must contain ``game`` and ``game_id`` columns.
    sd_lineup : pd.DataFrame
        Soccerdata lineup (output of ``reader.read_lineup().reset_index()``).
    reader : sd.ESPN
        The soccerdata ESPN reader instance (used to access ``data_dir``
        and the ``get()`` method for cached files).
    league_key : str
        Soccerdata league key, e.g. ``"BRA-Brasileirao"``.
    season : int
        Season year, e.g. ``2024``.

    Returns
    -------
    pd.DataFrame
        Enriched lineup with ``athlete_id`` and ``game_id`` columns added.
    """
    from soccerdata import _config  # late import to avoid module-level side-effects

    if "game_id" not in schedule.columns:
        LOGGER.warning(
            "_build_enriched_lineup: schedule has no 'game_id' column — "
            "returning soccerdata lineup without athlete_id"
        )
        return sd_lineup

    # ESPN internal league key (e.g. "bra.1") needed for the summary URL
    espn_league_key = _config.LEAGUE_DICT.get(league_key, {}).get("ESPN", "bra.1")

    custom_parts: list["pd.DataFrame"] = []
    success = 0
    fail = 0

    for _, sched_row in schedule.iterrows():
        game_id = int(sched_row["game_id"])
        game_name = sched_row["game"]

        # Load the summary JSON from soccerdata's local disk cache
        try:
            filepath = reader.data_dir / _ESPN_SUMMARY_FILE_MASK.format(game_id)
            url = _ESPN_SUMMARY_URL_MASK.format(
                espn_league_key, f"summary?event={game_id}"
            )
            summary_reader = reader.get(url, filepath)
            data = json.load(summary_reader)
        except Exception as exc:  # noqa: BLE001
            fail += 1
            LOGGER.warning(
                "_build_enriched_lineup: failed to load summary for game_id=%s: %s",
                game_id, exc,
            )
            continue

        game_info = {
            "game": game_name,
            "game_id": game_id,
            "league": league_key,
            "season": season,
        }

        rosters = data.get("rosters", [])
        for team_idx in range(2):
            if team_idx >= len(rosters) or "roster" not in rosters[team_idx]:
                continue
            try:
                team_name = (
                    data["boxscore"]["form"][team_idx]["team"]["displayName"]
                )
            except (KeyError, IndexError):
                continue

            lineup_df = parse_lineup_with_ids(
                roster_data=rosters[team_idx]["roster"],
                game_info=game_info,
                team_display_name=team_name,
                is_home=(team_idx == 0),
            )
            if not lineup_df.empty:
                custom_parts.append(lineup_df)

        success += 1

    LOGGER.info(
        "_build_enriched_lineup: summaries loaded success=%s fail=%s",
        success, fail,
    )

    if not custom_parts:
        LOGGER.warning(
            "_build_enriched_lineup: no custom lineup data — "
            "returning soccerdata lineup without athlete_id"
        )
        return sd_lineup

    # Only bring athlete_id and game_id from the custom side.
    # All numeric stats (goals, assists, cards) come from the more reliable
    # soccerdata lineup and are intentionally kept as-is.
    custom_df = pd.concat(custom_parts, ignore_index=True)
    merge_key = ["game", "player", "team"]
    custom_slim = (
        custom_df[merge_key + ["athlete_id", "game_id"]]
        .drop_duplicates(subset=merge_key)
    )

    enriched = sd_lineup.merge(custom_slim, on=merge_key, how="left")

    coverage = enriched["athlete_id"].notna().mean() * 100
    LOGGER.info(
        "_build_enriched_lineup: %s rows total, athlete_id coverage=%.1f%%",
        len(enriched), coverage,
    )

    return enriched


# ---------------------------------------------------------------------------
# Events enrichment: add game_id from schedule
# ---------------------------------------------------------------------------

def _enrich_events_with_game_id(
    schedule: "pd.DataFrame",
    events: "pd.DataFrame",
) -> "pd.DataFrame":
    """Add game_id to an events DataFrame by left-joining the schedule on ``game``.

    Parameters
    ----------
    schedule : pd.DataFrame
        Schedule DataFrame with at least ``game`` and ``game_id`` columns.
    events : pd.DataFrame
        Raw events DataFrame (output of ``reader.read_events().reset_index()``).

    Returns
    -------
    pd.DataFrame
        Events with a ``game_id`` column added (``None`` when unmatched).
        Returns *events* unchanged if either DataFrame lacks the required columns.
    """
    if events.empty:
        return events
    if "game_id" not in schedule.columns or "game" not in events.columns:
        LOGGER.warning(
            "_enrich_events_with_game_id: missing 'game' or 'game_id' column — "
            "returning events without game_id"
        )
        return events
    game_id_map = schedule[["game", "game_id"]].drop_duplicates(subset="game")
    return events.merge(game_id_map, on="game", how="left")


# ---------------------------------------------------------------------------
# Bronze layer: Granular extract-and-upload functions (one per entity)
# ---------------------------------------------------------------------------

def extract_schedule_to_minio(
    league_key: str,
    season: int,
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    raw_bucket: str = "datalake-raw",
) -> dict[str, Any]:
    """Fetch the season schedule from ESPN and upload schedule.json to MinIO.

    Returns
    -------
    dict
        ``{schedule_rows, game_map, bronze_base, elapsed_seconds}``
        where *game_map* is ``{game_name: game_id}`` — a serializable dict
        that downstream tasks use to correlate lineup/events with game IDs.
    """
    import soccerdata as sd

    t0 = time.perf_counter()
    ensure_brasileirao_mapping(league_key)
    reader = sd.ESPN(leagues=league_key, seasons=season)

    schedule = reader.read_schedule().reset_index()
    LOGGER.info("read_schedule: %s rows in %.2fs", len(schedule), time.perf_counter() - t0)

    # Build game_map for downstream tasks (JSON-serializable for XCom)
    game_map: dict[str, int] = {}
    if "game" in schedule.columns and "game_id" in schedule.columns:
        game_map = {
            str(row["game"]): int(row["game_id"])
            for _, row in schedule.iterrows()
            if pd.notna(row.get("game_id"))
        }

    bronze_base = f"espn/brasileirao/{season}"
    schedule_key = f"{bronze_base}/schedule.json"
    s3 = _make_s3_client(minio_endpoint, minio_access_key, minio_secret_key)
    s3.put_object(Bucket=raw_bucket, Key=schedule_key, Body=_to_json_str(schedule))
    LOGGER.info("Uploaded schedule to s3://%s/%s", raw_bucket, schedule_key)

    return {
        "schedule_rows": len(schedule),
        "game_map": game_map,
        "bronze_base": bronze_base,
        "elapsed_seconds": round(time.perf_counter() - t0, 2),
    }


def extract_matchsheet_to_minio(
    league_key: str,
    season: int,
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    raw_bucket: str = "datalake-raw",
) -> dict[str, Any]:
    """Fetch per-match team stats from ESPN and upload matchsheet.json to MinIO.

    Reads from soccerdata's local disk cache when available (populated by
    a prior ``extract_schedule_to_minio`` call on the same machine).

    Returns
    -------
    dict
        ``{matchsheet_rows, elapsed_seconds}``
    """
    import soccerdata as sd

    t0 = time.perf_counter()
    ensure_brasileirao_mapping(league_key)
    reader = sd.ESPN(leagues=league_key, seasons=season)

    matchsheet = reader.read_matchsheet().reset_index()
    matchsheet = matchsheet.drop(columns=["roster"], errors="ignore")
    LOGGER.info("read_matchsheet: %s rows in %.2fs", len(matchsheet), time.perf_counter() - t0)

    bronze_base = f"espn/brasileirao/{season}"
    matchsheet_key = f"{bronze_base}/matchsheet.json"
    s3 = _make_s3_client(minio_endpoint, minio_access_key, minio_secret_key)
    s3.put_object(Bucket=raw_bucket, Key=matchsheet_key, Body=_to_json_str(matchsheet))
    LOGGER.info("Uploaded matchsheet to s3://%s/%s", raw_bucket, matchsheet_key)

    return {
        "matchsheet_rows": len(matchsheet),
        "elapsed_seconds": round(time.perf_counter() - t0, 2),
    }


def extract_lineup_to_minio(
    league_key: str,
    season: int,
    game_map: dict[str, int],
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    raw_bucket: str = "datalake-raw",
) -> dict[str, Any]:
    """Fetch per-player lineup data from ESPN and upload lineup.json to MinIO.

    Calls ``read_lineup()`` (which also caches ESPN summary JSONs to disk),
    then enriches the result with ``athlete_id`` via ``_build_enriched_lineup``.

    Parameters
    ----------
    game_map : dict[str, int]
        Mapping of game name → game_id, as returned by
        ``extract_schedule_to_minio``.

    Returns
    -------
    dict
        ``{lineup_rows, elapsed_seconds}``
    """
    import soccerdata as sd

    t0 = time.perf_counter()
    ensure_brasileirao_mapping(league_key)
    reader = sd.ESPN(leagues=league_key, seasons=season)

    sd_lineup = reader.read_lineup().reset_index()
    LOGGER.info("read_lineup: %s rows in %.2fs", len(sd_lineup), time.perf_counter() - t0)

    # Reconstruct a minimal schedule DataFrame from game_map so
    # _build_enriched_lineup can correlate summary JSONs with games.
    schedule_slim = pd.DataFrame(
        [{"game": g, "game_id": gid} for g, gid in game_map.items()]
    )

    te = time.perf_counter()
    lineup = _build_enriched_lineup(schedule_slim, sd_lineup, reader, league_key, season)
    LOGGER.info("_build_enriched_lineup: %s rows in %.2fs", len(lineup), time.perf_counter() - te)

    bronze_base = f"espn/brasileirao/{season}"
    lineup_key = f"{bronze_base}/lineup.json"
    s3 = _make_s3_client(minio_endpoint, minio_access_key, minio_secret_key)
    s3.put_object(Bucket=raw_bucket, Key=lineup_key, Body=_to_json_str(lineup))
    LOGGER.info("Uploaded lineup to s3://%s/%s", raw_bucket, lineup_key)

    return {
        "lineup_rows": len(lineup),
        "elapsed_seconds": round(time.perf_counter() - t0, 2),
    }


def extract_events_to_minio(
    league_key: str,
    season: int,
    game_map: dict[str, int],
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    raw_bucket: str = "datalake-raw",
) -> dict[str, Any]:
    """Fetch match events (goals, cards, subs) from ESPN and upload events.json.

    ``read_events()`` may not be available for every ESPN league/season;
    failures are treated as non-fatal — an empty array is uploaded instead.

    Parameters
    ----------
    game_map : dict[str, int]
        Mapping of game name → game_id, as returned by
        ``extract_schedule_to_minio``.

    Returns
    -------
    dict
        ``{events_rows, elapsed_seconds}``
    """
    import soccerdata as sd

    t0 = time.perf_counter()
    ensure_brasileirao_mapping(league_key)
    reader = sd.ESPN(leagues=league_key, seasons=season)

    schedule_slim = pd.DataFrame(
        [{"game": g, "game_id": gid} for g, gid in game_map.items()]
    )

    try:
        events = reader.read_events().reset_index()
        events = _enrich_events_with_game_id(schedule_slim, events)
        LOGGER.info("read_events: %s rows in %.2fs", len(events), time.perf_counter() - t0)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("read_events failed (non-fatal): %s — events.json will be empty", exc)
        events = pd.DataFrame()

    bronze_base = f"espn/brasileirao/{season}"
    events_key = f"{bronze_base}/events.json"
    s3 = _make_s3_client(minio_endpoint, minio_access_key, minio_secret_key)
    s3.put_object(Bucket=raw_bucket, Key=events_key, Body=_to_json_str(events))
    LOGGER.info("Uploaded events to s3://%s/%s", raw_bucket, events_key)

    return {
        "events_rows": len(events),
        "elapsed_seconds": round(time.perf_counter() - t0, 2),
    }
