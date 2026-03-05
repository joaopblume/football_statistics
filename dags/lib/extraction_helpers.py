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
# Bronze layer: Extract from ESPN and upload to MinIO
# ---------------------------------------------------------------------------

def extract_and_upload_to_minio(
    league_key: str,
    season: int,
    minio_endpoint: str = "http://minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin123",
    raw_bucket: str = "datalake-raw",
) -> dict[str, Any]:
    """Extract schedule, matchsheet, and lineup from ESPN and upload to MinIO.

    Fetches all three ESPN soccerdata datasets, serializes them as JSON,
    and uploads to the MinIO Bronze layer (datalake-raw bucket).

    Parameters
    ----------
    league_key : str
        The soccerdata league key, e.g. ``"BRA-Brasileirao"``.
    season : int
        The season year to fetch (e.g. 2024).
    minio_endpoint : str
        MinIO S3 API endpoint URL.
    minio_access_key : str
        MinIO root user / access key.
    minio_secret_key : str
        MinIO root password / secret key.
    raw_bucket : str
        Bucket name for the Bronze/raw layer.

    Returns
    -------
    dict
        Metadata about the extraction: row counts, S3 paths, and timing.
    """
    import boto3
    import soccerdata as sd

    started = time.perf_counter()

    # Ensure the league mapping exists in soccerdata config
    ensure_brasileirao_mapping(league_key)

    # Initialize the ESPN reader
    reader = sd.ESPN(leagues=league_key, seasons=season)
    LOGGER.info("ESPN reader initialized for league=%s season=%s", league_key, season)

    # --- Fetch schedule (match list) ---
    t0 = time.perf_counter()
    schedule = reader.read_schedule().reset_index()
    LOGGER.info("read_schedule: %s rows in %.2fs", len(schedule), time.perf_counter() - t0)

    # --- Fetch matchsheet (team stats per match) ---
    t0 = time.perf_counter()
    matchsheet = reader.read_matchsheet().reset_index()
    # Drop the roster column (raw JSON blobs, too large for storage)
    matchsheet = matchsheet.drop(columns=["roster"], errors="ignore")
    LOGGER.info("read_matchsheet: %s rows in %.2fs", len(matchsheet), time.perf_counter() - t0)

    # --- Fetch lineup (player stats per match) ---
    t0 = time.perf_counter()
    lineup = reader.read_lineup().reset_index()
    LOGGER.info("read_lineup: %s rows in %.2fs", len(lineup), time.perf_counter() - t0)

    # Convert DataFrames to JSON strings (records orientation, ISO dates)
    schedule_json = json.dumps(
        json.loads(schedule.to_json(orient="records", date_format="iso")),
        indent=2, ensure_ascii=False,
    )
    matchsheet_json = json.dumps(
        json.loads(matchsheet.to_json(orient="records", date_format="iso")),
        indent=2, ensure_ascii=False,
    )
    lineup_json = json.dumps(
        json.loads(lineup.to_json(orient="records", date_format="iso")),
        indent=2, ensure_ascii=False,
    )

    # Connect to MinIO via boto3 S3 client
    s3_client = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name="us-east-1",
    )

    # Build the Bronze path prefix
    bronze_base = f"espn/brasileirao/{season}"

    # Upload schedule JSON to Bronze
    schedule_key = f"{bronze_base}/schedule.json"
    s3_client.put_object(Bucket=raw_bucket, Key=schedule_key, Body=schedule_json)
    LOGGER.info("Uploaded schedule to s3://%s/%s", raw_bucket, schedule_key)

    # Upload matchsheet JSON to Bronze
    matchsheet_key = f"{bronze_base}/matchsheet.json"
    s3_client.put_object(Bucket=raw_bucket, Key=matchsheet_key, Body=matchsheet_json)
    LOGGER.info("Uploaded matchsheet to s3://%s/%s", raw_bucket, matchsheet_key)

    # Upload lineup JSON to Bronze
    lineup_key = f"{bronze_base}/lineup.json"
    s3_client.put_object(Bucket=raw_bucket, Key=lineup_key, Body=lineup_json)
    LOGGER.info("Uploaded lineup to s3://%s/%s", raw_bucket, lineup_key)

    elapsed = time.perf_counter() - started
    LOGGER.info("extract_and_upload_to_minio completed in %.2fs", elapsed)

    # Return metadata for downstream logging/XCom
    return {
        "schedule_rows": len(schedule),
        "matchsheet_rows": len(matchsheet),
        "lineup_rows": len(lineup),
        "bronze_base": bronze_base,
        "raw_bucket": raw_bucket,
        "elapsed_seconds": round(elapsed, 2),
    }
