"""Unit tests for dags/lib/extraction_helpers.py."""

import json
from unittest.mock import MagicMock, patch

import pytest

# We need to add dags/ to path so we can import lib.*
import sys
from pathlib import Path

# Add the dags directory to the Python path for direct imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

import io
import pandas as pd

from lib.extraction_helpers import (
    _build_enriched_lineup,
    _enrich_events_with_game_id,
    _parse_clock_minute,
    _parse_substitution_data,
    build_queue_message,
    fetch_player_profile,
    parse_lineup_with_ids,
    slug,
)


# ---------------------------------------------------------------------------
# Test: slug()
# ---------------------------------------------------------------------------


class TestSlug:
    """Tests for the slug() filesystem-safe string converter."""

    def test_basic_text(self):
        """Simple text with spaces becomes underscored lowercase."""
        assert slug("Palmeiras x Flamengo") == "palmeiras_x_flamengo"

    def test_special_characters(self):
        """Non-alphanumeric characters are replaced with underscores."""
        assert slug("São Paulo F.C.") == "s_o_paulo_f_c"

    def test_numbers(self):
        """Numbers are preserved in the slug."""
        assert slug("Round 12") == "round_12"

    def test_empty_string(self):
        """Empty input returns 'unknown' fallback."""
        assert slug("") == "unknown"

    def test_only_special_chars(self):
        """Input with only special characters returns 'unknown'."""
        assert slug("---") == "unknown"

    def test_leading_trailing_underscores_stripped(self):
        """Leading/trailing underscores are stripped after conversion."""
        assert slug("__test__") == "test"


# ---------------------------------------------------------------------------
# Test: parse_lineup_with_ids()
# ---------------------------------------------------------------------------


class TestParseLineupWithIds:
    """Tests for the lineup parser that captures ESPN athlete IDs."""

    @pytest.fixture
    def sample_roster_data(self) -> list[dict]:
        """A minimal ESPN roster array with 2 players."""
        return [
            {
                "athlete": {
                    "id": "12345",
                    "displayName": "Raphael Veiga",
                },
                "position": {"name": "Midfielder"},
                "formationPlace": 10,
                "starter": True,
            },
            {
                "athlete": {
                    "id": "67890",
                    "displayName": "Endrick",
                },
                "position": {"name": "Forward"},
                "formationPlace": 9,
                "starter": False,
                "subbedIn": {
                    "didSub": True,
                    "clock": {"displayValue": "65'"},
                },
            },
        ]

    @pytest.fixture
    def game_info(self) -> dict:
        """Minimal game context dict."""
        return {
            "game": "Palmeiras vs Flamengo",
            "game_id": 999,
            "league": "BRA-Brasileirao",
            "season": 2026,
        }

    def test_athlete_ids_are_captured(self, sample_roster_data, game_info):
        """Verify the athlete_id column is populated from ESPN data."""
        df = parse_lineup_with_ids(
            roster_data=sample_roster_data,
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        # Should have 2 rows
        assert len(df) == 2
        # Athlete IDs should be integers
        assert df.iloc[0]["athlete_id"] == 12345
        assert df.iloc[1]["athlete_id"] == 67890

    def test_player_names_are_captured(self, sample_roster_data, game_info):
        """Verify player display names are extracted."""
        df = parse_lineup_with_ids(
            roster_data=sample_roster_data,
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        assert df.iloc[0]["player"] == "Raphael Veiga"
        assert df.iloc[1]["player"] == "Endrick"

    def test_position_is_captured(self, sample_roster_data, game_info):
        """Verify position names are extracted."""
        df = parse_lineup_with_ids(
            roster_data=sample_roster_data,
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        assert df.iloc[0]["position"] == "Midfielder"
        assert df.iloc[1]["position"] == "Forward"

    def test_team_and_game_context(self, sample_roster_data, game_info):
        """Verify team name, game name, and match context are set."""
        df = parse_lineup_with_ids(
            roster_data=sample_roster_data,
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        assert all(df["team"] == "Palmeiras")
        assert all(df["is_home"] == True)
        assert all(df["game_id"] == 999)

    def test_empty_roster(self, game_info):
        """An empty roster should return an empty DataFrame."""
        df = parse_lineup_with_ids(
            roster_data=[],
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        assert df.empty

    def test_missing_athlete_id(self, game_info):
        """A player entry without an athlete ID should result in None."""
        roster = [
            {
                "athlete": {"displayName": "Unknown Player"},
                "position": {"name": "Defender"},
                "starter": True,
            }
        ]
        df = parse_lineup_with_ids(
            roster_data=roster,
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        assert len(df) == 1
        assert df.iloc[0]["athlete_id"] is None
        assert df.iloc[0]["player"] == "Unknown Player"

    def test_starter_sub_in_is_start(self, sample_roster_data, game_info):
        """A starter player should have sub_in='start'."""
        df = parse_lineup_with_ids(
            roster_data=sample_roster_data,
            game_info=game_info,
            team_display_name="Palmeiras",
            is_home=True,
        )
        # First player is a starter
        assert df.iloc[0]["sub_in"] == "start"


# ---------------------------------------------------------------------------
# Test: _parse_clock_minute()
# ---------------------------------------------------------------------------


class TestParseClockMinute:
    """Tests for parsing ESPN clock display values."""

    def test_simple_minute(self):
        """A standard minute value like '65'' should parse to 65."""
        event = {"clock": {"displayValue": "65'"}}
        assert _parse_clock_minute(event) == 65

    def test_stoppage_time(self):
        """90+3' should parse to 93 (sum of digit groups)."""
        event = {"clock": {"displayValue": "90+3'"}}
        assert _parse_clock_minute(event) == 93

    def test_empty_clock(self):
        """An empty display value should return None."""
        event = {"clock": {"displayValue": ""}}
        assert _parse_clock_minute(event) is None

    def test_missing_clock_key(self):
        """A missing clock key should return None."""
        assert _parse_clock_minute({}) is None


# ---------------------------------------------------------------------------
# Test: build_queue_message()
# ---------------------------------------------------------------------------


class TestBuildQueueMessage:
    """Tests for the standardized queue message builder."""

    def test_message_has_all_required_fields(self):
        """Verify all expected keys are present in the message."""
        msg = build_queue_message(
            entity_type="match",
            entity_id=12345,
            payload_format="json",
            path="/output/match.json",
            provider="espn",
            league="BRA-Brasileirao",
            season=2026,
        )
        assert msg["entity_type"] == "match"
        assert msg["entity_id"] == 12345
        assert msg["payload_format"] == "json"
        assert msg["path"] == "/output/match.json"
        assert msg["provider"] == "espn"
        assert msg["league"] == "BRA-Brasileirao"
        assert msg["season"] == 2026
        assert "created_at" in msg  # timestamp should be auto-generated

    def test_player_profile_message(self):
        """Verify a player_profile message is correctly structured."""
        msg = build_queue_message(
            entity_type="player_profile",
            entity_id=67890,
            payload_format="json",
            path="/output/profiles/67890.json",
            provider="espn",
            league="BRA-Brasileirao",
            season=2026,
        )
        assert msg["entity_type"] == "player_profile"
        assert msg["entity_id"] == 67890


# ---------------------------------------------------------------------------
# Test: fetch_player_profile()
# ---------------------------------------------------------------------------


class TestFetchPlayerProfile:
    """Tests for the ESPN athlete profile fetcher."""

    @patch("lib.extraction_helpers.requests.get")
    @patch("lib.extraction_helpers.time.sleep")
    def test_successful_profile_fetch(self, mock_sleep, mock_get):
        """A successful API response should return a parsed profile dict."""
        # Mock the ESPN API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "athlete": {
                "id": "12345",
                "guid": "abc-def-123",
                "firstName": "Raphael",
                "lastName": "Veiga",
                "displayName": "Raphael Veiga",
                "fullName": "Raphael Augusto de Araújo Veiga",
                "jersey": "23",
                "position": {"id": "10", "name": "Midfielder"},
                "team": {"id": "100", "displayName": "Palmeiras"},
                "displayDOB": "6/19/1995",
                "age": 30,
                "gender": "MALE",
                "active": True,
                "links": [
                    {
                        "rel": ["playercard", "desktop"],
                        "href": "https://espn.com/soccer/player/12345",
                    }
                ],
            }
        }
        mock_get.return_value = mock_response

        # Call the function
        profile = fetch_player_profile(
            athlete_id=12345,
            league_espn_key="bra.1",
            delay=0,  # no delay for testing
        )

        # Verify the parsed profile
        assert profile is not None
        assert profile["espn_athlete_id"] == 12345
        assert profile["first_name"] == "Raphael"
        assert profile["last_name"] == "Veiga"
        assert profile["display_name"] == "Raphael Veiga"
        assert profile["position_name"] == "Midfielder"
        assert profile["team_name"] == "Palmeiras"
        assert profile["date_of_birth"] == "6/19/1995"
        assert profile["age"] == 30
        assert profile["is_active"] == True
        assert profile["profile_url"] == "https://espn.com/soccer/player/12345"

    @patch("lib.extraction_helpers.requests.get")
    @patch("lib.extraction_helpers.time.sleep")
    def test_failed_profile_fetch(self, mock_sleep, mock_get):
        """A network error should return None instead of crashing."""
        import requests as req
        mock_get.side_effect = req.RequestException("Network timeout")

        profile = fetch_player_profile(
            athlete_id=99999,
            league_espn_key="bra.1",
            delay=0,
        )

        assert profile is None


# ---------------------------------------------------------------------------
# Test: _build_enriched_lineup()
# ---------------------------------------------------------------------------

def _make_schedule(*game_tuples):
    """Build a minimal schedule DataFrame with (game, game_id) rows."""
    return pd.DataFrame(game_tuples, columns=["game", "game_id"])


def _make_sd_lineup(rows):
    """Build a minimal soccerdata lineup DataFrame from a list of dicts."""
    return pd.DataFrame(rows)


def _make_reader(summaries: dict):
    """Build a mock soccerdata ESPN reader.

    summaries: dict mapping game_id (int) -> summary dict (as returned by ESPN API)
    """
    reader = MagicMock()
    reader.data_dir = MagicMock()

    def fake_get(url, filepath):
        # Extract game_id from the URL (ends with ?event=<id>)
        game_id = int(url.split("event=")[-1])
        data = summaries.get(game_id)
        if data is None:
            raise FileNotFoundError(f"No summary for game_id={game_id}")
        return io.StringIO(json.dumps(data))

    reader.get.side_effect = fake_get
    return reader


def _minimal_summary(home_name: str, away_name: str, home_players, away_players):
    """Build a minimal ESPN summary JSON structure."""
    def _make_roster(players):
        return [
            {
                "athlete": {"id": str(p["athlete_id"]), "displayName": p["name"]},
                "position": {"name": p.get("position", "MF")},
                "starter": p.get("starter", True),
                "formationPlace": p.get("formation_place"),
                "subbedIn": False,
                "subbedOut": False,
            }
            for p in players
        ]

    return {
        "boxscore": {
            "form": [
                {"team": {"displayName": home_name}},
                {"team": {"displayName": away_name}},
            ]
        },
        "rosters": [
            {"roster": _make_roster(home_players)},
            {"roster": _make_roster(away_players)},
        ],
    }


class TestBuildEnrichedLineup:

    def test_athlete_id_merged_into_soccerdata_lineup(self):
        """athlete_id from the ESPN summary should appear in the enriched lineup."""
        schedule = _make_schedule(("Palmeiras - Flamengo", 101))

        sd_lineup = _make_sd_lineup([
            {"game": "Palmeiras - Flamengo", "player": "Veiga",  "team": "Palmeiras", "total_goals": 1},
            {"game": "Palmeiras - Flamengo", "player": "Arrascaeta", "team": "Flamengo", "total_goals": 0},
        ])

        summaries = {
            101: _minimal_summary(
                "Palmeiras", "Flamengo",
                home_players=[{"athlete_id": 111, "name": "Veiga"}],
                away_players=[{"athlete_id": 222, "name": "Arrascaeta"}],
            )
        }

        with patch.dict("soccerdata._config.LEAGUE_DICT", {"BRA-Brasileirao": {"ESPN": "bra.1"}}):
            reader = _make_reader(summaries)
            result = _build_enriched_lineup(
                schedule, sd_lineup, reader, "BRA-Brasileirao", 2024
            )

        assert "athlete_id" in result.columns
        veiga_row = result[result["player"] == "Veiga"].iloc[0]
        assert veiga_row["athlete_id"] == 111
        assert veiga_row["total_goals"] == 1  # soccerdata stats preserved

        arra_row = result[result["player"] == "Arrascaeta"].iloc[0]
        assert arra_row["athlete_id"] == 222

    def test_game_id_merged_into_soccerdata_lineup(self):
        """game_id should be added to every row."""
        schedule = _make_schedule(("Team A - Team B", 999))
        sd_lineup = _make_sd_lineup([
            {"game": "Team A - Team B", "player": "Player1", "team": "Team A", "total_goals": 0},
        ])
        summaries = {
            999: _minimal_summary(
                "Team A", "Team B",
                home_players=[{"athlete_id": 55, "name": "Player1"}],
                away_players=[],
            )
        }

        with patch.dict("soccerdata._config.LEAGUE_DICT", {"BRA-Brasileirao": {"ESPN": "bra.1"}}):
            result = _build_enriched_lineup(
                schedule, sd_lineup, _make_reader(summaries), "BRA-Brasileirao", 2024
            )

        assert result.iloc[0]["game_id"] == 999

    def test_fallback_when_no_game_id_column(self):
        """If schedule has no game_id column, return soccerdata lineup unchanged."""
        schedule = pd.DataFrame([{"game": "A - B"}])  # no game_id
        sd_lineup = _make_sd_lineup([{"game": "A - B", "player": "X", "team": "A"}])

        result = _build_enriched_lineup(
            schedule, sd_lineup, MagicMock(), "BRA-Brasileirao", 2024
        )

        assert "athlete_id" not in result.columns
        assert len(result) == 1

    def test_fallback_when_summary_json_missing(self):
        """If the ESPN summary JSON is not cached, the row is kept with athlete_id=None."""
        schedule = _make_schedule(("X - Y", 404))
        sd_lineup = _make_sd_lineup([
            {"game": "X - Y", "player": "Someone", "team": "X", "total_goals": 0},
        ])

        with patch.dict("soccerdata._config.LEAGUE_DICT", {"BRA-Brasileirao": {"ESPN": "bra.1"}}):
            # No summaries for game_id 404 → FileNotFoundError
            reader = _make_reader({})
            result = _build_enriched_lineup(
                schedule, sd_lineup, reader, "BRA-Brasileirao", 2024
            )

        # Should fall back to original sd_lineup when nothing was extracted
        assert len(result) == len(sd_lineup)

    def test_multiple_games_all_enriched(self):
        """Multiple games in the schedule should all get athlete_ids."""
        schedule = _make_schedule(("A - B", 1), ("C - D", 2))
        sd_lineup = _make_sd_lineup([
            {"game": "A - B", "player": "Alpha", "team": "A", "total_goals": 1},
            {"game": "C - D", "player": "Delta", "team": "C", "total_goals": 0},
        ])
        summaries = {
            1: _minimal_summary("A", "B", [{"athlete_id": 10, "name": "Alpha"}], []),
            2: _minimal_summary("C", "D", [{"athlete_id": 20, "name": "Delta"}], []),
        }

        with patch.dict("soccerdata._config.LEAGUE_DICT", {"BRA-Brasileirao": {"ESPN": "bra.1"}}):
            result = _build_enriched_lineup(
                schedule, sd_lineup, _make_reader(summaries), "BRA-Brasileirao", 2024
            )

        assert result[result["player"] == "Alpha"].iloc[0]["athlete_id"] == 10
        assert result[result["player"] == "Delta"].iloc[0]["athlete_id"] == 20

    def test_soccerdata_stats_not_overwritten(self):
        """Numeric stats from soccerdata (total_goals etc.) must not be overwritten."""
        schedule = _make_schedule(("X - Y", 77))
        sd_lineup = _make_sd_lineup([
            {
                "game": "X - Y", "player": "Scorer", "team": "X",
                "total_goals": 2, "goal_assists": 1, "yellow_cards": 0,
            },
        ])
        summaries = {
            77: _minimal_summary("X", "Y", [{"athlete_id": 99, "name": "Scorer"}], [])
        }

        with patch.dict("soccerdata._config.LEAGUE_DICT", {"BRA-Brasileirao": {"ESPN": "bra.1"}}):
            result = _build_enriched_lineup(
                schedule, sd_lineup, _make_reader(summaries), "BRA-Brasileirao", 2024
            )

        row = result[result["player"] == "Scorer"].iloc[0]
        assert row["total_goals"] == 2
        assert row["goal_assists"] == 1
        assert row["yellow_cards"] == 0
        assert row["athlete_id"] == 99


# ---------------------------------------------------------------------------
# Test: _enrich_events_with_game_id()
# ---------------------------------------------------------------------------


class TestEnrichEventsWithGameId:

    def _schedule(self, rows):
        return pd.DataFrame(rows)

    def _events(self, rows):
        return pd.DataFrame(rows)

    def test_game_id_added_from_schedule(self):
        """game_id should be merged from the schedule on the game column."""
        schedule = self._schedule([
            {"game": "A - B", "game_id": 101},
            {"game": "C - D", "game_id": 202},
        ])
        events = self._events([
            {"game": "A - B", "event_type": "goal", "player": "Alpha"},
            {"game": "C - D", "event_type": "yellow_card", "player": "Beta"},
        ])
        result = _enrich_events_with_game_id(schedule, events)
        assert result[result["game"] == "A - B"].iloc[0]["game_id"] == 101
        assert result[result["game"] == "C - D"].iloc[0]["game_id"] == 202

    def test_multiple_events_same_game(self):
        """Multiple events for the same game all get the correct game_id."""
        schedule = self._schedule([{"game": "A - B", "game_id": 77}])
        events = self._events([
            {"game": "A - B", "event_type": "goal"},
            {"game": "A - B", "event_type": "goal"},
            {"game": "A - B", "event_type": "yellow_card"},
        ])
        result = _enrich_events_with_game_id(schedule, events)
        assert (result["game_id"] == 77).all()

    def test_fallback_when_schedule_missing_game_id_column(self):
        """Returns events unchanged if schedule has no game_id column."""
        schedule = self._schedule([{"game": "A - B"}])  # no game_id
        events = self._events([{"game": "A - B", "event_type": "goal"}])
        result = _enrich_events_with_game_id(schedule, events)
        assert "game_id" not in result.columns
        assert len(result) == 1

    def test_fallback_when_events_missing_game_column(self):
        """Returns events unchanged if events has no game column."""
        schedule = self._schedule([{"game": "A - B", "game_id": 1}])
        events = self._events([{"event_type": "goal", "player": "X"}])  # no game
        result = _enrich_events_with_game_id(schedule, events)
        assert "game_id" not in result.columns

    def test_empty_events_returns_empty(self):
        """An empty events DataFrame is returned as-is."""
        schedule = self._schedule([{"game": "A - B", "game_id": 1}])
        events = pd.DataFrame()
        result = _enrich_events_with_game_id(schedule, events)
        assert result.empty

    def test_unmatched_game_gets_null_game_id(self):
        """An event whose game is not in the schedule gets game_id=NaN."""
        schedule = self._schedule([{"game": "A - B", "game_id": 1}])
        events = self._events([{"game": "X - Y", "event_type": "goal"}])
        result = _enrich_events_with_game_id(schedule, events)
        assert pd.isna(result.iloc[0]["game_id"])
