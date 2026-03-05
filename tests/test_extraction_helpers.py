"""Unit tests for dags/lib/extraction_helpers.py."""

import json
from unittest.mock import MagicMock, patch

import pytest

# We need to add dags/ to path so we can import lib.*
import sys
from pathlib import Path

# Add the dags directory to the Python path for direct imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

from lib.extraction_helpers import (
    build_queue_message,
    fetch_player_profile,
    parse_lineup_with_ids,
    slug,
    _parse_clock_minute,
    _parse_substitution_data,
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
