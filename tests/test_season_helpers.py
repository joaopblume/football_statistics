"""Unit tests for dags/lib/season_helpers.py.

All tests use mock connections — no real PostgreSQL required.
The mock simulates psycopg2's cursor/connection interface.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# Add dags/ to path so we can import lib.*
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

from lib.season_helpers import (
    _STAGE_META,
    _STAGE_PREREQUISITE,
    get_pending_season,
    mark_stage_completed,
    mark_stage_failed,
    mark_stage_started,
)


# ---------------------------------------------------------------------------
# Helpers to build mock psycopg2 connections
# ---------------------------------------------------------------------------

def _make_conn(fetchone_return=None):
    """Return a mock psycopg2 connection whose cursor returns *fetchone_return*."""
    cursor = MagicMock()
    cursor.fetchone.return_value = fetchone_return
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _make_get_conn(fetchone_return=None):
    """Return a get_conn_fn callable and the underlying (conn, cursor) pair."""
    conn, cursor = _make_conn(fetchone_return)
    return lambda: conn, conn, cursor


# ---------------------------------------------------------------------------
# get_pending_season
# ---------------------------------------------------------------------------

class TestGetPendingSeason:

    def test_returns_none_when_no_row(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchone_return=None)
        result = get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="bronze")
        assert result is None
        conn.close.assert_called_once()

    def test_returns_row_as_dict(self):
        get_conn_fn, conn, cursor = _make_get_conn(
            fetchone_return=(42, "BRA-Brasileirao", 2024, "pending")
        )
        result = get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="bronze")
        assert result == {"id": 42, "league_key": "BRA-Brasileirao", "season": 2024, "status": "pending"}
        conn.close.assert_called_once()

    def test_queries_correct_prerequisite_for_bronze(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchone_return=None)
        get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="bronze")
        args = cursor.execute.call_args[0]
        # Second positional arg is the params tuple
        params = args[1]
        assert params[1] == "pending"        # prerequisite status
        assert params[2] == "bronze"         # last_error_stage for retry

    def test_queries_correct_prerequisite_for_silver(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchone_return=None)
        get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="silver")
        params = cursor.execute.call_args[0][1]
        assert params[1] == "bronze_done"
        assert params[2] == "silver"

    def test_queries_correct_prerequisite_for_gold(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchone_return=None)
        get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="gold")
        params = cursor.execute.call_args[0][1]
        assert params[1] == "silver_done"
        assert params[2] == "gold"

    def test_passes_league_key_as_first_param(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchone_return=None)
        get_pending_season(get_conn_fn, "MY-League", stage="bronze")
        params = cursor.execute.call_args[0][1]
        assert params[0] == "MY-League"

    def test_raises_on_unknown_stage(self):
        get_conn_fn, _, _ = _make_get_conn()
        with pytest.raises(ValueError, match="Unknown stage"):
            get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="invalid")

    def test_closes_connection_on_success(self):
        get_conn_fn, conn, _ = _make_get_conn(
            fetchone_return=(1, "BRA-Brasileirao", 2024, "pending")
        )
        get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="bronze")
        conn.close.assert_called_once()

    def test_closes_connection_even_on_db_error(self):
        conn, cursor = _make_conn()
        cursor.execute.side_effect = Exception("DB error")
        conn.cursor.return_value = cursor

        def get_conn_fn():
            return conn

        with pytest.raises(Exception, match="DB error"):
            get_pending_season(get_conn_fn, "BRA-Brasileirao", stage="bronze")
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# mark_stage_started
# ---------------------------------------------------------------------------

class TestMarkStageStarted:

    @pytest.mark.parametrize("stage", ["bronze", "silver", "gold"])
    def test_sets_correct_running_status(self, stage):
        get_conn_fn, conn, cursor = _make_get_conn()
        mark_stage_started(get_conn_fn, season_id=7, stage=stage)

        expected_running, _, started_col, _ = _STAGE_META[stage]
        sql_call = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]

        assert started_col in sql_call
        assert params[0] == expected_running
        assert params[1] == 7
        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_clears_last_error_columns(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        mark_stage_started(get_conn_fn, season_id=3, stage="silver")
        sql_call = cursor.execute.call_args[0][0]
        assert "last_error" in sql_call
        assert "last_error_stage" in sql_call


# ---------------------------------------------------------------------------
# mark_stage_completed
# ---------------------------------------------------------------------------

class TestMarkStageCompleted:

    @pytest.mark.parametrize("stage,expected_done", [
        ("bronze", "bronze_done"),
        ("silver", "silver_done"),
        ("gold",   "complete"),
    ])
    def test_sets_correct_done_status(self, stage, expected_done):
        get_conn_fn, conn, cursor = _make_get_conn()
        mark_stage_completed(get_conn_fn, season_id=5, stage=stage)

        _, done_status, _, completed_col = _STAGE_META[stage]
        sql_call = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]

        assert completed_col in sql_call
        assert params[0] == expected_done
        assert params[1] == 5
        conn.commit.assert_called_once()
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# mark_stage_failed
# ---------------------------------------------------------------------------

class TestMarkStageFailed:

    def test_sets_failed_status(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        mark_stage_failed(get_conn_fn, season_id=9, stage="silver", error="Boom")

        sql_call = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]

        assert "failed" in sql_call
        assert params[0] == "Boom"          # error (truncated)
        assert params[1] == "silver"        # last_error_stage
        assert params[2] == 9              # id
        conn.commit.assert_called_once()
        conn.close.assert_called_once()

    def test_truncates_long_error(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        long_error = "x" * 5000
        mark_stage_failed(get_conn_fn, season_id=1, stage="bronze", error=long_error)

        params = cursor.execute.call_args[0][1]
        assert len(params[0]) == 2000

    @pytest.mark.parametrize("stage", ["bronze", "silver", "gold"])
    def test_records_correct_stage(self, stage):
        get_conn_fn, conn, cursor = _make_get_conn()
        mark_stage_failed(get_conn_fn, season_id=1, stage=stage, error="err")
        params = cursor.execute.call_args[0][1]
        assert params[1] == stage


# ---------------------------------------------------------------------------
# Full lifecycle integration (mock-level)
# ---------------------------------------------------------------------------

class TestLifecycle:

    def test_bronze_lifecycle(self):
        """Simulate a full bronze success: pending → bronze_running → bronze_done."""
        calls = []

        def make_get_conn(fetchone):
            def get_conn_fn():
                conn, cursor = _make_conn(fetchone_return=fetchone)
                cursor.execute.side_effect = lambda sql, params=None: calls.append(
                    ("execute", params)
                )
                return conn
            return get_conn_fn

        # Step 1: get_pending_season returns a pending row
        row = get_pending_season(
            make_get_conn((1, "BRA-Brasileirao", 2024, "pending")),
            "BRA-Brasileirao",
            stage="bronze",
        )
        assert row["season"] == 2024
        assert row["id"] == 1

        # Step 2: mark_stage_started
        mark_stage_started(make_get_conn(None), season_id=1, stage="bronze")

        # Step 3: mark_stage_completed
        mark_stage_completed(make_get_conn(None), season_id=1, stage="bronze")

        # Verify the three SQL calls happened (via params inspection)
        statuses = [p[0] for _, p in calls if p]
        assert "bronze_running" in statuses
        assert "bronze_done" in statuses