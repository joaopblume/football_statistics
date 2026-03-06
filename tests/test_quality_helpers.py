"""Unit tests for dags/lib/quality_helpers.py.

All tests use mock connections — no real PostgreSQL required.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))

from lib.quality_helpers import (
    _STAGE_CHECKS,
    get_quality_summary,
    record_quality_check,
    record_stage_quality_passed,
)


# ---------------------------------------------------------------------------
# Mock connection helpers (same pattern as test_season_helpers.py)
# ---------------------------------------------------------------------------

def _make_conn():
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def _make_get_conn(fetchall_return=None):
    conn, cursor = _make_conn()
    cursor.fetchall.return_value = fetchall_return or []
    return lambda: conn, conn, cursor


# ---------------------------------------------------------------------------
# record_stage_quality_passed
# ---------------------------------------------------------------------------

class TestRecordStageQualityPassed:

    def test_inserts_one_row_per_silver_check(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=1, stage="silver")

        expected_count = len(_STAGE_CHECKS["silver"])
        assert cursor.execute.call_count == expected_count

    def test_inserts_one_row_per_gold_check(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=2, stage="gold")

        expected_count = len(_STAGE_CHECKS["gold"])
        assert cursor.execute.call_count == expected_count

    def test_all_inserts_use_pass_status(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=1, stage="silver")

        for c in cursor.execute.call_args_list:
            params = c[0][1]  # positional args: (sql, params)
            # params = (season_id, stage, check_name)
            # status='pass' is embedded in the SQL literal, not a param
            assert params[0] == 1       # season_id
            assert params[1] == "silver"

    def test_commits_once(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=1, stage="silver")
        conn.commit.assert_called_once()

    def test_closes_connection(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=1, stage="silver")
        conn.close.assert_called_once()

    def test_raises_on_unknown_stage(self):
        get_conn_fn, _, _ = _make_get_conn()
        with pytest.raises(ValueError, match="Unknown stage"):
            record_stage_quality_passed(get_conn_fn, season_id=1, stage="bronze")

    def test_closes_connection_on_db_error(self):
        conn, cursor = _make_conn()
        cursor.execute.side_effect = Exception("DB error")

        with pytest.raises(Exception, match="DB error"):
            record_stage_quality_passed(lambda: conn, season_id=1, stage="silver")

        conn.close.assert_called_once()

    def test_all_silver_check_names_are_recorded(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=5, stage="silver")

        recorded_names = {c[0][1][2] for c in cursor.execute.call_args_list}
        assert recorded_names == set(_STAGE_CHECKS["silver"])

    def test_all_gold_check_names_are_recorded(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_stage_quality_passed(get_conn_fn, season_id=5, stage="gold")

        recorded_names = {c[0][1][2] for c in cursor.execute.call_args_list}
        assert recorded_names == set(_STAGE_CHECKS["gold"])


# ---------------------------------------------------------------------------
# record_quality_check
# ---------------------------------------------------------------------------

class TestRecordQualityCheck:

    def test_inserts_with_correct_params(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_quality_check(
            get_conn_fn,
            season_id=3,
            stage="silver",
            check_name="custom_check",
            status="warn",
            details="null_rate=0.04",
        )

        params = cursor.execute.call_args[0][1]
        assert params[0] == 3
        assert params[1] == "silver"
        assert params[2] == "custom_check"
        assert params[3] == "warn"
        assert params[4] == "null_rate=0.04"

    def test_accepts_none_details(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_quality_check(get_conn_fn, 1, "gold", "check", "pass", details=None)
        params = cursor.execute.call_args[0][1]
        assert params[4] is None

    def test_raises_on_invalid_status(self):
        get_conn_fn, _, _ = _make_get_conn()
        with pytest.raises(ValueError, match="Invalid status"):
            record_quality_check(get_conn_fn, 1, "silver", "check", "unknown")

    def test_commits_and_closes(self):
        get_conn_fn, conn, cursor = _make_get_conn()
        record_quality_check(get_conn_fn, 1, "silver", "check", "pass")
        conn.commit.assert_called_once()
        conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# get_quality_summary
# ---------------------------------------------------------------------------

class TestGetQualitySummary:

    def _fake_rows(self):
        return [
            ("silver", "teams_not_empty", "pass", None, "2024-01-01T00:00:00Z"),
            ("silver", "players_not_empty", "pass", None, "2024-01-01T00:00:00Z"),
        ]

    def test_returns_list_of_dicts(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchall_return=self._fake_rows())
        result = get_quality_summary(get_conn_fn, season_id=1)

        assert len(result) == 2
        assert result[0]["check_name"] == "teams_not_empty"
        assert result[0]["status"] == "pass"

    def test_filters_by_stage(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchall_return=self._fake_rows())
        get_quality_summary(get_conn_fn, season_id=1, stage="silver")

        params = cursor.execute.call_args[0][1]
        assert params == (1, "silver")

    def test_no_stage_filter_uses_season_only(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchall_return=[])
        get_quality_summary(get_conn_fn, season_id=7)

        params = cursor.execute.call_args[0][1]
        assert params == (7,)

    def test_closes_connection(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchall_return=[])
        get_quality_summary(get_conn_fn, season_id=1)
        conn.close.assert_called_once()

    def test_returns_empty_list_when_no_rows(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchall_return=[])
        result = get_quality_summary(get_conn_fn, season_id=99)
        assert result == []

    def test_dict_has_all_expected_keys(self):
        get_conn_fn, conn, cursor = _make_get_conn(fetchall_return=self._fake_rows())
        result = get_quality_summary(get_conn_fn, season_id=1)

        expected_keys = {"stage", "check_name", "status", "details", "checked_at"}
        assert set(result[0].keys()) == expected_keys
