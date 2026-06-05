"""Quality check helpers for the Brasileirao data pipeline.

Records quality check results to ``pipeline_quality_checks`` after each
Silver and Gold stage completes successfully.

Design rationale
----------------
- Quality *gates* (fail-fast assertions) live in the Spark notebooks and
  abort the notebook if data is bad.  If the notebook succeeds, all gates
  passed.
- Quality *records* (this module) capture WHAT was validated and WHEN, giving
  a historical audit trail per season in PostgreSQL.
- All records written here have ``status='pass'`` because a failed notebook
  never reaches the Airflow task that calls these functions.
"""

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Check catalogue — must mirror the assertions in the Spark notebooks
# ---------------------------------------------------------------------------

# Keys used as check_name values in pipeline_quality_checks.
_STAGE_CHECKS: dict[str, list[str]] = {
    "silver": [
        "teams_not_empty",
        "players_not_empty",
        "match_statistics_not_empty",
        "player_match_stats_not_empty",
        "team_name_null_rate_ok",
        "player_name_null_rate_ok",
        "player_team_null_rate_ok",
        "match_game_null_rate_ok",
        "player_match_game_null_rate_ok",
        "home_score_not_all_null",
    ],
    "gold": [
        "player_season_stats_not_empty",
        "player_name_not_null",
        "team_name_not_null",
    ],
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def record_stage_quality_passed(
    get_conn_fn,
    season_id: int,
    stage: str,
) -> None:
    """Record all expected quality checks as passed for a stage.

    Called by the Airflow DAG *after* the Silver or Gold notebook succeeds.
    Every check in ``_STAGE_CHECKS[stage]`` is written with ``status='pass'``.

    Parameters
    ----------
    get_conn_fn : callable
        Zero-argument callable that returns a psycopg2 connection with
        ``autocommit=False``.  Same contract as in ``season_helpers.py``.
    season_id : int
        ``pipeline_season_control.id`` for the season being processed.
    stage : str
        ``'silver'`` or ``'gold'``.

    Raises
    ------
    ValueError
        If *stage* is not in ``_STAGE_CHECKS``.
    """
    checks = _STAGE_CHECKS.get(stage)
    if checks is None:
        raise ValueError(f"Unknown stage for quality checks: {stage!r}")

    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            for check_name in checks:
                cur.execute(
                    """
                    INSERT INTO pipeline_quality_checks
                        (season_id, stage, check_name, status)
                    VALUES (%s, %s, %s, 'pass')
                    """,
                    (season_id, stage, check_name),
                )
        conn.commit()
        LOGGER.info(
            "Recorded %d quality checks (all pass) for season_id=%s stage=%s",
            len(checks), season_id, stage,
        )
    finally:
        conn.close()


def record_quality_check(
    get_conn_fn,
    season_id: int,
    stage: str,
    check_name: str,
    status: str,
    details: str | None = None,
) -> None:
    """Insert a single quality check result into ``pipeline_quality_checks``.

    Use this for one-off or custom checks that are not part of the standard
    ``_STAGE_CHECKS`` catalogue.

    Parameters
    ----------
    status : str
        ``'pass'``, ``'warn'``, or ``'fail'``.
    details : str, optional
        Human-readable context (e.g. ``"null_rate=0.03, threshold=0.05"``).
    """
    if status not in ("pass", "warn", "fail"):
        raise ValueError(f"Invalid status: {status!r}. Must be 'pass', 'warn', or 'fail'.")

    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_quality_checks
                    (season_id, stage, check_name, status, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (season_id, stage, check_name, status, details),
            )
        conn.commit()
        LOGGER.info(
            "Quality check recorded: season_id=%s stage=%s check=%s status=%s",
            season_id, stage, check_name, status,
        )
    finally:
        conn.close()


def record_quality_report(
    get_conn_fn,
    season_id: int,
    stage: str,
    checks: list[dict[str, Any]],
) -> int:
    """Record a batch of *evaluated* quality checks for a stage.

    Unlike ``record_stage_quality_passed`` (which blindly writes 'pass' for a
    fixed catalogue), this records the **measured** result of each check that
    the Spark notebook actually computed.  Each entry in *checks* must be a
    dict with keys ``check_name``, ``status`` ('pass'|'warn'|'fail') and an
    optional ``details`` string (e.g. ``"null_rate=0.03, threshold=0.05"``).

    The notebook is expected to ``raise`` on any hard failure *before* writing
    its tables, so in practice only 'pass'/'warn' rows reach this function on a
    successful run — but 'fail' is accepted and validated for completeness.

    Returns the number of rows written.

    Raises
    ------
    ValueError
        If *checks* is empty, an entry is missing ``check_name``, or a
        ``status`` is not one of 'pass'/'warn'/'fail'.
    """
    if not checks:
        raise ValueError("record_quality_report: 'checks' must be non-empty")

    valid_status = {"pass", "warn", "fail"}
    # Validate everything up front so we never write a partial batch.
    for entry in checks:
        name = entry.get("check_name")
        status = entry.get("status")
        if not name:
            raise ValueError(f"record_quality_report: entry missing check_name: {entry!r}")
        if status not in valid_status:
            raise ValueError(
                f"record_quality_report: invalid status {status!r} for {name!r}"
            )

    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            for entry in checks:
                cur.execute(
                    """
                    INSERT INTO pipeline_quality_checks
                        (season_id, stage, check_name, status, details)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        season_id,
                        stage,
                        entry["check_name"],
                        entry["status"],
                        entry.get("details"),
                    ),
                )
        conn.commit()
        n_fail = sum(1 for e in checks if e["status"] == "fail")
        n_warn = sum(1 for e in checks if e["status"] == "warn")
        LOGGER.info(
            "Recorded %d quality checks (warn=%d fail=%d) for season_id=%s stage=%s",
            len(checks), n_warn, n_fail, season_id, stage,
        )
        return len(checks)
    finally:
        conn.close()


def get_quality_summary(
    get_conn_fn,
    season_id: int,
    stage: str | None = None,
) -> list[dict[str, Any]]:
    """Retrieve quality check results for a season.

    Parameters
    ----------
    stage : str, optional
        Filter to a specific stage (``'silver'`` or ``'gold'``).
        Returns all stages when omitted.

    Returns
    -------
    list[dict]
        Each dict has keys: ``stage``, ``check_name``, ``status``,
        ``details``, ``checked_at``.
    """
    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            if stage:
                cur.execute(
                    """
                    SELECT stage, check_name, status, details, checked_at
                    FROM pipeline_quality_checks
                    WHERE season_id = %s AND stage = %s
                    ORDER BY checked_at DESC
                    """,
                    (season_id, stage),
                )
            else:
                cur.execute(
                    """
                    SELECT stage, check_name, status, details, checked_at
                    FROM pipeline_quality_checks
                    WHERE season_id = %s
                    ORDER BY stage, checked_at DESC
                    """,
                    (season_id,),
                )
            return [
                {
                    "stage":      row[0],
                    "check_name": row[1],
                    "status":     row[2],
                    "details":    row[3],
                    "checked_at": row[4],
                }
                for row in cur.fetchall()
            ]
    finally:
        conn.close()
