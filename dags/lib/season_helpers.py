"""Helpers for reading and updating pipeline_season_control in PostgreSQL.

The season control table is the single source of truth for which seasons
the pipeline should process and what stage each is at.  DAGs call these
helpers instead of using hardcoded SEASON constants.

Connection pattern follows ingestion_helpers.py: callers pass a zero-argument
callable (get_conn_fn) that returns a psycopg2 connection with autocommit=False.
This keeps the helpers Airflow-agnostic and fully unit-testable.

Status lifecycle:
    pending
      └─► bronze_running ──► bronze_done
                                 └─► silver_running ──► silver_done
                                                           └─► gold_running ──► complete
    Any stage → failed  (last_error_stage records which one)
    Reset for retry: UPDATE SET status = 'pending' (see infra/postgres/README.md)
"""

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

# Maps stage name → (running_status, done_status, started_col, completed_col)
_STAGE_META: dict[str, tuple[str, str, str, str]] = {
    "bronze": ("bronze_running", "bronze_done",  "bronze_started_at",  "bronze_completed_at"),
    "silver": ("silver_running", "silver_done",  "silver_started_at",  "silver_completed_at"),
    "gold":   ("gold_running",   "complete",     "gold_started_at",    "gold_completed_at"),
}

# Maps stage → the status a row must have to be eligible for that stage
_STAGE_PREREQUISITE: dict[str, str] = {
    "bronze": "pending",
    "silver": "bronze_done",
    "gold":   "silver_done",
}

# DDL executed by ensure_season_control_table (mirrors the migration file)
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline_season_control (
    id              BIGSERIAL    PRIMARY KEY,
    league_key      VARCHAR(100) NOT NULL,
    season          INTEGER      NOT NULL,
    status          VARCHAR(30)  NOT NULL DEFAULT 'pending',
    bronze_started_at   TIMESTAMPTZ,
    bronze_completed_at TIMESTAMPTZ,
    silver_started_at   TIMESTAMPTZ,
    silver_completed_at TIMESTAMPTZ,
    gold_started_at     TIMESTAMPTZ,
    gold_completed_at   TIMESTAMPTZ,
    last_error       TEXT,
    last_error_stage VARCHAR(30),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pipeline_season UNIQUE (league_key, season)
);
"""

_CREATE_TRIGGER_SQL = """
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tg_pipeline_season_control_updated_at ON pipeline_season_control;
CREATE TRIGGER tg_pipeline_season_control_updated_at
    BEFORE UPDATE ON pipeline_season_control
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_season_control_league_status
    ON pipeline_season_control (league_key, status, season ASC);
"""


# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

def ensure_season_control_table(get_conn_fn) -> None:
    """Create pipeline_season_control table if it does not exist.

    Idempotent: safe to call on every DAG run. Mirrors the migration file
    infra/postgres/migrations/001_pipeline_season_control.sql.
    """
    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE_SQL)
            cur.execute(_CREATE_TRIGGER_SQL)
            cur.execute(_CREATE_INDEX_SQL)
        conn.commit()
        LOGGER.debug("pipeline_season_control table ensured")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

def get_pending_season(
    get_conn_fn,
    league_key: str,
    stage: str,
) -> dict[str, Any] | None:
    """Return the next season row eligible to be processed at *stage*.

    Eligibility rules:
      bronze → status = 'pending'           OR (status = 'failed' AND last_error_stage = 'bronze')
      silver → status = 'bronze_done'       OR (status = 'failed' AND last_error_stage = 'silver')
      gold   → status = 'silver_done'       OR (status = 'failed' AND last_error_stage = 'gold')

    Returns the lowest season number that qualifies (ORDER BY season ASC),
    or None if no eligible row exists.

    Parameters
    ----------
    get_conn_fn : callable
        Zero-argument function returning a psycopg2 connection.
    league_key : str
        League identifier, e.g. 'BRA-Brasileirao'.
    stage : str
        One of 'bronze', 'silver', 'gold'.
    """
    if stage not in _STAGE_META:
        raise ValueError(f"Unknown stage: {stage!r}. Must be one of {list(_STAGE_META)}")

    prerequisite_status = _STAGE_PREREQUISITE[stage]

    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, league_key, season, status
                FROM pipeline_season_control
                WHERE league_key = %s
                  AND (
                      status = %s
                      OR (status = 'failed' AND last_error_stage = %s)
                  )
                ORDER BY season ASC
                LIMIT 1
                """,
                (league_key, prerequisite_status, stage),
            )
            row = cur.fetchone()

        if row is None:
            LOGGER.info(
                "get_pending_season: no eligible season for stage=%s league=%s",
                stage, league_key,
            )
            return None

        result = {
            "id": row[0],
            "league_key": row[1],
            "season": row[2],
            "status": row[3],
        }
        LOGGER.info(
            "get_pending_season: found season=%s (id=%s status=%s) for stage=%s",
            result["season"], result["id"], result["status"], stage,
        )
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def mark_stage_started(get_conn_fn, season_id: int, stage: str) -> None:
    """Transition a season row to the running status for *stage*.

    Also clears last_error / last_error_stage from any previous failure
    so the row is clean for the new attempt.

    Parameters
    ----------
    get_conn_fn : callable
        Zero-argument function returning a psycopg2 connection.
    season_id : int
        Primary key of the pipeline_season_control row.
    stage : str
        One of 'bronze', 'silver', 'gold'.
    """
    running_status, _, started_col, _ = _STAGE_META[stage]
    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE pipeline_season_control
                SET status           = %s,
                    {started_col}    = NOW(),
                    last_error       = NULL,
                    last_error_stage = NULL
                WHERE id = %s
                """,
                (running_status, season_id),
            )
        conn.commit()
        LOGGER.info("mark_stage_started: id=%s stage=%s → %s", season_id, stage, running_status)
    finally:
        conn.close()


def mark_stage_completed(get_conn_fn, season_id: int, stage: str) -> None:
    """Transition a season row to the done/complete status for *stage*.

    Parameters
    ----------
    get_conn_fn : callable
        Zero-argument function returning a psycopg2 connection.
    season_id : int
        Primary key of the pipeline_season_control row.
    stage : str
        One of 'bronze', 'silver', 'gold'.
    """
    _, done_status, _, completed_col = _STAGE_META[stage]
    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE pipeline_season_control
                SET status          = %s,
                    {completed_col} = NOW()
                WHERE id = %s
                """,
                (done_status, season_id),
            )
        conn.commit()
        LOGGER.info("mark_stage_completed: id=%s stage=%s → %s", season_id, stage, done_status)
    finally:
        conn.close()


def mark_stage_failed(
    get_conn_fn,
    season_id: int,
    stage: str,
    error: str,
) -> None:
    """Transition a season row to 'failed', recording the stage and error.

    Parameters
    ----------
    get_conn_fn : callable
        Zero-argument function returning a psycopg2 connection.
    season_id : int
        Primary key of the pipeline_season_control row.
    stage : str
        One of 'bronze', 'silver', 'gold'.
    error : str
        Human-readable error description (truncated to 2000 chars).
    """
    conn = get_conn_fn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_season_control
                SET status           = 'failed',
                    last_error       = %s,
                    last_error_stage = %s
                WHERE id = %s
                """,
                (error[:2000], stage, season_id),
            )
        conn.commit()
        LOGGER.error(
            "mark_stage_failed: id=%s stage=%s error=%s", season_id, stage, error[:200]
        )
    finally:
        conn.close()