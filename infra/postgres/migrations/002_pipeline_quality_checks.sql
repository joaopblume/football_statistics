-- Migration 002: pipeline_quality_checks
-- Tracks quality check results per season/stage for observability.
--
-- Run: psql "$DATABASE_URL" -f infra/postgres/migrations/002_pipeline_quality_checks.sql
--
-- Depends on: 001_pipeline_season_control.sql (must be applied first)
-- Safe to re-run (idempotent).

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_quality_checks (
    id          BIGSERIAL PRIMARY KEY,
    season_id   INT NOT NULL REFERENCES pipeline_season_control(id) ON DELETE CASCADE,
    stage       VARCHAR(20)  NOT NULL CHECK (stage IN ('silver', 'gold')),
    check_name  VARCHAR(100) NOT NULL,
    status      VARCHAR(10)  NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
    details     TEXT,
    checked_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quality_checks_season_stage
    ON pipeline_quality_checks (season_id, stage, checked_at DESC);

COMMENT ON TABLE pipeline_quality_checks IS
    'Per-season, per-stage quality check results. Populated by Airflow after '
    'Silver/Gold notebooks succeed. All checks recorded here are genuine passes '
    '— if the notebook failed, this table is never written.';

COMMENT ON COLUMN pipeline_quality_checks.season_id    IS 'FK to pipeline_season_control.id';
COMMENT ON COLUMN pipeline_quality_checks.stage        IS 'Pipeline stage: silver or gold';
COMMENT ON COLUMN pipeline_quality_checks.check_name   IS 'Identifier of the check (e.g. teams_not_empty)';
COMMENT ON COLUMN pipeline_quality_checks.status       IS 'Result: pass, warn, or fail';
COMMENT ON COLUMN pipeline_quality_checks.details      IS 'Optional context (measured value, threshold, etc.)';

COMMIT;
