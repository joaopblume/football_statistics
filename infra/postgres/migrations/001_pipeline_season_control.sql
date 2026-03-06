-- =============================================================================
-- Migration: 001_pipeline_season_control
-- Purpose  : Create the pipeline_season_control table.
--
-- This table is the single source of truth for which seasons the pipeline
-- should process and what stage each season is currently at.
--
-- The pipeline DAGs read from this table instead of using hardcoded SEASON
-- constants, enabling:
--   - Multi-season support without code changes
--   - Incremental retry from the failed stage (not from scratch)
--   - Full audit trail of when each stage ran and any errors
--
-- Status lifecycle:
--   pending
--     └─► bronze_running ──► bronze_done
--                                └─► silver_running ──► silver_done
--                                                          └─► gold_running ──► complete
--   Any stage can transition to: failed  (last_error_stage records which stage)
--   Reset for retry: UPDATE SET status = 'pending' WHERE league_key = '...' AND season = ...
--
-- How to run:
--   psql "$DATABASE_URL" -f infra/postgres/migrations/001_pipeline_season_control.sql
--
-- Idempotent: safe to run multiple times (uses IF NOT EXISTS / ON CONFLICT DO NOTHING).
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_season_control (
    id              BIGSERIAL    PRIMARY KEY,
    league_key      VARCHAR(100) NOT NULL,
    season          INTEGER      NOT NULL,
    status          VARCHAR(30)  NOT NULL DEFAULT 'pending',

    -- Per-stage timestamps
    bronze_started_at   TIMESTAMPTZ,
    bronze_completed_at TIMESTAMPTZ,
    silver_started_at   TIMESTAMPTZ,
    silver_completed_at TIMESTAMPTZ,
    gold_started_at     TIMESTAMPTZ,
    gold_completed_at   TIMESTAMPTZ,

    -- Last failure tracking (overwritten on each failure)
    last_error       TEXT,
    last_error_stage VARCHAR(30),

    -- Row metadata
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pipeline_season UNIQUE (league_key, season)
);

-- Auto-update updated_at on every UPDATE
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

-- Index for the common DAG query: find the next season to process per stage
CREATE INDEX IF NOT EXISTS idx_season_control_league_status
    ON pipeline_season_control (league_key, status, season ASC);

-- =============================================================================
-- Seed: initial seasons to process
-- Add more rows here as new seasons begin.
-- =============================================================================
INSERT INTO pipeline_season_control (league_key, season, status)
VALUES ('BRA-Brasileirao', 2024, 'pending')
ON CONFLICT (league_key, season) DO NOTHING;