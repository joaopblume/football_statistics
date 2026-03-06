-- Migration 003: Seed initial seasons for the Brasileirao pipeline.
--
-- Inserts seasons 2022–2025 into pipeline_season_control so the Bronze DAG
-- (running on @weekly schedule) can process them sequentially in order.
-- Safe to run multiple times (ON CONFLICT DO NOTHING).

INSERT INTO pipeline_season_control (league_key, season, status)
VALUES
    ('BRA-Brasileirao', 2022, 'pending'),
    ('BRA-Brasileirao', 2023, 'pending'),
    ('BRA-Brasileirao', 2024, 'pending'),
    ('BRA-Brasileirao', 2025, 'pending')
ON CONFLICT (league_key, season) DO NOTHING;
