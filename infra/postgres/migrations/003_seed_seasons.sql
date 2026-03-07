-- Migration 003: Seed seasons 2022–2026 for all configured leagues.
--
-- Seasons are processed newest-first (2026 → 2025 → … → 2022) because
-- get_pending_season now uses ORDER BY season DESC.
--
-- Each league runs its own Bronze DAG (@hourly) and picks up the next
-- pending season for that specific league independently.
--
-- Safe to run multiple times (ON CONFLICT DO NOTHING).

INSERT INTO pipeline_season_control (league_key, season, status)
VALUES
    -- Brasileirao
    ('BRA-Brasileirao', 2026, 'pending'),
    ('BRA-Brasileirao', 2025, 'pending'),
    ('BRA-Brasileirao', 2024, 'pending'),
    ('BRA-Brasileirao', 2023, 'pending'),
    ('BRA-Brasileirao', 2022, 'pending'),

    -- Serie A
    ('ITA-Serie A', 2026, 'pending'),
    ('ITA-Serie A', 2025, 'pending'),
    ('ITA-Serie A', 2024, 'pending'),
    ('ITA-Serie A', 2023, 'pending'),
    ('ITA-Serie A', 2022, 'pending'),

    -- Premier League
    ('ENG-Premier League', 2026, 'pending'),
    ('ENG-Premier League', 2025, 'pending'),
    ('ENG-Premier League', 2024, 'pending'),
    ('ENG-Premier League', 2023, 'pending'),
    ('ENG-Premier League', 2022, 'pending'),

    -- Ligue 1
    ('FRA-Ligue 1', 2026, 'pending'),
    ('FRA-Ligue 1', 2025, 'pending'),
    ('FRA-Ligue 1', 2024, 'pending'),
    ('FRA-Ligue 1', 2023, 'pending'),
    ('FRA-Ligue 1', 2022, 'pending')

ON CONFLICT (league_key, season) DO NOTHING;
