import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pendulum
import soccerdata as sd
from airflow.sdk import dag, task


LEAGUE_KEY = "BRA-Brasileirao"
SEASON = 2026
LOGGER = logging.getLogger(__name__)


def _ensure_brasileirao_mapping() -> None:
    from soccerdata import _config

    if LEAGUE_KEY not in _config.LEAGUE_DICT:
        _config.LEAGUE_DICT[LEAGUE_KEY] = {
            "ESPN": "bra.1",
            "season_start": "Apr",
            "season_end": "Dec",
        }


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", str(value)).strip("_").lower()
    return slug or "unknown"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, default=str, indent=2)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["soccerdata", "raw", "queue", "brasileirao"],
)
def get_brasileirao():
    @task()
    def extract_raw_and_enqueue() -> str:
        started = time.perf_counter()
        LOGGER.info("Inicio da task extract_raw_and_enqueue")
        LOGGER.info("Parametros: league=%s season=%s", LEAGUE_KEY, SEASON)

        _ensure_brasileirao_mapping()
        LOGGER.info("Mapping da liga garantido em runtime")
        reader = sd.ESPN(leagues=LEAGUE_KEY, seasons=SEASON)
        LOGGER.info("Reader ESPN inicializado")

        output_root = Path(os.getenv("OUTPUT_DIR", "output")).resolve()
        max_matches = int(os.getenv("MAX_MATCHES", "0"))
        log_every = int(os.getenv("LOG_EVERY", "10"))
        LOGGER.info("Diretorio output=%s MAX_MATCHES=%s LOG_EVERY=%s", output_root, max_matches, log_every)
        run_ts = pendulum.now("UTC").format("YYYYMMDDTHHmmss")
        run_root = output_root / "raw" / "soccerdata" / _slug(LEAGUE_KEY) / str(SEASON) / run_ts
        queue_file = output_root / "queue" / "pending.jsonl"
        queue_file.parent.mkdir(parents=True, exist_ok=True)
        LOGGER.info("Run root=%s queue_file=%s", run_root, queue_file)

        t_schedule = time.perf_counter()
        LOGGER.info("Iniciando read_schedule")
        schedule = reader.read_schedule().reset_index()
        LOGGER.info("read_schedule concluido em %.2fs", time.perf_counter() - t_schedule)
        LOGGER.info("Schedule carregado com %s linhas", len(schedule))
        if max_matches > 0:
            schedule = schedule.head(max_matches)
            LOGGER.info("Schedule truncado para %s partidas por MAX_MATCHES", len(schedule))
        t_save_schedule = time.perf_counter()
        _write_csv(run_root / "schedule.csv", schedule)
        LOGGER.info("Schedule salvo em CSV em %.2fs", time.perf_counter() - t_save_schedule)
        LOGGER.info("Schedule salvo em CSV")

        queue_messages: list[dict[str, Any]] = []
        collected_lineups: list[pd.DataFrame] = []

        # 1) Por partida: schedule row em JSON + lineup em CSV
        total_matches = len(schedule)
        lineup_success = 0
        lineup_fail = 0
        for idx, row in enumerate(schedule.itertuples(index=False), start=1):
            loop_started = time.perf_counter()
            game_id = int(getattr(row, "game_id"))
            game_slug = _slug(getattr(row, "game"))
            match_dir = run_root / "matches" / f"{game_id}_{game_slug}"

            if idx == 1 or idx % max(1, log_every) == 0:
                LOGGER.info("Processando partida %s/%s game_id=%s", idx, total_matches, game_id)

            schedule_row = {k: getattr(row, k) for k in schedule.columns}
            schedule_json = match_dir / "schedule_row.json"
            _write_json(schedule_json, schedule_row)
            queue_messages.append(
                {
                    "entity_type": "match",
                    "entity_id": game_id,
                    "payload_format": "json",
                    "path": str(schedule_json),
                    "provider": "espn",
                    "league": LEAGUE_KEY,
                    "season": SEASON,
                    "created_at": pendulum.now("UTC").to_iso8601_string(),
                }
            )

            try:
                t_lineup = time.perf_counter()
                lineup = reader.read_lineup(match_id=game_id).reset_index()
                LOGGER.info(
                    "read_lineup game_id=%s concluido em %.2fs",
                    game_id,
                    time.perf_counter() - t_lineup,
                )
            except Exception as exc:  # noqa: BLE001
                lineup_fail += 1
                LOGGER.exception("Falha ao ler lineup game_id=%s: %s", game_id, exc)
                continue

            lineup_success += 1
            collected_lineups.append(lineup)
            lineup_csv = match_dir / "lineup.csv"
            _write_csv(lineup_csv, lineup)
            queue_messages.append(
                {
                    "entity_type": "match_lineup",
                    "entity_id": game_id,
                    "payload_format": "csv",
                    "path": str(lineup_csv),
                    "provider": "espn",
                    "league": LEAGUE_KEY,
                    "season": SEASON,
                    "created_at": pendulum.now("UTC").to_iso8601_string(),
                }
            )

            if idx == 1 or idx % max(1, log_every) == 0:
                LOGGER.info(
                    "Partida game_id=%s concluida em %.2fs (lineup_rows=%s, sucesso=%s, falha=%s)",
                    game_id,
                    time.perf_counter() - loop_started,
                    len(lineup),
                    lineup_success,
                    lineup_fail,
                )

        LOGGER.info(
            "Loop de partidas finalizado. total=%s sucesso=%s falha=%s queue_messages_atual=%s",
            total_matches,
            lineup_success,
            lineup_fail,
            len(queue_messages),
        )

        # 2) Por time e jogador: arquivos agregados do lineup da temporada
        if collected_lineups:
            t_concat = time.perf_counter()
            all_lineups = pd.concat(collected_lineups, ignore_index=True)
            LOGGER.info("Concat de lineups concluido em %.2fs", time.perf_counter() - t_concat)
        else:
            all_lineups = pd.DataFrame()
        LOGGER.info("Lineups consolidados: %s linhas", len(all_lineups))

        team_groups = all_lineups.groupby("team", dropna=True) if not all_lineups.empty else []
        team_total = int(team_groups.ngroups) if not all_lineups.empty else 0
        team_count = 0
        for team_name, team_df in team_groups:
            team_path = run_root / "teams" / f"{_slug(team_name)}.csv"
            _write_csv(team_path, team_df)
            queue_messages.append(
                {
                    "entity_type": "team",
                    "entity_id": str(team_name),
                    "payload_format": "csv",
                    "path": str(team_path),
                    "provider": "espn",
                    "league": LEAGUE_KEY,
                    "season": SEASON,
                    "created_at": pendulum.now("UTC").to_iso8601_string(),
                }
            )
            team_count += 1
            if team_count == 1 or team_count % max(1, log_every) == 0:
                LOGGER.info("Team arquivo salvo: %s (%s/%s)", team_name, team_count, team_total)

        player_groups = all_lineups.groupby("player", dropna=True) if not all_lineups.empty else []
        player_total = int(player_groups.ngroups) if not all_lineups.empty else 0
        player_count = 0
        for player_name, player_df in player_groups:
            player_path = run_root / "players" / f"{_slug(player_name)}.csv"
            _write_csv(player_path, player_df)
            queue_messages.append(
                {
                    "entity_type": "player",
                    "entity_id": str(player_name),
                    "payload_format": "csv",
                    "path": str(player_path),
                    "provider": "espn",
                    "league": LEAGUE_KEY,
                    "season": SEASON,
                    "created_at": pendulum.now("UTC").to_iso8601_string(),
                }
            )
            player_count += 1
            if player_count == 1 or player_count % max(1, log_every) == 0:
                LOGGER.info("Player arquivo salvo: %s (%s/%s)", player_name, player_count, player_total)

        with queue_file.open("a", encoding="utf-8") as f:
            for msg in queue_messages:
                f.write(json.dumps(msg, ensure_ascii=False) + "\n")
        LOGGER.info("Fila gravada em %s com %s mensagens", queue_file, len(queue_messages))

        elapsed = time.perf_counter() - started
        LOGGER.info("Task finalizada em %.2fs", elapsed)

        return (
            f"Raw extraido em {run_root}. "
            f"Mensagens enfileiradas: {len(queue_messages)} em {queue_file}."
        )

    extract_raw_and_enqueue()


get_brasileirao()
