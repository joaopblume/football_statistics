import argparse
import os
import sys
import time
from typing import Any, Dict, List

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

if __package__ is None or __package__ == "":
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from database.connection import SessionLocal
from extraction.api_client import ApiFootballClient
from extraction.mappers import (
    map_league,
    map_player,
    map_player_statistics,
    map_season,
    map_team,
    map_venue,
)
from extraction.repositories import upsert_many, upsert_one


def _fail(message: str) -> None:
    print(f"[FALHA] {message}")


def _ok(message: str) -> None:
    print(f"[SUCESSO] {message}")


def _diagnose_database_error(exc: SQLAlchemyError, session) -> None:
    original = getattr(exc, "orig", None)
    original_text = str(original) if original else "sem detalhes do driver"
    safe_url = session.bind.url.render_as_string(hide_password=True) if session.bind else "indisponivel"

    print("[DIAGNOSTICO] Falha detalhada de conexao:")
    print(f"[DIAGNOSTICO] URL (senha oculta): {safe_url}")
    print(f"[DIAGNOSTICO] Driver reportou: {original_text}")
    print(
        "[DIAGNOSTICO] ENV PG*: "
        f"PGHOST={os.getenv('PGHOST', '<ausente>')} "
        f"PGPORT={os.getenv('PGPORT', '<ausente>')} "
        f"PGDATABASE={os.getenv('PGDATABASE', '<ausente>')} "
        f"PGUSER={os.getenv('PGUSER', '<ausente>')} "
        f"PGPASSWORD={'<definida>' if os.getenv('PGPASSWORD') else '<ausente>'}"
    )


def _check_database_ready(session) -> bool:
    expected_tables = {
        "leagues",
        "seasons",
        "venues",
        "teams",
        "players",
        "player_statistics",
    }

    try:
        session.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        _fail(f"Conexao com banco falhou: {exc}")
        _diagnose_database_error(exc, session)
        return False

    inspector = inspect(session.bind)
    existing_tables = set(inspector.get_table_names())
    missing_tables = sorted(expected_tables - existing_tables)

    if missing_tables:
        _fail(f"Tabela(s) ausente(s) no banco: {', '.join(missing_tables)}")
        return False

    _ok("Conexao com banco validada e tabelas necessarias encontradas.")
    return True


def run_extraction(league_id: int, season: int) -> int:
    session = SessionLocal()
    client = None

    try:
        print(f"[INFO] Iniciando extracao league_id={league_id} season={season}")
        if not _check_database_ready(session):
            return 1

        client = ApiFootballClient(per_page_delay_seconds=10.0, rate_limit_retry_seconds=10)

        # 1) League + season
        league_data = client.get_league(league_id=league_id, season=season)
        league_response = league_data.get("response") or []
        if not league_response:
            _fail("Nenhum dado de liga retornado pela API.")
            print(
                f"[DIAGNOSTICO] errors={league_data.get('errors')} "
                f"results={league_data.get('results')} paging={league_data.get('paging')}"
            )
            return 1

        league_entry = league_response[0]
        league_payload = map_league(league_entry.get("league") or {}, league_entry.get("country") or {})
        season_payloads = [
            map_season(league_id=league_payload["id"], season_payload=item)
            for item in (league_entry.get("seasons") or [])
            if item.get("year") == season
        ]

        upsert_one(session, "league", league_payload)
        _ok(f"Liga '{league_payload.get('name')}' preparada para persistencia.")

        if season_payloads:
            count = upsert_many(session, "season", season_payloads)
            _ok(f"{count} registro(s) de temporada preparado(s).")
        else:
            print("[AVISO] Temporada especificada nao encontrada no payload de leagues.")

        # 2) Teams + venues
        teams_data = client.get_teams(league_id=league_id, season=season)
        team_rows = teams_data.get("response") or []
        if not team_rows:
            _fail("Nenhum time retornado para a liga/temporada informada.")
            return 1

        venues_to_upsert: List[Dict[str, Any]] = []
        teams_to_upsert: List[Dict[str, Any]] = []

        for item in team_rows:
            team_payload = item.get("team") or {}
            venue_payload = item.get("venue") or {}

            mapped_venue = map_venue(venue_payload)
            if mapped_venue.get("id") is not None:
                venues_to_upsert.append(mapped_venue)

            teams_to_upsert.append(map_team(team_payload=team_payload, venue_id=mapped_venue.get("id")))

        venue_count = upsert_many(session, "venue", venues_to_upsert)
        team_count = upsert_many(session, "team", teams_to_upsert)
        _ok(f"{venue_count} venue(s) e {team_count} time(s) preparados para persistencia.")

        # 3) Players + player_statistics
        players_to_upsert: List[Dict[str, Any]] = []
        stats_to_upsert: List[Dict[str, Any]] = []

        for team_item in teams_to_upsert:
            team_id = team_item.get("id")
            if team_id is None:
                continue

            print(f"[INFO] Extraindo jogadores do time_id={team_id}...")
            players = client.get_all_players_by_team(team_id=team_id, season=season)
            if not players:
                print(f"[AVISO] Nenhum jogador retornado para time_id={team_id}.")
                print("[INFO] Aguardando 10s antes do proximo time.")
                time.sleep(10)
                continue

            for row in players:
                player_payload = row.get("player") or {}
                player_id = player_payload.get("id")
                if player_id is None:
                    continue

                players_to_upsert.append(map_player(player_payload))

                for stats in (row.get("statistics") or []):
                    stats_to_upsert.append(
                        map_player_statistics(
                            stats_payload=stats,
                            player_id=player_id,
                            team_id=(stats.get("team") or {}).get("id") or team_id,
                            league_id=(stats.get("league") or {}).get("id") or league_id,
                            season=(stats.get("league") or {}).get("season") or season,
                        )
                    )

            print("[INFO] Aguardando 10s antes do proximo time.")
            time.sleep(10)

        player_count = upsert_many(session, "player", players_to_upsert)
        stats_count = upsert_many(session, "player_statistics", stats_to_upsert)
        _ok(f"{player_count} jogador(es) e {stats_count} estatistica(s) preparados para persistencia.")

        session.commit()
        _ok("Extracao finalizada e dados persistidos com sucesso.")
        return 0

    except SQLAlchemyError as exc:
        session.rollback()
        _fail(f"Erro de banco de dados: {exc}")
        return 1
    except KeyboardInterrupt:
        session.rollback()
        _fail("Execucao interrompida pelo usuario (KeyboardInterrupt).")
        return 1
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        _fail(f"Erro inesperado: {exc}")
        return 1
    finally:
        session.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extrai dados da API-Football e persiste no PostgreSQL")
    parser.add_argument("--league-id", type=int, default=71, help="ID da liga na API-Football")
    parser.add_argument("--season", type=int, required=True, help="Temporada a ser extraida")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    raise SystemExit(run_extraction(league_id=args.league_id, season=args.season))
