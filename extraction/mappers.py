from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional


def _to_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def map_league(league_payload: Dict[str, Any], country_payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": league_payload.get("id"),
        "name": league_payload.get("name"),
        "type": league_payload.get("type"),
        "logo": league_payload.get("logo"),
        "country_name": country_payload.get("name"),
        "country_code": country_payload.get("code"),
        "country_flag": country_payload.get("flag"),
    }


def map_season(league_id: int, season_payload: Dict[str, Any]) -> Dict[str, Any]:
    coverage = season_payload.get("coverage") or {}
    fixtures = coverage.get("fixtures") or {}

    return {
        "league_id": league_id,
        "year": season_payload.get("year"),
        "start_date": _to_date(season_payload.get("start")),
        "end_date": _to_date(season_payload.get("end")),
        "current": season_payload.get("current"),
        "coverage_fixtures_events": fixtures.get("events"),
        "coverage_fixtures_lineups": fixtures.get("lineups"),
        "coverage_fixtures_statistics_fixtures": fixtures.get("statistics_fixtures"),
        "coverage_fixtures_statistics_players": fixtures.get("statistics_players"),
        "coverage_injuries": coverage.get("injuries"),
        "coverage_odds": coverage.get("odds"),
        "coverage_players": coverage.get("players"),
        "coverage_predictions": coverage.get("predictions"),
        "coverage_standings": coverage.get("standings"),
        "coverage_top_assists": coverage.get("top_assists"),
        "coverage_top_cards": coverage.get("top_cards"),
        "coverage_top_scorers": coverage.get("top_scorers"),
    }


def map_venue(venue_payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": venue_payload.get("id"),
        "name": venue_payload.get("name"),
        "address": venue_payload.get("address"),
        "city": venue_payload.get("city"),
        "capacity": venue_payload.get("capacity"),
        "surface": venue_payload.get("surface"),
        "image": venue_payload.get("image"),
    }


def map_team(team_payload: Dict[str, Any], venue_id: Optional[int]) -> Dict[str, Any]:
    return {
        "id": team_payload.get("id"),
        "name": team_payload.get("name"),
        "code": team_payload.get("code"),
        "country": team_payload.get("country"),
        "founded": team_payload.get("founded"),
        "national": team_payload.get("national"),
        "logo": team_payload.get("logo"),
        "venue_id": venue_id,
    }


def map_player(player_payload: Dict[str, Any]) -> Dict[str, Any]:
    birth = player_payload.get("birth") or {}

    return {
        "id": player_payload.get("id"),
        "firstname": player_payload.get("firstname"),
        "lastname": player_payload.get("lastname"),
        "name": player_payload.get("name"),
        "age": player_payload.get("age"),
        "nationality": player_payload.get("nationality"),
        "height": player_payload.get("height"),
        "weight": player_payload.get("weight"),
        "injured": player_payload.get("injured"),
        "photo": player_payload.get("photo"),
        "birth_date": _to_date(birth.get("date")),
        "birth_place": birth.get("place"),
        "birth_country": birth.get("country"),
    }


def map_player_statistics(
    stats_payload: Dict[str, Any],
    player_id: int,
    team_id: int,
    league_id: int,
    season: int,
) -> Dict[str, Any]:
    games = stats_payload.get("games") or {}
    goals = stats_payload.get("goals") or {}
    shots = stats_payload.get("shots") or {}
    passes = stats_payload.get("passes") or {}
    tackles = stats_payload.get("tackles") or {}
    duels = stats_payload.get("duels") or {}
    dribbles = stats_payload.get("dribbles") or {}
    fouls = stats_payload.get("fouls") or {}
    cards = stats_payload.get("cards") or {}
    penalty = stats_payload.get("penalty") or {}
    substitutes = stats_payload.get("substitutes") or {}

    return {
        "player_id": player_id,
        "team_id": team_id,
        "league_id": league_id,
        "season": season,
        "position": games.get("position"),
        "number": games.get("number"),
        "captain": games.get("captain"),
        "rating": _to_decimal(games.get("rating")),
        "appearances": games.get("appearences"),
        "lineups": games.get("lineups"),
        "minutes": games.get("minutes"),
        "goals_total": goals.get("total"),
        "goals_assists": goals.get("assists"),
        "goals_conceded": goals.get("conceded"),
        "goals_saves": goals.get("saves"),
        "shots_total": shots.get("total"),
        "shots_on": shots.get("on"),
        "passes_total": passes.get("total"),
        "passes_key": passes.get("key"),
        "passes_accuracy": passes.get("accuracy"),
        "tackles_total": tackles.get("total"),
        "tackles_blocks": tackles.get("blocks"),
        "tackles_interceptions": tackles.get("interceptions"),
        "duels_total": duels.get("total"),
        "duels_won": duels.get("won"),
        "dribbles_attempts": dribbles.get("attempts"),
        "dribbles_success": dribbles.get("success"),
        "dribbles_past": dribbles.get("past"),
        "fouls_committed": fouls.get("committed"),
        "fouls_drawn": fouls.get("drawn"),
        "cards_yellow": cards.get("yellow"),
        "cards_yellowred": cards.get("yellowred"),
        "cards_red": cards.get("red"),
        "penalty_won": penalty.get("won"),
        "penalty_commited": penalty.get("commited"),
        "penalty_scored": penalty.get("scored"),
        "penalty_missed": penalty.get("missed"),
        "penalty_saved": penalty.get("saved"),
        "substitutes_in": substitutes.get("in"),
        "substitutes_out": substitutes.get("out"),
        "substitutes_bench": substitutes.get("bench"),
    }
