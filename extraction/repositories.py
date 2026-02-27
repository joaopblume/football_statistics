from typing import Dict, Iterable

from sqlalchemy.orm import Session

from database.models import League, Player, PlayerStatistics, Season, Team, Venue


MODEL_MAP = {
    "league": League,
    "season": Season,
    "venue": Venue,
    "team": Team,
    "player": Player,
    "player_statistics": PlayerStatistics,
}


def upsert_one(session: Session, model_name: str, payload: Dict) -> None:
    model = MODEL_MAP[model_name]
    session.merge(model(**payload))


def upsert_many(session: Session, model_name: str, rows: Iterable[Dict]) -> int:
    total = 0
    for row in rows:
        upsert_one(session=session, model_name=model_name, payload=row)
        total += 1
    return total
