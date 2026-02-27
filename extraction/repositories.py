from typing import Dict, Iterable, List, Sequence, Tuple

from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
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
    upsert_many(session=session, model_name=model_name, rows=[payload])


def upsert_many(session: Session, model_name: str, rows: Iterable[Dict]) -> int:
    model = MODEL_MAP[model_name]
    rows_list = list(rows)
    if not rows_list:
        return 0

    pk_cols = [col.name for col in sa_inspect(model).primary_key]
    deduped_rows = _dedupe_by_pk(rows_list, pk_cols)

    stmt = pg_insert(model).values(deduped_rows)

    update_cols = {
        col.name: getattr(stmt.excluded, col.name)
        for col in model.__table__.columns
        if col.name not in pk_cols
    }

    if update_cols:
        stmt = stmt.on_conflict_do_update(index_elements=pk_cols, set_=update_cols)
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=pk_cols)

    session.execute(stmt)
    return len(deduped_rows)


def _dedupe_by_pk(rows: List[Dict], pk_cols: Sequence[str]) -> List[Dict]:
    ordered: Dict[Tuple, Dict] = {}
    for row in rows:
        key = tuple(row.get(pk) for pk in pk_cols)
        if all(value is not None for value in key):
            ordered[key] = row
    return list(ordered.values())
