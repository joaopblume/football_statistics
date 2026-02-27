from sqlalchemy import Boolean, Column, ForeignKey, Integer, Numeric, Text

from database.base import Base


class PlayerStatistics(Base):
    __tablename__ = "player_statistics"

    player_id = Column(Integer, ForeignKey("players.id"), primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"), primary_key=True)
    league_id = Column(Integer, primary_key=True)
    season = Column(Integer, primary_key=True)

    position = Column(Text)
    number = Column(Integer)
    captain = Column(Boolean)
    rating = Column(Numeric(4, 2))

    appearances = Column(Integer)
    lineups = Column(Integer)
    minutes = Column(Integer)

    goals_total = Column(Integer)
    goals_assists = Column(Integer)
    goals_conceded = Column(Integer)
    goals_saves = Column(Integer)

    shots_total = Column(Integer)
    shots_on = Column(Integer)

    passes_total = Column(Integer)
    passes_key = Column(Integer)
    passes_accuracy = Column(Text)

    tackles_total = Column(Integer)
    tackles_blocks = Column(Integer)
    tackles_interceptions = Column(Integer)

    duels_total = Column(Integer)
    duels_won = Column(Integer)

    dribbles_attempts = Column(Integer)
    dribbles_success = Column(Integer)
    dribbles_past = Column(Integer)

    fouls_committed = Column(Integer)
    fouls_drawn = Column(Integer)

    cards_yellow = Column(Integer)
    cards_yellowred = Column(Integer)
    cards_red = Column(Integer)

    penalty_won = Column(Integer)
    penalty_commited = Column(Integer)
    penalty_scored = Column(Integer)
    penalty_missed = Column(Integer)
    penalty_saved = Column(Integer)

    substitutes_in = Column(Integer)
    substitutes_out = Column(Integer)
    substitutes_bench = Column(Integer)
