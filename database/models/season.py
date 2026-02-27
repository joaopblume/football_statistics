from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer
from sqlalchemy.orm import relationship

from database.base import Base


class Season(Base):
    __tablename__ = "seasons"

    league_id = Column(Integer, ForeignKey("leagues.id"), primary_key=True)
    year = Column(Integer, primary_key=True)

    start_date = Column(Date)
    end_date = Column(Date)
    current = Column(Boolean)

    coverage_fixtures_events = Column(Boolean)
    coverage_fixtures_lineups = Column(Boolean)
    coverage_fixtures_statistics_fixtures = Column(Boolean)
    coverage_fixtures_statistics_players = Column(Boolean)
    coverage_injuries = Column(Boolean)
    coverage_odds = Column(Boolean)
    coverage_players = Column(Boolean)
    coverage_predictions = Column(Boolean)
    coverage_standings = Column(Boolean)
    coverage_top_assists = Column(Boolean)
    coverage_top_cards = Column(Boolean)
    coverage_top_scorers = Column(Boolean)

    league = relationship("League", back_populates="seasons")
