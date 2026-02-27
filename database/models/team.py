from sqlalchemy import Boolean, Column, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from database.base import Base


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    code = Column(Text)
    country = Column(Text)
    founded = Column(Integer)
    national = Column(Boolean)
    logo = Column(Text)

    venue_id = Column(Integer, ForeignKey("venues.id"))
    venue = relationship("Venue")
