from sqlalchemy import Boolean, Column, Integer, Text
from sqlalchemy.orm import relationship

from database.base import Base


class League(Base):
    __tablename__ = "leagues"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    type = Column(Text)
    logo = Column(Text)
    country_name = Column(Text)
    country_code = Column(Text)
    country_flag = Column(Text)

    seasons = relationship("Season", back_populates="league")
