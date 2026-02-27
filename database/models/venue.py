from sqlalchemy import Column, Integer, Text

from database.base import Base


class Venue(Base):
    __tablename__ = "venues"

    id = Column(Integer, primary_key=True)
    name = Column(Text)
    address = Column(Text)
    city = Column(Text)
    capacity = Column(Integer)
    surface = Column(Text)
    image = Column(Text)
