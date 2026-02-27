from sqlalchemy import Boolean, Column, Date, Integer, Text

from database.base import Base


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True)
    firstname = Column(Text)
    lastname = Column(Text)
    name = Column(Text)
    age = Column(Integer)
    nationality = Column(Text)
    height = Column(Text)
    weight = Column(Text)
    injured = Column(Boolean)
    photo = Column(Text)

    birth_date = Column(Date)
    birth_place = Column(Text)
    birth_country = Column(Text)
