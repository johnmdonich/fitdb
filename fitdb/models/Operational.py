from datetime import date

from sqlmodel import SQLModel, Field


class Users(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    username: str
    email: str
    first_name: str
    last_name: str
    dob: date
    gender: str

    class Config:
        orm_mode = True


class Bikes(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    brand: str
    model: str
    year: int

    class Config:
        orm_mode = True


class Sensors(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    brand: str
    model: str
    description: str

    class Config:
        orm_mode = True
