from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class TrackingStatus(Enum):
    AutoUntracked = 0
    AutoTracked = 1
    ManualUntracked = 2
    ManualTracked = 3
    

class TickerDay(BaseModel):
    ticker: str = Field(max_length=7)
    timestamp: int


class TickerDayClose(TickerDay):
    close: float = Field(gt=0)


class TickerDayScore(TickerDay):
    score: Optional[float] = Field(default=None, alias="sroc")


class TickerDayScoredClose(TickerDay):
    close: float = Field(gt=0)
    score: float = Field(default=0, alias="sroc")


class Symbol(BaseModel):
    symbol: str = Field(max_length=7)
    name: str
    mktcap: int = Field(alias="marketCap")
    country: str
    industry: str
    sector: str


class UserSymbol(Symbol):
    tracking: TrackingStatus = TrackingStatus.AutoUntracked
    owned: bool


