from enum import Enum
from typing import Optional, Annotated
from pydantic import BaseModel, Field, BeforeValidator


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

def str_to_int(v: str) -> int:
    if v == '':
        return 0
    try:
        return int(float(v))
    except ValueError:
        return -1

MktCap = Annotated[int, BeforeValidator(str_to_int)]
Sym = Annotated[str, BeforeValidator(lambda v: v.strip())]

class Symbol(BaseModel):
    symbol: Sym = Field(max_length=7)
    name: str
    mktcap: MktCap = Field(alias="marketCap")
    country: str
    industry: str
    sector: str


class UserSymbol(Symbol):
    tracking: TrackingStatus = TrackingStatus.AutoUntracked
    owned: bool


