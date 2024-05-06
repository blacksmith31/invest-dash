from pydantic import BaseModel, Field


class TickerDay(BaseModel):
    ticker: str = Field(max_length=7)
    ts: float

class TickerDayClose(TickerDay):
    close: float = Field(gt=0)

class TickerDayScore(TickerDay):
    score: float = Field(default=0, alias="sroc")

class TickerDayScoredClose(TickerDay):
    close: float = Field(gt=0)
    score: float = Field(default=0, alias="sroc")

class Symbol(BaseModel):
    symbol: str = Field(max_length=7)
    name: str
    mktcap: float
    country: str
    industry: str
    sector: str

class UserSymbol(Symbol):
    ### TODO Replace with tracking enum
    tracking: int
    owned: bool
