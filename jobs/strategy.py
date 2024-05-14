
from abc import ABC, abstractmethod
# from dataclasses import dataclass
from pydantic.dataclasses import dataclass
import polars as pl
from typing import List

from backend.db import select_ticker_closes

@dataclass
class StrategyBase(ABC):
    scored_tickers: int
    top_tickers: int

    @abstractmethod
    def score(self):
        pass

@dataclass
class SROCStrategy(StrategyBase):
    """
    Smoothed Rate Of Change Momentum ranking
    """
    ema_window: int
    roc_window: int

    def score(self, ticker: str) -> List[dict]:
        result = select_ticker_closes(ticker)
        # print(f"ticker closes: {result}")
        df = pl.from_dicts(result)
        df = df.with_columns(df["close"].ewm_mean(span=self.ema_window).alias("ema"))
        df = df.with_columns((100.0 * (df["ema"] / df["ema"].shift(self.roc_window) - 1.0)).alias("sroc"))
        df = df.drop(["close", "ema"]).drop_nulls("sroc")
        return df.to_dicts()

strat = SROCStrategy(scored_tickers=10, top_tickers=5, ema_window=13, roc_window=90)
scores = strat.score('AAPL')
print(f"scores: {scores[-20:]}")


