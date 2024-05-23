
from abc import ABC, abstractmethod
# from dataclasses import field
from dataclasses import dataclass, field
# from pydantic.dataclasses import dataclass
import polars as pl
from typing import List


@dataclass
class StrategyBase(ABC):
    scored_tickers: int
    top_tickers: int
    eval_days: int = field(init=False)
    query_days: int = field(init=False)
    prune_before_days: int = field(init=False)

    @abstractmethod
    def __post_init__(self):
        pass

    @abstractmethod
    def score(self, ticker_closes: List[dict]) -> List[dict]:
        pass

@dataclass
class SROCStrategy(StrategyBase):
    """
    Smoothed Rate Of Change Momentum ranking
    """
    ema_window: int
    roc_window: int

    def __post_init__(self):
        self.eval_days = max(self.ema_window, self.roc_window)
        self.query_days = ((self.eval_days // 5) * 2) + self.eval_days
        self.prune_before_days = self.query_days * 2

    def score(self, ticker_closes: List[dict]) -> List[dict]:
        # result = select_ticker_closes(ticker)
        # print(f"ticker closes: {result}")
        df = pl.from_dicts(ticker_closes)
        df = df.with_columns(df["close"].ewm_mean(span=self.ema_window).alias("ema"))
        df = df.with_columns((100.0 * (df["ema"] / df["ema"].shift(self.roc_window) - 1.0)).alias("sroc"))
        df = df.drop(["close", "ema"]).drop_nulls("sroc")
        return df.to_dicts()




