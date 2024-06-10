from datetime import datetime, timedelta
import polars as pl
from typing import List

from backend import db
from backend.helpers import dt_day_shift_ts, day_scores_compare
from schemas.schemas import TickerDayScore

def pivot_daily_scores():
    data = db.select_tickers_scores(int(datetime.now().timestamp() - 8*86400))
    # data = data[:10]
    df = pl.from_dicts(data)
    df = df.with_columns(pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.date().alias("date"))
    df = df.pivot(index="ticker", columns="date", values="sroc")
    with pl.Config(tbl_rows=20):
        print(df.head(20))
    date_cols = [col for col in reversed(df.columns) if col.startswith(str(datetime.now().year))]
    df = df.with_columns([pl.coalesce(date_cols[idx:]) for idx in range(0, len(date_cols) - 1)])
    df = df.sort(date_cols[0], descending=True, nulls_last=True)
    return df
    
def changes(limit:int=20, days:int=7, window:int=7):
    now = datetime.now()
    current_ts = dt_day_shift_ts(now, 0)
    min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    print(f"current ts: {current_ts}, min ts: {min_ts}")
    current_list = db.select_prev_days_scores(limit=limit, max_ts=current_ts, min_ts=min_ts)
    for day in current_list:
        print(f"curr day: {day}")
    return
    current_list = [TickerDayScore.model_validate(day) for day in current_list]
    added = []
    removed = []
    for day in range(1, days + 2):
        prev_ts = current_ts - 86400*day
        min_ts = prev_ts - 86400*8
        prev_list = db.select_prev_days_scores(limit, prev_ts, min_ts)
        prev_list = [TickerDayScore.model_validate(day) for day in prev_list]
        prevadd, prevrem = day_scores_compare(current_list, prev_list)
        added_syms = [day.ticker for day in added]
        removed_syms = [day.ticker for day in removed]
        added += [score for score in prevadd if score.ticker not in added_syms]
        removed += [score for score in prevrem if score.ticker not in removed_syms]
    added = [day.model_dump() for day in added]
    removed = [day.model_dump() for day in removed]

def score(ticker_closes: List[dict]) -> List[dict]:
    # result = select_ticker_closes(ticker)
    # print(f"ticker closes: {result}")
    df = pl.from_dicts(ticker_closes)
    df = df.with_columns(df["close"].ewm_mean(span=13).alias("ema"))
    df = df.with_columns((100.0 * (df["ema"] / df["ema"].shift(90) - 1.0)).alias("sroc"))
    df = df.drop(["close", "ema"]).drop_nulls("sroc")
    return df.to_dicts()

def prev_days_scores_owned(limit: int=20, window:int=7) -> list[dict]:
    now = datetime.now()
    current_ts = dt_day_shift_ts(now, 0)
    min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    data = db.select_prev_days_scores_owned(limit=limit, max_ts=current_ts, min_ts=min_ts)
    return data


if __name__ == "__main__":
    # df = pivot_daily_scores()
    # with pl.Config(tbl_rows=20):
    #     print(df.head(20))
    # changes()

    # ticker_history = db.select_ticker_closes('TYL')
    # print(ticker_history[-1])
    # scores = score(ticker_history)
    # print(scores)
    print(prev_days_scores_owned())

