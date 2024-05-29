from datetime import datetime
import polars as pl
from backend import db

def pivot_daily_scores():
    data = db.select_tickers_scores(int(datetime.now().timestamp() - 14*86400))
    # data = data[:10]
    df = pl.from_dicts(data)
    df = df.with_columns(pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.date().alias("date"))
    df = df.pivot(index="ticker", columns="date", values="sroc")
    date_cols = [col for col in reversed(df.columns) if col.startswith(str(datetime.now().year))]
    df = df.with_columns([pl.coalesce(date_cols[idx:]) for idx in range(0, len(date_cols) - 1)])
    df = df.sort(date_cols[0], descending=True, nulls_last=True)
    return df
    
if __name__ == "__main__":
    print(pivot_daily_scores())

