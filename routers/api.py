from collections.abc import Mapping
from datetime import datetime, timedelta
from fastapi import APIRouter
from operator import itemgetter
import polars as pl
from typing import Dict, List

from fastapi.responses import JSONResponse

from backend.helpers import (
    dt_day_shift_ts,
    day_scores_compare
)
from backend import db
from schemas.schemas import Symbol, TickerDayClose, TickerDayScore

router = APIRouter(
    prefix="/api",
    tags=["api"],
    default_response_class=JSONResponse
)

@router.get("/tickers/scores", response_model=List[TickerDayScore])
async def get_tickers_score(window:int=7, limit:int=100):
    now = datetime.now()
    current_ts = dt_day_shift_ts(now, 0)
    curr_min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    data = db.select_prev_days_scores(limit, curr_min_ts, current_ts)
    return data

@router.get(path="/changes",  response_model=Mapping[str, List[TickerDayScore]])
async def changes(limit:int=20, days:int=7, window:int=7):
    now = datetime.now() - timedelta(days=1)
    current_ts = dt_day_shift_ts(now, 0)
    min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    current_list = db.select_prev_days_scores(limit=limit, max_ts=current_ts, min_ts=min_ts)
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
    added = [day.model_dump(by_alias=True) for day in added]
    removed = [day.model_dump(by_alias=True) for day in removed]
    return {"added": added, "removed": removed}

@router.get("/symbols/header", response_model=List[Symbol])
def symbols(limit:int=1000):
    data = db.view_symbol_hdr(limit=limit)
    return data

@router.get("/symbols/names")
def symbol_names(limit:int=1000):
    # http://localhost:8080/top_symbols/?n=10
    data = db.select_top_symbols_mcap(limit)
    return data

@router.get("/daily_scores")
async def view_daily_scores(days:int=7):
    data = db.view_daily_scores(days)
    col_names = list(data[0].keys())
    max_date = max(col_names[1:])
    print(f"max date {max_date}, in all: {all([max_date in row for row in data])}")
    sorted_data = sorted(data, key=itemgetter(max_date), reverse=True)
    return sorted_data

@router.get("/daily_scores_2")
async def pivot_daily_scores(days:int=7):
    data = db.select_tickers_scores(int(datetime.now().timestamp() - (days+1)*86400))
    # data = data[:10]
    df = pl.from_dicts(data)
    df = df.with_columns(pl.from_epoch(pl.col("timestamp"), time_unit="s").dt.date().alias("date"))
    df = df.pivot(index="ticker", columns="date", values="sroc")
    with pl.Config(tbl_rows=20):
        print(df.head(20))
    date_cols = [col for col in reversed(df.columns) if col.startswith(str(datetime.now().year))]
    df = df.with_columns([pl.coalesce(date_cols[idx:]) for idx in range(0, len(date_cols) - 1)])
    df = df.sort(date_cols[0], descending=True, nulls_last=True)
    data = df.to_dicts()
    return data

@router.get("/tickers/close", response_model=List[TickerDayClose])
def get_tickers_close():
    result = db.select_sorted_closes()
    return result


@router.get("/tickers/{symbol}/close", response_model=List[TickerDayClose])
def get_ticker(symbol: str):
    result = db.select_ticker_closes(symbol)
    return result


@router.get("/tickers/{symbol}/scores", response_model=List[TickerDayScore])
def get_score(symbol: str):
    result = db.select_ticker_scores(symbol)
    return result



