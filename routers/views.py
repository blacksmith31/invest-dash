from datetime import datetime, timedelta
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
import operator
import polars as pl
from typing import Annotated, Dict, List

from jinja2_fragments.fastapi import Jinja2Blocks
from polars.type_aliases import RowTotalsDefinition

from backend import db
from backend.helpers import (
    compare,
    day_scores_compare,
    dt_day_shift_ts,
    fmt_currency_word, 
    ts_to_datestr,
    ts_to_str, 
    score_round, 
)
from schemas.schemas import Symbol, TickerDayClose, TickerDayScore
from config.settings import settings

templates = Jinja2Blocks(settings.TEMPLATE_DIR)

router = APIRouter(
    prefix="/views",
    tags=["views"]
)


@router.get("/main", status_code=200, response_class=HTMLResponse)
async def root(request:Request, window:int=7, limit:int=20):
    now = datetime.now() - timedelta(days=1)
    current_ts = dt_day_shift_ts(now, 0)
    min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    data = db.select_prev_days_scores_owned(limit, current_ts, min_ts)
    context = {"request": request,
               "data": data,
               "ts_to_datestr": ts_to_datestr,
               "round": score_round}
    block_name = None
    if request.headers.get("HX-Request"):
        block_name = "table"
    return templates.TemplateResponse("index.html",
                                      context,
                                      block_name=block_name)

@router.get("/chart_data", status_code=200, response_class=HTMLResponse)
async def chart_data(request: Request, ticker: str = ''):
    data = db.select_ticker_history(ticker)
    # print(f"request: {request.json()}")
    # labels = [Markup(ts_to_str(row["timestamp"])) for row in data]
    labels = [row["timestamp"] for row in data]
    closes = [round(row["close"] or 0, 2) for row in data]
    scores = [round(row["sroc"] or 0, 2) for row in data]
    context = {"request": request,
               "ticker": ticker,
               "labels": labels,
               "y1": closes,
               "y2": scores,
               "ts_to_str": ts_to_str}
    return templates.TemplateResponse("chart.html",
                                      context)

@router.put("/set_own/{ticker}", status_code=200, response_class=HTMLResponse)
async def set_own(request: Request, ticker: str):
    print(f"set own ticker: {ticker}")
    db.update_symbol_own(ticker)
    row = db.select_ticker_own(ticker)
    context = {"request": request,
               "row": row,
               "ts_to_datestr": ts_to_datestr,
               "round": score_round}
    return templates.TemplateResponse("tbl_ticker.html", context)


@router.get("/changes", status_code=200, response_class=HTMLResponse)
async def changes(request: Request, limit:int=20, days:int=7, window:int=7):
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
    added = [day.model_dump() for day in added]
    removed = [day.model_dump() for day in removed]
    context = {"request": request,
               "ts_to_datestr": ts_to_datestr,
               "added_symbols": added,
               "removed_symbols": removed}
    return templates.TemplateResponse("changes.html",
                                      context)

@router.get("/to_sell", status_code=200, response_class=HTMLResponse)
def view_to_sell(request: Request, window:int=7, limit:int=20):
    owned = db.select_owned_symbols()
    now = datetime.now() - timedelta(days=1)
    current_ts = dt_day_shift_ts(now, 0)
    min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    top = db.select_prev_days_scores_owned(limit, current_ts, min_ts)
    delta = compare(owned, "symbol", top, "ticker")
    context = {"request": request,
               "data": delta}
    return templates.TemplateResponse("view_to_sell.html", context)

@router.delete("/sell/{ticker}", status_code=200)
def sell(request: Request, ticker: str, window:int=7, limit:int=20):
    db.update_symbol_own(ticker)
    return

@router.get("/symbols", status_code=200, response_class=HTMLResponse)
def symbols_hdr(request: Request, limit:int=1000):
    data = db.view_symbol_hdr(limit=limit)
    context = {"request": request,
               "fmt_currency": fmt_currency_word,
               "data": data}
    return templates.TemplateResponse("view_symbol_hdr.html", context)

@router.post("/owned", status_code=200, response_class=HTMLResponse)
def add_own(request: Request, symbol: Annotated[str, Form()], window:int=7, limit:int=20):
    all_symbols = db.select_top_symbols_mcap()
    all_symbols = [row["symbol"] for row in all_symbols]
    if symbol in all_symbols:
        print(f"{symbol} FOUND IN ALL SYMBOLS")
        db.update_symbol_own(symbol)
    owned = db.select_owned_symbols()
    now = datetime.now() - timedelta(days=1)
    current_ts = dt_day_shift_ts(now, 0)
    min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    top = db.select_prev_days_scores_owned(limit, current_ts, min_ts)
    delta = compare(owned, "symbol", top, "ticker")
    context = {"request": request,
               "data": delta}
    return templates.TemplateResponse("view_to_sell.html", context)

@router.get("/view_scores", status_code=200, response_class=HTMLResponse)
def view_daily_scores(request: Request, days: int = 7):
    data = db.view_daily_scores(days)
    col_names = list(data[0].keys())
    max_date = max(col_names[1:])
    print(f"max date {max_date}, in all: {all([max_date in row for row in data])}")
    sorted_data = sorted(data, key=operator.itemgetter(max_date), reverse=True)
    context = {"request": request,
               "col_names": col_names,
               "data": sorted_data}
    return templates.TemplateResponse("view_daily_scores.html", context)

@router.get("/view_scores_2", status_code=200, response_class=HTMLResponse)
async def pivot_daily_scores(request: Request, days:int=7):
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
    col_names = list(data[0].keys())
    context = {"request": request,
               "col_names": col_names,
               "data": data}
    return templates.TemplateResponse("view_daily_scores.html", context)
