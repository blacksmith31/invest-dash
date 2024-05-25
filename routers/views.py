from datetime import datetime, timedelta
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
import operator
from typing import Dict, List

from jinja2_fragments.fastapi import Jinja2Blocks

from backend import db
from backend.helpers import (
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
    data = db.select_prev_days_scores(limit, current_ts)
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

@router.get("/changes", status_code=200, response_class=HTMLResponse)
async def changes(request: Request, limit:int=20, days:int=7):
    now = datetime.now() - timedelta(days=1)
    current_ts = dt_day_shift_ts(now, 0)
    current_list = db.select_prev_days_scores(limit=limit, max_ts=current_ts)
    current_list = [TickerDayScore.model_validate(day) for day in current_list]
    added = []
    removed = []
    for day in range(1, days + 2):
        prev_ts = current_ts - 86400*day
        prev_list = db.select_prev_days_scores(limit, prev_ts)
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

@router.get("/symbols_hdr", status_code=200, response_class=HTMLResponse)
def symbols_hdr(request: Request, limit:int=1000):
    data = db.view_symbol_hdr(limit=limit)
    context = {"request": request,
               "fmt_currency": fmt_currency_word,
               "data": data}
    return templates.TemplateResponse("view_symbol_hdr.html", context)


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

