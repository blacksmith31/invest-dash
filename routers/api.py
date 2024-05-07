from datetime import datetime, timedelta
from fastapi import APIRouter
from operator import itemgetter
from typing import Dict, List

from backend.helpers import (
    dt_day_shift_ts,
    day_scores_compare
)
from backend import db
from schemas.schemas import Symbol, TickerDayScore

router = APIRouter(
    prefix="/api",
    tags=["api"]
)


@router.get("/symbols", response_model=List[Symbol])
def symbols(limit:int=1000):
    data = db.view_symbol_hdr(limit=limit)
    return data


@router.get(path="/changes", response_model=Dict[str, List[TickerDayScore]])
async def changes(limit:int=20, days:int=7, window:int=7):
    now = datetime.now()
    current_ts = dt_day_shift_ts(now, 0)
    curr_min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    prev_max_dt = now - timedelta(days=days+1)
    prev_max_ts = dt_day_shift_ts(prev_max_dt, 0)
    prev_min_ts = dt_day_shift_ts(prev_max_dt, -1 * (window + 1))
    current = db.select_prev_days_scores(limit=limit, min_ts=curr_min_ts, max_ts=current_ts)
    past = db.select_prev_days_scores(limit=limit, min_ts=prev_min_ts, max_ts=prev_max_ts)
    added, removed = day_scores_compare(current, past)
    return {"current": current, "past": past, "added": added, "removed": removed}

@router.get("/daily_scores")
async def view_daily_scores(days:int=7):
    data = db.view_daily_scores(days)
    col_names = list(data[0].keys())
    max_date = max(col_names[1:])
    print(f"max date {max_date}, in all: {all([max_date in row for row in data])}")
    sorted_data = sorted(data, key=itemgetter(max_date), reverse=True)
    return data
