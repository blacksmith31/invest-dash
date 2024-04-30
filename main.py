import operator
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2_fragments.fastapi import Jinja2Blocks
import logging
from pathlib import Path

from backend import db
from backend.helpers import (
    day_scores_compare,
    fmt_currency_word, 
    from_json,
    ts_to_str, 
    score_round, 
    fmt_currency
)
from backend.updater import update
from backend.symbols import symbol_update

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

BASE_PATH = Path(__file__).resolve().parent
templates = Jinja2Blocks(directory=str(BASE_PATH / "frontend/templates"))
templates.env.filters["from_json"] = from_json

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    trigger = CronTrigger(year='*', month='*', day='*',
                          day_of_week='mon-fri', hour='21',
                          minute='55', timezone="US/Eastern")
    symbol_trigger = CronTrigger(year='*', month='*', day='*',
                                 day_of_week='sat', hour='5',
                                 minute='0', timezone="US/Eastern")
    scheduler.add_job(update, trigger=trigger, name="Updater")
    scheduler.add_job(symbol_update, trigger=symbol_trigger, name="SymbolUpdate")
    scheduler.start()
    yield
    # do stuff at shutdown here
    
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.get("/", status_code=200, response_class=HTMLResponse)
def root(request: Request, limit: int = 10):
    data = db.select_latest_scores(limit)
    context = {"request": request,
               "data": data,
               "ts_to_str": ts_to_str,
               "round": score_round}
    block_name = None
    if request.headers.get("HX-Request"):
        block_name = "table"
    return templates.TemplateResponse("index.html",
                                      context,
                                      block_name=block_name)

@app.get("/chart_data", status_code=200, response_class=HTMLResponse)
def chart_data(request: Request, ticker: str = ''):
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
    block_name = None
    # if request.headers.get("HX-Request"):
    #     block_name = "chart"
    return templates.TemplateResponse("chart.html",
                                      context,
                                      block_name=block_name)

@app.get("/symbols_html", status_code=200, response_class=HTMLResponse)
def symbols_html(request: Request):
    data = db.view_symbol_hdr()
    context = {"request": request,
               "fmt_currency": fmt_currency_word,
               "data": data}
    return templates.TemplateResponse("view_symbol_hdr.html", context)

@app.get("/symbols_hdr", status_code=200, response_class=HTMLResponse)
def symbols_hdr(request: Request):
    data = db.view_symbol_hdr(limit=1000)
    context = {"request": request,
               "fmt_currency": fmt_currency,
               "data": data}
    return templates.TemplateResponse("view_symbol_hdr.html", context)

@app.get("/compare")
def compare(prev_days:int=7):
    d1 = db.select_prev_days_scores(limit=5, days=0)
    d2 = db.select_prev_days_scores(limit=5, days=prev_days)
    added, removed = day_scores_compare(d1, d2)
    return added, removed

@app.get("/view_scores", status_code=200, response_class=HTMLResponse)
def view_daily_scores(request: Request):
    data = db.view_daily_scores()
    col_names = list(data[0].keys())
    max_date = max(col_names[1:])
    print(f"max date {max_date}, in all: {all([max_date in row for row in data])}")
    sorted_data = sorted(data, key=operator.itemgetter(max_date), reverse=True)
    context = {"request": request,
               "col_names": col_names,
               "data": sorted_data}
    return templates.TemplateResponse("view_daily_scores.html", context)

@app.get("/symbols", status_code=200, response_class=JSONResponse)
def symbols():
    data = db.view_symbol_hdr()
    return data

@app.get("/top_symbols/", status_code=200, response_class=JSONResponse)
def top_n_symbols(n:int=1000):
    # http://localhost:8080/top_symbols/?n=10
    data = db.select_top_symbols_mcap(n)
    return data

@app.get("/tickers/close")
def get_tickers():
    result = db.select_sorted_closes()
    return result


@app.get("/tickers/{symbol}/close")
def get_ticker(symbol: str):
    result = db.select_ticker_closes(symbol)
    return result


@app.get("/tickers/{symbol}/scores")
def get_score(symbol: str):
    result = db.select_ticker_scores(symbol)
    return result



