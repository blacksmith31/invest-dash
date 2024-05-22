
from apscheduler.schedulers.background import BackgroundScheduler
from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging
from config.logs import setup_logging

from jobs.jobs import SymbolJob, TickerJob
from jobs.resources import SymbolsHeader, YFiChart
from jobs.trigger import ContinuousSubweekly
from schemas.strategy import SROCStrategy

schlogger = logging.getLogger('apsch')
exelogger = logging.getLogger('apexe')
joblogger = logging.getLogger('job')
setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler(logger=schlogger)

    ticker_trigger = ContinuousSubweekly(day_of_week='mon-fri', hour='*', minute='*', timezone='US/Eastern')
    resource = YFiChart()
    strategy = SROCStrategy(scored_tickers=1000, top_tickers=20, ema_window=13, roc_window=90)
    ticker_job = TickerJob(resource=resource, trigger=ticker_trigger, strategy=strategy, logger=joblogger, spread='day')

    symbol_trigger = ContinuousSubweekly(day_of_week='sat', hour='5', minute='27', timezone='US/Eastern')
    symbol_resource = SymbolsHeader()
    symbol_job = SymbolJob(resource=symbol_resource, trigger=symbol_trigger)

    scheduler.add_job(ticker_job.run, trigger=ticker_trigger, name="Updater_v2")
    scheduler.add_job(symbol_job.run, trigger=symbol_trigger, name="SymbolUpdate_v2")
    scheduler.start()
    scheduler._executors['default']._logger = exelogger
    yield
    # do stuff at shutdown here
