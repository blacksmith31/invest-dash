
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

# from backend.updater import update
from backend.symbols import symbol_update
from jobs.jobs import TickerJob
from jobs.resources import YFiChart
from jobs.trigger import ContinuousSubweekly
from schemas.strategy import SROCStrategy

logging.basicConfig()
logger = logging.getLogger('apscheduler').setLevel(logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()

    ticker_trigger = ContinuousSubweekly(day_of_week='mon-fri', hour='*', minute='*', timezone='US/Eastern')
    resource = YFiChart()
    strategy = SROCStrategy(scored_tickers=1000, top_tickers=20, ema_window=13, roc_window=90)
    ticker_job = TickerJob(resource=resource, trigger=ticker_trigger, strategy=strategy, logger=logger, spread='day')

    # trigger = CronTrigger(year='*', month='*', day='*',
    #                     day_of_week='mon-fri', hour='21',
    #                     minute='55', timezone="US/Eastern")
    symbol_trigger = CronTrigger(year='*', month='*', day='*',
                                 day_of_week='sat', hour='5',
                                 minute='0', timezone="US/Eastern")

    # scheduler.add_job(update, trigger=trigger, name="Updater")
    scheduler.add_job(ticker_job.run, trigger=ticker_trigger, name="Updater_v2")
    scheduler.add_job(symbol_update, trigger=symbol_trigger, name="SymbolUpdate")
    scheduler.start()
    yield
    # do stuff at shutdown here
