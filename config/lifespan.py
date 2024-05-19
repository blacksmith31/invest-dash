
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from contextlib import asynccontextmanager
from fastapi import FastAPI

from backend.updater import update
from backend.symbols import symbol_update

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
