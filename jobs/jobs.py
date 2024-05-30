from abc import ABC, abstractmethod
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from logging import Logger
import random
import time
from typing import List

from backend.db import (
    update_prev_pos,
    insert_update_sym_hdr,
    select_top_symbols_mcap,
    insert_closes_many,
    update_sroc_many,
    select_max_ticker_ts,
    select_ticker_closes
)
from backend.dtz import Eastern
from jobs.trigger import ContinuousSubweekly
from jobs.resources import ResourceBase
from schemas.schemas import Symbol, TickerDayClose, Spread
from schemas.strategy import StrategyBase

class JobBase(ABC):

    def pre_fetch(self):
        raise NotImplementedError

    def fetch(self):
        raise NotImplementedError

    def post_fetch(self):
        raise NotImplementedError

    def save(self):
        raise NotImplementedError

    @abstractmethod
    def run(self):
        pass


class SymbolJob(JobBase):
    
    def __init__(self, 
                 resource: ResourceBase, 
                 trigger: CronTrigger, 
                 strategy=None, 
                 logger: Logger | None=None
                 ):
        self.resource = resource
        self.trigger = trigger
        self.strategy = strategy
        self.logger = logger

    def pre_fetch(self):
        update_prev_pos()

    def fetch(self) -> List[dict]:
        return self.resource.get()
    
    def validate(self, syms: List[dict]) -> List[Symbol]:
        updated = []
        # keys = ["symbol", "name", "marketCap", "country", "industry", "sector"]
        for sym in syms: 
            try:
                updated.append(Symbol.model_validate(sym))
            except:
                msg = f"Symbol Update| input symbol: {sym['symbol']}, mktcap: {sym['marketCap']}"
                self.logger.warning(msg, exc_info=True) if self.logger else print(msg)
                continue
        return updated

    def run(self):
        # pre_fetch
        self.pre_fetch()
        # fetch
        raw_symbols = self.fetch()
        # validate
        models = self.validate(raw_symbols)
        # save
        insert_update_sym_hdr(models)

class TickerJob(JobBase):

    def __init__(self, 
                 resource: ResourceBase, 
                 trigger: ContinuousSubweekly, 
                 strategy: StrategyBase, 
                 logger: Logger | None=None,
                 spread: Spread = 'week'
                 ):
        self.resource = resource
        self.trigger = trigger
        self.strategy = strategy
        self.logger = logger
        self.spread = spread

    def pre_fetch(self, dt:datetime) -> List[str]:
        db_tickers = [row["symbol"] for row in select_top_symbols_mcap(n=self.strategy.scored_tickers)]
        tickers = self._get_tickerslice(db_tickers, dt)
        ######
        # This is for pruning -- Different Job?
        # min_ts = datetime.timestamp(datetime.now() - timedelta(days=MAX_DAYS))
        return tickers

    def fetch(self, ticker, days):
        # dont use current day return as timestamp will be off
        ticker_data = self.resource.get(ticker, days)[:-1]
        return ticker_data

    def validate(self, ticker_data: List[dict]) -> List[TickerDayClose]:
        ### could be made generic to validate closes and scores...
        validated = []
        for row in ticker_data:
            try:
                validated.append(TickerDayClose.model_validate(row))
            except:
                msg = f"validation failed for {row}"
                self.logger.warning(msg, exc_info=True) if self.logger else print(msg)
                continue
        return validated

    def post_fetch(self, validated: List[TickerDayClose]) -> List[dict]:
        # calc sroc for ticker and update sroc table
        ### type of indicator to calculate should come from strategy?
        ### score calculation should be a method of the strategy
        dumped = [day.model_dump() for day in validated]
        sroc_data = self.strategy.score(dumped)
        return sroc_data


    def save_ticker_days(self, ticker_data):
        rows = insert_closes_many(ticker_data)
        self.logger.info(f"inserted {rows} rows for close data")

    def save_scores(self, sroc_data):
        rows = update_sroc_many(sroc_data)
        self.logger.info(f"inserted {rows} rows for score data")

    def run(self):
        tickers = self.pre_fetch(datetime.now(tz=Eastern))
        if not tickers:
            msg = f"No tickers found at this execution time"
            self.logger.info(msg) if self.logger else print(msg)
        for i, ticker in enumerate(tickers):
            msg = f"Updating Ticker: {ticker} | index {i}/{len(tickers)}"
            self.logger.info(msg) if self.logger else print(msg)
            latest_data = select_max_ticker_ts(ticker)
            query_days = self._calc_query_days(latest_data)
            if query_days < 1:
                continue
            ticker_data = self.fetch(ticker, query_days)
            if not ticker_data:
                continue
            validated = self.validate(ticker_data)
            self.save_ticker_days(validated)
            ticker_history = select_ticker_closes(ticker)
            validated_history = [TickerDayClose.model_validate(row) for row in ticker_history]
            sroc_data = self.post_fetch(validated_history)
            self.save_scores(sroc_data)
            if (i+1) < len(tickers):
                sleep_time = random.randrange(1, 10)
                msg = f"sleep: {sleep_time}"
                self.logger.info(msg) if self.logger else print(msg)
                time.sleep(sleep_time)

    def _get_tickerslice(self, all_tickers: List[str], dt: datetime) -> List[str]:
        match self.spread:
            case 'day':
                executions = self.trigger.daily_executions
            case 'week':
                executions = self.trigger.weekly_executions
            case _:
                return []
        tickers_per_exec = len(all_tickers) // executions
        if tickers_per_exec == 0:
            tickers_per_exec = 1
        for i, et in enumerate(self.trigger.exec_times(dt, self.spread)):
            if i >= executions:
                msg = f"Completed {i} executions for the {self.spread}"
                self.logger.info(msg) if self.logger else print(msg)
                return []
            delta = abs((et - dt).total_seconds())
            if delta < 30:
                msg = f"slicing at execution index {i}/{executions}, with {tickers_per_exec} ticker(s) per execution"
                self.logger.info(msg) if self.logger else print(msg)
                sl_start = i * tickers_per_exec
                if i + 1 == executions:
                    return all_tickers[sl_start:]
                return all_tickers[sl_start:sl_start + tickers_per_exec]
        return []

    def _calc_query_days(self, latest_data: dict[str, int]) -> int:
        latest, daycount = latest_data['latest'], latest_data['daycount']
        if latest is not None and daycount is not None:
            msg = f"latest: {datetime.fromtimestamp(latest)}, daycount: {daycount}"
            self.logger.info(msg) if self.logger else print(msg)

        if latest is None:
            query_days = self.strategy.query_days
            msg = f"no data, query days = {query_days}"
            self.logger.debug(msg) if self.logger else print(msg)
        else:
            days_since_latest = int((datetime.now().timestamp() - latest)/86400)
            if days_since_latest == 0:
                query_days = 0
                msg = "Zero days since latest, continuing"
            elif days_since_latest + daycount >= self.strategy.query_days:
                query_days = days_since_latest
                msg = f"recent data, query days = {query_days}"
            else:
                query_days = self.strategy.query_days
                msg = f"not enough data, query days = {query_days}"
            self.logger.debug(msg) if self.logger else print(msg)
        return query_days

def test():
    t = ContinuousSubweekly(hour='*', minute='58', second='0')
    now = datetime.now(tz=Eastern)
    print(f"curr time eastern: {now}")
    for et in t.exec_times(now, "day"):
        print(f"exec time: {str(et)}")
        print(abs((et - datetime.now(tz=Eastern)).total_seconds()))
if __name__ == "__main__":
    test()
