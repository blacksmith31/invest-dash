from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, List

from schemas.schemas import Symbol, TickerDayClose
from backend.db import (
    update_prev_pos,
    insert_update_sym_hdr,
    select_top_symbols_mcap,
    insert_closes_many,
    update_sroc_many,
    select_max_ticker_ts
)

if TYPE_CHECKING:
    from .trigger import ContinuousSubweekly
    from .resources import ResourceBase
    from logging import Logger
    from apscheduler.triggers.cron import CronTrigger

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
    def update(self):
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

    def update(self):
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
                 strategy=None, 
                 logger: Logger | None=None
                 ):
        self.resource = resource
        self.trigger = trigger
        self.strategy = strategy
        self.logger = logger
        self.tickers = self.pre_fetch()

    def pre_fetch(self) -> List[str]:
        # num top symbols should come from strrategy
        # ticker_slice should come from trigger, qty
        # min_ts comes from strategy
        db_tickers = [row["symbol"] for row in select_top_symbols_mcap(n=1000)]
        tickers = self._get_tickerslice(db_tickers)
        ######
        # This is for pruning -- Different Job?
        # min_ts = datetime.timestamp(datetime.now() - timedelta(days=MAX_DAYS))
        return tickers

    def fetch(self, ticker, days):
        # dont use current day return as timestamp will be off
        ticker_data = self.resource.get(ticker, days)[:-1]
        return ticker_data

    def validate(self, ticker_data: List[dict]) -> List[TickerDayClose]:
        validated = []
        for row in ticker_data:
            try:
                validated.append(TickerDayClose.model_validate(row))
            except:
                msg = f"validation failed for {row}"
                self.logger.warning(msg, exc_info=True) if self.logger else print(msg)
                continue
        return validated

    def post_fetch(self, ticker):
        # calc sroc for ticker and update sroc table
        sroc_data = calc_sroc(ticker)
        return sroc_data


    def save(self, ticker_data, sroc_data):
        insert_closes_many(ticker_data)
        update_sroc_many(sroc_data)

    def update(self):
        for ticker in self.tickers:
            # calc days worth of data to query based on what is saved in db
            # logger.info(f"{datetime.now()}:: Update: {ticker}")
            query_days = self._calc_query_days(ticker)
            if query_days < 1:
                continue
            ticker_data = self.fetch(ticker, query_days)
            validated = self.validate(ticker_data)
            sroc_data = self.post_fetch(ticker)
            self.save(validated, sroc_data)

        sleep_time = random.randrange(1, 10)
        logger.info(f"sleep: {sleep_time}")
        time.sleep(sleep_time)

    def _get_tickerslice(self, all_tickers: List[str]) -> List[str]:
        # self.trigger.daily_executions
        # self.trigger.days_per_week
        # self.trigger.weekly_executions

        # weekday(): mon=0, sun=6
        wkday = datetime.now().weekday()
        tkrs_per_day = round(len(all_tickers) / 5)
        sl_start = wkday * tkrs_per_day
        if wkday == 4:
            # friday just goes to the end
            return all_tickers[sl_start:]
        sl_end = sl_start + tkrs_per_day
        return all_tickers[sl_start: sl_end]

    def _calc_query_days(self, ticker: str, min_days: int = QUERY_DAYS) -> int:
        latest_data = select_max_ticker_ts(ticker)[0]
        latest, daycount = latest_data['latest'], latest_data['daycount']

        logger.info(f"{ticker}: latest: {latest}, daycount: {daycount}")

        if latest is None:
            query_days = min_days
            logger.info(f"no data, query days = {query_days}")
        else:
            days_since_latest = int((datetime.now().timestamp() - latest)/86400)
            logger.info(f"days_since: {days_since_latest}")
            if days_since_latest + daycount < min_days or days_since_latest > min_days:
                query_days = min_days
                logger.info(f"not enough data, query days = {query_days}")
            elif days_since_latest == 0:
                logger.info("Zero days, continuing")
                query_days = 0
            else:
                query_days = days_since_latest
                logger.info(f"recent data, query days = {query_days}")
        return query_days
