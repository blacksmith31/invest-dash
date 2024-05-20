from datetime import datetime
import pytest
from backend.dtz import Eastern
from jobs.trigger import ContinuousSubweekly
from jobs.resources import YFiChart
from jobs.jobs import TickerJob
from schemas.strategy import SROCStrategy

trigger = ContinuousSubweekly(day_of_week='thu', hour='4', minute='0', second='0', timezone='US/Eastern')
trigger1 = ContinuousSubweekly(day_of_week='thu', hour='4', minute='0', second='0', timezone='US/Eastern')
trigger2 = ContinuousSubweekly(day_of_week='mon-fri', hour='4', minute='0', second='0', timezone='US/Eastern')
trigger3 = ContinuousSubweekly(day_of_week='mon-fri', hour='*', minute='*', second='0', timezone='US/Eastern')

dts = [
    datetime(2024, 5, 16, 4, 0, 0, tzinfo=Eastern)
    ,datetime(2024, 5, 16, 4, 0, 0, tzinfo=Eastern)
    ,datetime(2024, 5, 16, 4, 0, 0, tzinfo=Eastern)
    ,datetime(2024, 5, 16, 0, 8, 0, tzinfo=Eastern)
]
resource = YFiChart()

strategy = SROCStrategy(scored_tickers=10, top_tickers=5, ema_window=13, roc_window=90)

jobs = [
    TickerJob(resource=resource, trigger=trigger, strategy=strategy, spread="day")
    ,TickerJob(resource=resource, trigger=trigger1, strategy=strategy, spread="week")
    ,TickerJob(resource=resource, trigger=trigger2, strategy=strategy, spread="week")
    ,TickerJob(resource=resource, trigger=trigger3, strategy=strategy, spread="day")
]

all_tickers = ["META", "BHVN", "AMD", "NVDA", "SMCI", "VRT", "DELL", "SE", "CEG", "NRG"]

expected_slices = [
    ["META", "BHVN", "AMD", "NVDA", "SMCI", "VRT", "DELL", "SE", "CEG", "NRG"]
    ,["META", "BHVN", "AMD", "NVDA", "SMCI", "VRT", "DELL", "SE", "CEG", "NRG"]
    ,["DELL", "SE"]
    ,["CEG"]
]

@pytest.mark.parametrize("job, dt, expected", list(zip(jobs, dts, expected_slices)))
def test__get_tickerslice(job, dt, expected) -> None:
    # (self, all_tickers: List[str], dt: datetime)
    # print(f"times per day: {trigger.daily_executions}")
    assert job._get_tickerslice(all_tickers, dt) == expected

######################################################################################
data = [
    {"latest": None, "daycount": 0}
    ,{"latest": 1715344486, "daycount": 150}
    ,{"latest": 1715344486, "daycount": 5}
]
expected_days = [
    126
    ,9
    ,126
]
@pytest.mark.parametrize("job, latest_data, expected", list(zip(jobs, data, expected_days)))
def test__calc_query_days(job, latest_data, expected):
    assert job._calc_query_days(latest_data) == expected

