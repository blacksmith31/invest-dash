import pytest
from tasks.trigger import ContinuousSubweekly


triggers = [
    ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='1-10', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='1,10', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='1-10/2', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='0', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*/2', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='1,5,12,37', second='0', timezone='US/Eastern')
]

expected_daily = [
    86400
    ,1440
    ,14400
    ,2880
    ,7200
    ,24
    ,720
    ,96
]

@pytest.mark.parametrize("trigger, expected_daily", list(zip(triggers, expected_daily)))
def test_daily_executions(trigger, expected_daily) -> None:
    assert trigger.daily_executions == expected_daily
