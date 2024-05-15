import pytest
from jobs.trigger import ContinuousSubweekly


triggers = [
    ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='1-10', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='1,10', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='1-10/2', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='0', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='*/2', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='*', minute='1,5,12,37', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour=None, minute='1,5,12,37', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='1', minute=None, second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='*', hour='1', minute='5', second=None, timezone='US/Eastern')
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
    ,96
    ,60
    ,1
]

@pytest.mark.parametrize("trigger, expected_daily", list(zip(triggers, expected_daily)))
def test_daily_executions(trigger, expected_daily) -> None:
    assert trigger.daily_executions == expected_daily

#########################
triggers = [
    ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='mon-fri', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='sat', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='mon,wed,fri', hour='*', minute='*', second='*', timezone='US/Eastern')
]

expected_days = [
    7
    ,5
    ,1
    ,3
]

@pytest.mark.parametrize("trigger, expected_day", list(zip(triggers, expected_days)))
def test_day_executions(trigger, expected_day) -> None:
    assert trigger.days_per_week == expected_day

#################################
triggers = [
    ContinuousSubweekly(day_of_week='*', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='mon-fri', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='sat', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='mon,wed,fri', hour='*', minute='*', second='*', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='mon', hour='5', minute='5', second='0,30', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week='mon-fri', hour='5', minute='5', second='0', timezone='US/Eastern')
    ,ContinuousSubweekly(day_of_week=None, hour='5', minute='5', second='0', timezone='US/Eastern')
]

expected_weeklys = [
    604800
    ,432000
    ,86400
    ,259200
    ,2
    ,5
    ,7
]

@pytest.mark.parametrize("trigger, expected_weekly", list(zip(triggers, expected_weeklys)))
def test_weekly_executions(trigger, expected_weekly) -> None:
    assert trigger.weekly_executions == expected_weekly
