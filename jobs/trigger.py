from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.cron.expressions import AllExpression, RangeExpression
from collections.abc import Generator
from datetime import datetime, timedelta
from typing import List

from backend.dtz import Eastern
from schemas.schemas import Spread

class ContinuousSubweekly(CronTrigger):
    def __init__(self, year='*', month='*', day='*', week=None, 
                 day_of_week=None, hour=None, minute=None, second=None, 
                 timezone=None):
        super().__init__(year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour,
                 minute=minute, second=second, start_date=None, end_date=None, timezone=timezone,
                 jitter=None)

    @property
    def daily_executions(self) -> int:
        hour_count = 1
        minute_count = 1
        second_count = 1
        for field in self.fields:
            exprs = field.expressions
            if field.name == 'hour':
                hour_count = self._expr_count(exprs, 'hour')
            elif field.name == "minute":
                minute_count = self._expr_count(exprs, "minute")
            elif field.name == "second":
                second_count = self._expr_count(exprs, "second")
        return hour_count * minute_count * second_count

    @property
    def days_per_week(self) -> int:
        day_count = 1
        for field in self.fields:
            exprs = field.expressions
            if field.name == 'day_of_week':
                day_count = self._expr_count(exprs, 'day_of_week')
        return day_count

    @property
    def weekly_executions(self) -> int:
        return self.days_per_week * self.daily_executions

    def exec_times(self, dt: datetime, spread: Spread) -> Generator[datetime, None, None]:
        starttime = self.get_next_fire_time(None, dt - timedelta(seconds=5))
        if not isinstance(starttime, datetime):
            raise Exception("No more executions")
        starttime = starttime.replace(hour=0, minute=0, second=0, microsecond=0) 
        endtime = starttime + timedelta(hours=23, minutes=59, seconds=59)
        if spread == "week":
            starttime = starttime - timedelta(days=starttime.weekday())
            endtime = starttime + timedelta(days=6, hours=23, minutes=59, seconds=59)
        # print(f"starttime: {starttime}")
        # print(f"endtime: {endtime}")
        ptr = starttime
        count = 0
        while ptr < endtime and count < self.weekly_executions:
            ptr = self.get_next_fire_time(None, ptr)
            if not isinstance(ptr, datetime):
                return
            if ptr > endtime:
                return
            yield ptr
            ptr += timedelta(seconds=1)
            count += 1

    def week_exec_times(self):
        pass        

    def _expr_count(self, exprs: List[AllExpression], field_name: str) -> int:
        count = 0
        for expr in exprs:
            # sum the hours from each
            if isinstance(expr, RangeExpression):
            # if type(expr) is RangeExpression:
                # print(f"this is a RangeExpression")
                count += self._range_expr_counter(expr)
            elif type(expr) is AllExpression:
                # print(f"parent type all expression {expr}")
                count += self._all_expr_counter(expr, field_name)
            else:
                # WeekdayPositionExpression, LastDayOfMonthExpression
                raise TypeError(f"type {type(expr)} not handled")
        if count:
            return count
        return 0

    def _range_expr_counter(self, expr: RangeExpression) -> int:
        last = int(expr.last) if expr.last else 0
        first = int(expr.first) if expr.first else 0
        step = expr.step
        if last == first:
            # single value
            count = 1
        else:
            # range inclusive
            count = last - first + 1
            if step is not None:
                count = count // step
        # print(f"final range expr count: {count}")
        return count
    
    def _all_expr_counter(self, expr: AllExpression, field_name: str) -> int:
        qty_map = {"day_of_week": 7, "hour": 24, "minute": 60, "second": 60}
        count = qty_map[field_name]
        if expr.step:
            count //= expr.step
        return count

def test():
    t = ContinuousSubweekly(day_of_week='wed-thu', hour='15', timezone='US/Eastern')
    fields = t.fields 
    print(fields)
    for field in fields:
        print(field.expressions)
    print(t.get_next_fire_time(None, datetime.now()))
    print(f"Weekly: {t.weekly_executions}")
    for exec in t.exec_times("week"):
        print(f"yielded time: {exec}")
    print(f"Daily: {t.daily_executions}")
    for exec in t.exec_times("day"):
        print(f"yielded time: {exec}")
if __name__ == "__main__":
    test()


