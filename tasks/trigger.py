from datetime import datetime
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.cron.expressions import AllExpression, RangeExpression, WeekdayRangeExpression

class ContinuousSubweekly(CronTrigger):
    def __init__(self, year='*', month='*', day='*', week=None, 
                 day_of_week=None, hour=None, minute=None, second=None, 
                 timezone=None):
        super().__init__(year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour,
                 minute=minute, second=second, start_date=None, end_date=None, timezone=timezone,
                 jitter=None)

    @property
    def daily_executions(self):
        hour_count = 0
        minute_count = 0
        second_count = 0
        for field in self.fields:
            # count hours
            if field.name == 'hour':
                print(f"Found hour field")
                # get each expression if more than 1
                exprs = field.expressions
                for expr in exprs:
                    # sum the hours from each
                    hour_count += self._expr_count(expr)
                    print(f"hour count: {hour_count}")
            # count minutes
            # count seconds
            # seconds * minutes * hours
        return hour_count

    def get_fields(self):
        return self.fields
    
    def _expr_count(self, expr: AllExpression) -> int:
        if isinstance(expr, RangeExpression):
            print(f"this is a RangeExpression")
            last = int(expr.last) if expr.last else 0
            first = int(expr.first) if expr.first else 0
            step = expr.step
            count: int
            if last == first:
                # single value
                count = 1
            else:
                # range inclusive
                count = last - first + 1
                if step is not None:
                    count = count // step
            print(f"final count: {count}")
        else:
            count = 0
        return count

def test():
    t = ContinuousSubweekly(day_of_week='sat', hour='*/2', minute='10-50/5', timezone='US/Eastern')
    fields = t.get_fields() 
    print(fields)
    for field in fields:
        print(field.expressions)
    print(t.get_next_fire_time(None, datetime.now()))
    print(f"Daily: {t.daily_executions}")

if __name__ == "__main__":
    test()


