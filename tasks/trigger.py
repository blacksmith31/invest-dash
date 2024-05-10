from datetime import datetime
from apscheduler.triggers.cron import CronTrigger

class ContinuousSubweekly(CronTrigger):
    def __init__(self, year='*', month='*', day='*', week=None, 
                 day_of_week=None, hour=None, minute=None, second=None, 
                 timezone=None):
        super().__init__(year=year, month=month, day=day, week=week, day_of_week=day_of_week, hour=hour,
                 minute=minute, second=second, start_date=None, end_date=None, timezone=timezone,
                 jitter=None)

    @property
    def daily_executions(self):
        for field in self.fields:
            # count hours
            if field.name == 'hour':
                # get each expression if more than 1
                # sum the hours from each
                get_expr_count()
            # count minutes
            # count seconds
            # seconds * minutes * hours

    def get_fields(self):
        return self.fields

def test():
    t = ContinuousSubweekly(day_of_week='sat', hour='2', minute='0', timezone='US/Eastern')
    fields = t.get_fields() 
    print(fields)
    for field in fields:
        print(field.expressions)
    print(t.get_next_fire_time(None, datetime.now()))

if __name__ == "__main__":
    test()


