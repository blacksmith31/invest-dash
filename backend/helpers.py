from datetime import datetime, timedelta
import json
import math

def ts_to_str(ts: float):
    return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")

def days_ago_to_ts(days: int):
    return math.floor(datetime.timestamp(datetime.today() - timedelta(days=days)))

def list_compare(current, previous):
    new = [item for item in current if item not in previous]
    old = [item for item in previous if item not in current]
    return new, old

def score_round(score: float):
    return round(score or 0, 2)

def from_json(s):
    return json.loads(s)

def fmt_currency(val):
    return '${:,.0f}'.format(float(val))

if __name__ == "__main__":
    #print(ts_to_str(1695758401))
    # print(fmt_currency(1695758401))
    print(f"0 days ago: {days_ago_to_ts(0)}")
