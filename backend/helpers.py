from datetime import datetime, timedelta
import json
import math

from backend.dtz import Eastern

def ts_to_str(ts: float):
    return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")

def start_of_day(dt: datetime):
    return dt.replace(hour=9, minute=30, second=0, microsecond=0)

def days_ago_to_ts(days: int):
    day_start = start_of_day(datetime.now()).astimezone(Eastern)
    return math.floor(datetime.timestamp(day_start - timedelta(days=days)))

def dt_day_shift_ts(dt:datetime, days: int):
    dt_east = dt.astimezone(Eastern)
    day_start = start_of_day(dt_east)
    return math.floor(datetime.timestamp(day_start + timedelta(days=days)))

def day_scores_compare(current, previous):
    curr_tickers = [item["ticker"] for item in current]
    prev_tickers = [item["ticker"] for item in previous]
    added = [item for item in current if item["ticker"] not in prev_tickers]
    removed = [item for item in previous if item["ticker"] not in curr_tickers]
    return added, removed

def score_round(score: float):
    return round(score or 0, 2)

def from_json(s):
    return json.loads(s)

def fmt_currency(val):
    return '${:,.0f}'.format(float(val))

def fmt_currency_word(val):
    millnames = ['', 'Th', 'M', 'B', 'T']
    val = float(val)
    millidx = max(0, min(len(millnames)-1, 
                  int(math.floor(0 if val == 0 else math.log10(abs(val))/3))))
    if val > 1000000:
        return '${:,.2f}{}'.format(val / 10**(3 * millidx), millnames[millidx])
    else:
        return '${:,.2f}'.format(float(val))

if __name__ == "__main__":
    #print(ts_to_str(1695758401))
    # for v in [123, 1234, 12345, 123456, 1234567, 123456789, 1234567890, 123456789000, 1234567890000]:
    #     print(fmt_currency_word(v))
    zero_day = days_ago_to_ts(0)
    max_ts = days_ago_to_ts(7)
    print(f"7 days ago: {max_ts}")
    print(f"14 days ago: {ts_day_shift(max_ts, 7)}")
    # added, removed = list_compare([4, 5, 6, 1], [1, 2, 3, 4])
    # print(f"added: {added}, removed: {removed}")
