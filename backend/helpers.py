from datetime import datetime, timedelta
import json
import math

def ts_to_str(ts: float):
    return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")

def days_ago_to_ts(days: int):
    return math.floor(datetime.timestamp(datetime.today() - timedelta(days=days)))

def list_compare(current, previous):
    added = [item for item in current if item not in previous]
    removed = [item for item in previous if item not in current]
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
    for v in [123, 1234, 12345, 123456, 1234567, 123456789, 1234567890, 123456789000, 1234567890000]:
        print(fmt_currency_word(v))
    # print(f"0 days ago: {days_ago_to_ts(0)}")
    # added, removed = list_compare([4, 5, 6, 1], [1, 2, 3, 4])
    # print(f"added: {added}, removed: {removed}")
