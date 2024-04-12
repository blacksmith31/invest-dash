from datetime import datetime
import json


def ts_to_str(ts: float):
    return datetime.fromtimestamp(ts).strftime("%Y/%m/%d %H:%M:%S")


def score_round(score: float):
    return round(score or 0, 2)


def from_json(s):
    return json.loads(s)

def fmt_currency(val):
    return '${:,.0f}'.format(float(val))

if __name__ == "__main__":
    #print(ts_to_str(1695758401))
    print(fmt_currency(1695758401))
