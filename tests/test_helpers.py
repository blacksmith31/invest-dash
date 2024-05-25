from datetime import datetime, date, timedelta
import pytest

from backend.helpers import dt_day_shift_ts 
from backend import db

# dt_day_shift_ts(dt:datetime, days: int):
in_dates = [datetime(2024, 5, 3, hour=6, minute=8, second=35)
            ,datetime(2024, 5, 3, hour=6, minute=8, second=35)
            ,datetime(2024, 5, 3, hour=6, minute=8, second=35)
            ,datetime(2024, 5, 3, hour=23, minute=8, second=35)]
days = [0, -1, -7, 0]#, -7]
expected = [1714743000, 1714656600, 1714138200, 1714829400]#, 1713447000]
@pytest.mark.parametrize("dt, days, expected", list(zip(in_dates, days, expected)))
def test_ts_day_shift(dt, days, expected):
    assert dt_day_shift_ts(dt, days) == expected

@pytest.mark.skip(reason="manual test")
def test_compare(window=7, days=7, limit=20):
    now = datetime.now() - timedelta(days=1)
    print(f"now: {now}")
    current_ts = dt_day_shift_ts(now, 0)
    print(f"current ts: {datetime.fromtimestamp(current_ts)}")
    curr_min_ts = dt_day_shift_ts(now, -1 * (window + 1))
    print(f"current min ts - {datetime.fromtimestamp(curr_min_ts)}")
    prev_max_dt = now - timedelta(days=days+2)
    print(f"prev max dt - {prev_max_dt}")
    prev_max_ts = dt_day_shift_ts(prev_max_dt, 0)
    print(f"prev max ts - {datetime.fromtimestamp(prev_max_ts)}")
    prev_min_ts = dt_day_shift_ts(prev_max_dt, -1 * (window + 1))
    print(f"prev min ts - {datetime.fromtimestamp(prev_min_ts)}")
    # current = db.select_prev_days_scores(limit=limit, min_ts=curr_min_ts, max_ts=current_ts)
    # past = db.select_prev_days_scores(limit=limit, min_ts=prev_min_ts, max_ts=prev_max_ts)

if __name__ == "__main__":
    test_compare()
