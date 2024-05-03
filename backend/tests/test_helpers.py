from datetime import datetime, date
import pytest

from backend.helpers import dt_day_shift_ts 

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
