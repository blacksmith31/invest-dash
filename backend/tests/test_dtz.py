from datetime import datetime, timezone, time
from backend.dtz import HOUR, Eastern
import pytest

u0 = datetime(2016, 3, 13, 5, tzinfo=timezone.utc)
utc_times = [u0 + i*HOUR for i in range(4)]
eastern_std_to_dst_times = [u.astimezone(Eastern) for u in utc_times]
expected_times_std = [time(hour=i) for i in (0, 1, 3, 4)]
expected_tznames_std = ['EST', 'EST', 'EDT', 'EDT']

@pytest.mark.parametrize("eastern_dtime, expected_time_std", list(zip(eastern_std_to_dst_times, expected_times_std)))
def test_eastern_tz(eastern_dtime, expected_time_std) -> None:
    assert eastern_dtime.time() == expected_time_std

@pytest.mark.parametrize("eastern_dtime, expected_tzname_std", list(zip(eastern_std_to_dst_times, expected_tznames_std)))
def test_eastern_tzname(eastern_dtime, expected_tzname_std) -> None:
    assert eastern_dtime.tzname() == expected_tzname_std


u0 = datetime(2016, 11, 6, 4, tzinfo=timezone.utc)
utc_times = [u0 + i*HOUR for i in range(4)]
eastern_dst_to_std_times = [u.astimezone(Eastern) for u in utc_times]
expected_times_dst = [time(hour=i) for i in (0, 1, 1, 2)]
expected_tznames_dst = ['EDT', 'EDT', 'EST', 'EST']


@pytest.mark.parametrize("eastern_dtime, expected_time_dst", list(zip(eastern_dst_to_std_times, expected_times_dst)))
def test_eastern_tz_dst(eastern_dtime, expected_time_dst) -> None:
    assert eastern_dtime.time() == expected_time_dst

@pytest.mark.parametrize("eastern_dtime, expected_tzname_dst", list(zip(eastern_dst_to_std_times, expected_tznames_dst)))
def test_eastern_tzname_dst(eastern_dtime, expected_tzname_dst) -> None:
    assert eastern_dtime.tzname() == expected_tzname_dst
