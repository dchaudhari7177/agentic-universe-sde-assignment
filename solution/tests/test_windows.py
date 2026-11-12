from datetime import timedelta

import pytest

from solution.windows import CallingPolicy, DailyWindow

from .conftest import ist

REG = DailyWindow.parse("09:00", "21:00", None)
RENEW = CallingPolicy((DailyWindow.parse("10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"]), REG))
ZEN = CallingPolicy((DailyWindow.parse("09:30", "18:00", ["mon", "tue", "wed", "thu", "fri"]), REG))
LATE = CallingPolicy((DailyWindow.parse("08:00", "22:00", None), REG))  # wider than regulation
GUARD = timedelta(seconds=60)


def test_window_is_half_open():
    assert not RENEW.can_start(ist(2026, 11, 12, 9, 59, 59))
    assert RENEW.can_start(ist(2026, 11, 12, 10, 0))
    assert RENEW.can_start(ist(2026, 11, 12, 18, 59, 59))
    assert not RENEW.can_start(ist(2026, 11, 12, 19, 0))


def test_guard_stops_starts_just_before_close():
    assert RENEW.can_start(ist(2026, 11, 12, 18, 58, 59), GUARD)
    assert not RENEW.can_start(ist(2026, 11, 12, 18, 59, 0), GUARD)
    assert not RENEW.can_start(ist(2026, 11, 12, 18, 59, 30), GUARD)


def test_disallowed_day():
    assert not RENEW.can_start(ist(2026, 11, 15, 12, 0))  # Sunday
    assert not ZEN.can_start(ist(2026, 11, 14, 12, 0))    # Saturday


def test_regulatory_window_always_applies():
    assert not LATE.can_start(ist(2026, 11, 12, 8, 30))
    assert LATE.can_start(ist(2026, 11, 12, 9, 0))
    assert not LATE.can_start(ist(2026, 11, 12, 21, 30))


def test_timezone_is_ist_not_utc():
    # 05:00 UTC = 10:30 IST: inside the window although the UTC hour is not.
    from datetime import datetime, timezone
    assert RENEW.can_start(datetime(2026, 11, 12, 5, 0, tzinfo=timezone.utc))


@pytest.mark.parametrize("t, expected", [
    (ist(2026, 11, 12, 12, 0), ist(2026, 11, 12, 12, 0)),    # already open: now
    (ist(2026, 11, 12, 7, 0), ist(2026, 11, 12, 10, 0)),     # before open: today's open
    (ist(2026, 11, 12, 19, 30), ist(2026, 11, 13, 10, 0)),   # after close: tomorrow
    (ist(2026, 11, 14, 19, 30), ist(2026, 11, 16, 10, 0)),   # Sat evening: skip Sunday
])
def test_next_start_renewals(t, expected):
    assert RENEW.next_start(t) == expected


def test_next_start_zen_skips_weekend():
    assert ZEN.next_start(ist(2026, 11, 13, 18, 0)) == ist(2026, 11, 16, 9, 30)


def test_next_start_respects_guard():
    assert RENEW.next_start(ist(2026, 11, 12, 18, 59, 30), GUARD) == ist(2026, 11, 13, 10, 0)


def test_window_must_not_cross_midnight():
    with pytest.raises(ValueError):
        DailyWindow.parse("22:00", "02:00", None)
