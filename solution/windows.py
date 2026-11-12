"""Calendar maths for calling windows (R1). Pure functions, no I/O, no clock.

All windows are local IST wall-clock times, half-open `[start, end)`, and do not cross midnight.
A campaign may dial only when it is inside *every* window in its policy: its own window and the
regulatory 09:00-21:00 window.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def _hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


@dataclass(frozen=True)
class DailyWindow:
    start: time
    end: time
    days: frozenset[str] | None  # None means every day

    @classmethod
    def parse(cls, start: str, end: str, days: list[str] | None) -> "DailyWindow":
        w = cls(_hhmm(start), _hhmm(end), frozenset(d.lower() for d in days) if days is not None else None)
        if w.start >= w.end:
            raise ValueError(f"window {start}-{end} must not cross midnight")
        if w.days is not None and not w.days <= set(DAYS):
            raise ValueError(f"unknown day names in {days}")
        return w

    def bounds_on(self, d: date) -> tuple[datetime, datetime] | None:
        if self.days is not None and DAYS[d.weekday()] not in self.days:
            return None
        return (datetime.combine(d, self.start, IST), datetime.combine(d, self.end, IST))


@dataclass(frozen=True)
class CallingPolicy:
    windows: tuple[DailyWindow, ...]

    def interval_on(self, d: date) -> tuple[datetime, datetime] | None:
        """The intersection of all windows on IST date `d`, or None if dialling is closed all day."""
        lo, hi = None, None
        for w in self.windows:
            b = w.bounds_on(d)
            if b is None:
                return None
            lo = b[0] if lo is None else max(lo, b[0])
            hi = b[1] if hi is None else min(hi, b[1])
        if lo is None or lo >= hi:
            return None
        return lo, hi

    def can_start(self, t: datetime, guard: timedelta = timedelta(0)) -> bool:
        """May a call be *started* at `t`?

        `guard` stops us starting a call in the last few seconds of a window: the provider checks
        the window when our request arrives, which is slightly after we decide to send it.
        """
        iv = self.interval_on(t.astimezone(IST).date())
        return iv is not None and iv[0] <= t < iv[1] - guard

    def next_start(self, t: datetime, guard: timedelta = timedelta(0)) -> datetime:
        """Earliest instant >= t at which `can_start` is true."""
        local = t.astimezone(IST)
        for offset in range(0, 15):
            d = local.date() + timedelta(days=offset)
            iv = self.interval_on(d)
            if iv is None:
                continue
            start, end = iv[0], iv[1] - guard
            candidate = max(t, start)
            if candidate < end:
                return candidate
        raise ValueError("policy never opens within two weeks")
