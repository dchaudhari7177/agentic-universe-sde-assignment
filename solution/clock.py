"""Simulated time. Every business rule uses the provider's clock, never the machine's."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta


class Clock:
    def now(self) -> datetime:  # pragma: no cover - interface
        raise NotImplementedError

    def sim_seconds(self, real_seconds: float) -> float:  # pragma: no cover - interface
        raise NotImplementedError


@dataclass
class ProviderClock(Clock):
    """sim_now = sim_epoch + (time.time() - real_epoch) * speed, as documented by the provider.

    Computing it locally avoids a round-trip per decision. It is exact when the provider runs on
    the same host; otherwise the window guard absorbs small skew.
    """

    sim_epoch: datetime
    real_epoch: float
    speed: float

    @classmethod
    def from_payload(cls, payload: dict) -> "ProviderClock":
        return cls(datetime.fromisoformat(payload["sim_epoch"]), float(payload["real_epoch"]),
                   float(payload["speed"]))

    def now(self) -> datetime:
        return self.sim_epoch + timedelta(seconds=(time.time() - self.real_epoch) * self.speed)

    def sim_seconds(self, real_seconds: float) -> float:
        return real_seconds * self.speed


@dataclass
class FakeClock(Clock):
    """Manually advanced clock for tests."""

    current: datetime
    speed: float = 600.0

    def now(self) -> datetime:
        return self.current

    def advance(self, **kw) -> datetime:
        self.current += timedelta(**kw)
        return self.current

    def sim_seconds(self, real_seconds: float) -> float:
        return real_seconds * self.speed
