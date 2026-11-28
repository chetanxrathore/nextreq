"""Client-side rate limiting: ``RateLimit`` and the ``rate_limit=`` client option."""

from __future__ import annotations

import math
import threading
import time
from typing import Final, Union

__all__ = ["RateLimit", "RateLimitTypes"]


class RateLimit:
    """At most ``rate`` requests every ``per`` seconds.

    ::

        client = nextreq.Client(rate_limit=nextreq.RateLimit(600, per=60))

    Requests are spaced evenly (one every ``per / rate`` seconds), so the limit
    holds over any window of ``per`` seconds, not only on average. ``burst``
    lets that many requests leave back to back after an idle period before
    the spacing applies again.

    One ``RateLimit`` can be given to several clients, sync or async, in any
    number of threads; they then share one budget. Every request sent to the
    server counts, including retries, redirects and authentication handshakes.
    """

    __slots__ = ("_interval", "_lock", "_slot", "_tolerance", "burst", "per", "rate")

    def __init__(self, rate: float, per: float = 1.0, *, burst: int = 1) -> None:
        if isinstance(rate, bool) or not (math.isfinite(rate) and rate > 0):
            raise ValueError("rate must be a positive number of requests")
        if not (math.isfinite(per) and per > 0):
            raise ValueError("per must be a positive number of seconds")
        if isinstance(burst, bool) or not isinstance(burst, int) or burst < 1:
            raise ValueError("burst must be an integer >= 1")
        self.rate: Final = rate
        self.per: Final = per
        self.burst: Final = burst
        self._interval = per / rate
        self._tolerance = (burst - 1) * self._interval
        self._lock = threading.Lock()
        # The time at which the next evenly spaced request is due.
        self._slot = 0.0

    def reserve(self) -> float:
        """Book the next free slot and return how many seconds away it is.

        The caller is expected to wait that long before it sends. Slots are
        handed out in the order of the calls, so waiters are served first
        come, first served.
        """
        with self._lock:
            # perf_counter, not monotonic: the latter ticks every 15.6 ms on
            # Windows before Python 3.13, which is coarser than many limits.
            now = time.perf_counter()
            start = max(now, self._slot - self._tolerance)
            self._slot = max(self._slot, now) + self._interval
        return start - now

    def __repr__(self) -> str:
        burst = f", burst={self.burst}" if self.burst != 1 else ""
        return f"RateLimit({self.rate:g}, per={self.per:g}{burst})"


RateLimitTypes = Union[float, RateLimit, None]


def coerce_rate_limit(value: RateLimitTypes) -> RateLimit | None:
    """``10`` means ten requests per second; ``None`` means no limit."""
    if value is None or isinstance(value, RateLimit):
        return value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("rate_limit must be a number of requests per second or a RateLimit")
    return RateLimit(value)
