"""Validate trades and build one-minute event-time candles."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Tick:
    instrument: str
    timestamp: datetime
    ltp: Decimal
    ltq: int

    @classmethod
    def parse(cls, message: str | bytes) -> "Tick":
        data = json.loads(message, parse_float=Decimal)
        if not isinstance(data, dict):
            raise ValueError("expected a JSON object")
        instrument = data.get("instrument")
        timestamp = data.get("timestamp")
        price = data.get("ltp")
        quantity = data.get("ltq")
        if not isinstance(instrument, str) or not instrument.strip():
            raise ValueError("invalid instrument")
        if not isinstance(timestamp, str) or "T" not in timestamp:
            raise ValueError("expected an ISO 8601 timestamp")
        timestamp = datetime.fromisoformat(timestamp)
        if timestamp.utcoffset() is None:
            raise ValueError("timestamp must include an offset")
        if isinstance(price, bool) or not isinstance(price, (int, Decimal)):
            raise ValueError("price must be a finite positive number")
        price = Decimal(price)
        if not price.is_finite() or price <= 0:
            raise ValueError("price must be a finite positive number")
        if type(quantity) is not int or quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        return cls(instrument, timestamp.astimezone(UTC), price, quantity)


@dataclass(slots=True)
class Candle:
    instrument: str
    start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    first: datetime
    last: datetime

    @classmethod
    def from_tick(cls, tick: Tick, start: datetime) -> "Candle":
        return cls(
            tick.instrument, start, tick.ltp, tick.ltp, tick.ltp, tick.ltp,
            tick.ltq, tick.timestamp, tick.timestamp,
        )

    def update(self, tick: Tick) -> None:
        if tick.timestamp < self.first:
            self.open, self.first = tick.ltp, tick.timestamp
        if tick.timestamp >= self.last:
            self.close, self.last = tick.ltp, tick.timestamp
        self.high = max(self.high, tick.ltp)
        self.low = min(self.low, tick.ltp)
        self.volume += tick.ltq


class CandleAggregator:
    def __init__(self) -> None:
        self._active: dict[str, Candle] = {}
        self.late_ticks = 0

    def ingest(self, tick: Tick) -> Candle | None:
        start = tick.timestamp.replace(second=0, microsecond=0)
        active = self._active.get(tick.instrument)
        if active is None or start > active.start:
            self._active[tick.instrument] = Candle.from_tick(tick, start)
            return active
        if start == active.start:
            active.update(tick)
        else:
            self.late_ticks += 1
        return None
