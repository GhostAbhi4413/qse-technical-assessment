"""Single feed connection, instrument routing and CSV persistence."""

import asyncio
import csv
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from .model import Candle, CandleAggregator, Tick

log = logging.getLogger(__name__)
CANDLE_FIELDS = ("instrument", "start_utc", "open", "high", "low", "close", "volume")


class CandleCsv:
    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a+", newline="", encoding="utf-8") as stream:
            stream.seek(0)
            header = next(csv.reader(stream), None)
            if header is None:
                csv.writer(stream).writerow(CANDLE_FIELDS)
            elif tuple(header) != CANDLE_FIELDS:
                raise ValueError(f"incompatible candle CSV header in {self.path}")

    def append(self, candle: Candle) -> None:
        with self.path.open("a", newline="", encoding="utf-8") as stream:
            csv.writer(stream).writerow((
                candle.instrument, candle.start.isoformat(), candle.open,
                candle.high, candle.low, candle.close, candle.volume,
            ))


class Subscription:
    def __init__(self, handler: Callable[[Tick], Awaitable[None]], capacity: int):
        self.handler = handler
        self.queue: asyncio.Queue[Tick | None] = asyncio.Queue(capacity)
        self.dropped = 0

    async def consume(self) -> None:
        while (tick := await self.queue.get()) is not None:
            try:
                await self.handler(tick)
            except Exception:
                log.exception("strategy failed for %s", tick.instrument)


class MarketData:
    def __init__(self, url: str, sink: CandleCsv, retry_delay: float = 0.25):
        self.url = url
        self.sink = sink
        self.retry_delay = retry_delay
        self.candles = CandleAggregator()
        self.invalid_messages = 0
        self._routes: dict[str, list[Subscription]] = defaultdict(list)
        self._subscriptions: list[Subscription] = []
        self._running = False

    def subscribe(
        self, instruments: set[str], handler: Callable[[Tick], Awaitable[None]],
        *, capacity: int = 256,
    ) -> Subscription:
        if self._running:
            raise RuntimeError("register subscribers before starting the feed")
        if not instruments or capacity < 1:
            raise ValueError("subscription requires instruments and positive capacity")
        subscription = Subscription(handler, capacity)
        self._subscriptions.append(subscription)
        for instrument in instruments:
            self._routes[instrument].append(subscription)
        return subscription

    async def _receive(self, message: str | bytes) -> None:
        try:
            tick = Tick.parse(message)
        except (ValueError, ArithmeticError, RecursionError):
            self.invalid_messages += 1
            return

        completed = self.candles.ingest(tick)
        for subscription in self._routes.get(tick.instrument, ()):
            try:
                subscription.queue.put_nowait(tick)
            except asyncio.QueueFull:
                subscription.dropped += 1
        if completed is not None:
            # A single writer keeps append order. Finish an in-flight write on stop.
            write = asyncio.create_task(asyncio.to_thread(self.sink.append, completed))
            try:
                await asyncio.shield(write)
            except asyncio.CancelledError:
                await write
                raise

    async def _stream(self) -> None:
        delay = self.retry_delay
        while True:
            try:
                async with connect(self.url, proxy=None, max_size=64 * 1024) as socket:
                    async for message in socket:
                        delay = self.retry_delay
                        try:
                            await self._receive(message)
                        except OSError as exc:
                            raise RuntimeError('candle CSV write failed') from exc
            except (WebSocketException, OSError) as exc:
                log.warning('feed connection interrupted: %s', exc)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5.0)

    async def run(self) -> None:
        if self._running:
            raise RuntimeError("feed already running")
        self._running = True
        try:
            await asyncio.to_thread(self.sink.initialize)
            async with asyncio.TaskGroup() as group:
                workers = [
                    group.create_task(subscription.consume())
                    for subscription in self._subscriptions
                ]
                try:
                    await self._stream()
                finally:
                    for subscription in self._subscriptions:
                        await subscription.queue.put(None)
                    await asyncio.gather(*workers)
        finally:
            self._running = False
