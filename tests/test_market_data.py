import asyncio
import csv
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from websockets.asyncio.server import serve

from market_data.feed import CandleCsv, MarketData
from market_data.model import CandleAggregator, Tick


def message(symbol, time, price=100, quantity=2):
    return json.dumps({
        "instrument": symbol, "timestamp": f"2026-11-17T{time}Z",
        "ltp": price, "ltq": quantity,
    })


class CandleTests(unittest.TestCase):
    def test_event_order_ties_volume_and_late_policy(self):
        aggregator = CandleAggregator()
        for raw in (
            message("A", "09:15:30", 102),
            message("B", "09:15:02", 80),
            message("A", "09:15:10", 99),
            message("A", "09:15:30", 103),
            message("A", "09:15:30", 103),
        ):
            self.assertIsNone(aggregator.ingest(Tick.parse(raw)))
        candle = aggregator.ingest(Tick.parse(message("A", "09:16:00")))
        self.assertEqual(candle.start.isoformat(), "2026-11-17T09:15:00+00:00")
        self.assertEqual(
            (candle.open, candle.high, candle.low, candle.close, candle.volume),
            (Decimal(99), Decimal(103), Decimal(99), Decimal(103), 8),
        )
        self.assertIsNone(aggregator.ingest(Tick.parse(message("A", "09:15:59.999"))))
        self.assertEqual(aggregator.late_ticks, 1)
        other = aggregator.ingest(Tick.parse(message("B", "09:18:00")))
        self.assertEqual((other.instrument, other.volume), ("B", 2))

    def test_validation_and_utc_conversion(self):
        tick = Tick.parse(message("A", "09:15:00.125", 25999.50).replace(
            "09:15:00.125Z", "14:45:00.125+05:30",
        ))
        self.assertEqual(tick.timestamp.isoformat(), "2026-11-17T09:15:00.125000+00:00")
        self.assertEqual(tick.ltp, Decimal("25999.50"))
        for raw in (
            "[]", "{", message("A", "09:15:00", True),
            message("A", "09:15:00", float("nan")),
            message("A", "09:15:00", quantity=1.5),
            message("A", "09:15:00").replace("Z", ""),
        ):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                Tick.parse(raw)


class FeedTests(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_routing_and_completed_csv_on_stop(self):
        connections = 0
        received_a = []
        received_all = []
        delivered = asyncio.Event()

        async def publisher(socket):
            nonlocal connections
            connections += 1
            if connections == 1:
                await socket.send("invalid")
                await socket.send(message("A", "09:15:00"))
                await socket.send(message("B", "09:15:01"))
                await socket.close()
            else:
                await socket.send(message("A", "09:16:00"))
                await socket.wait_closed()

        async def a_only(tick):
            received_a.append(tick.instrument)

        async def all_symbols(tick):
            received_all.append(tick.instrument)
            if len(received_all) == 3:
                delivered.set()

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "candles.csv"
            async with serve(publisher, "127.0.0.1", 0) as server:
                port = server.sockets[0].getsockname()[1]
                feed = MarketData(f"ws://127.0.0.1:{port}", CandleCsv(path), 0.01)
                feed.subscribe({"A"}, a_only)
                feed.subscribe({"A", "B"}, all_symbols)
                task = asyncio.create_task(feed.run())
                try:
                    await asyncio.wait_for(delivered.wait(), timeout=3)
                finally:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
            self.assertEqual(connections, 2)
            self.assertEqual(feed.invalid_messages, 1)
            self.assertEqual(received_a, ["A", "A"])
            self.assertEqual(received_all, ["A", "B", "A"])
            CandleCsv(path).initialize()
            with path.open(newline="", encoding="utf-8") as stream:
                self.assertEqual(list(csv.DictReader(stream)), [{
                    "instrument": "A", "start_utc": "2026-11-17T09:15:00+00:00",
                    "open": "100", "high": "100", "low": "100", "close": "100",
                    "volume": "2",
                }])

    async def test_full_subscription_does_not_block_other_subscribers(self):
        async def handler(tick):
            pass

        feed = MarketData("ws://127.0.0.1:0", CandleCsv(Path("unused.csv")))
        slow = feed.subscribe({"A"}, handler, capacity=1)
        other = feed.subscribe({"A"}, handler, capacity=2)
        await feed._receive(message("A", "09:15:00"))
        await feed._receive(message("A", "09:15:01"))
        self.assertEqual((slow.queue.qsize(), slow.dropped), (1, 1))
        self.assertEqual((other.queue.qsize(), other.dropped), (2, 0))
