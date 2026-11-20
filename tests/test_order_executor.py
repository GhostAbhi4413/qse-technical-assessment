import csv
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from order_executor.executor import LIMIT, WINDOW_NS, SimulatedExchange, TradeOrderExecutor


class ExecutorTests(unittest.TestCase):
    def test_concurrent_mixed_actions_respect_every_rolling_window(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "orders.csv"
            barrier = threading.Barrier(8)

            with TradeOrderExecutor(SimulatedExchange(path)) as executor:
                def produce(index):
                    order_id = str(index)
                    barrier.wait(timeout=3)
                    return (
                        executor.place(order_id, 1, "100"),
                        executor.modify(order_id, 2, "101"),
                        executor.cancel(order_id),
                    )

                with ThreadPoolExecutor(max_workers=8) as pool:
                    batches = list(pool.map(produce, range(8)))
            self.assertTrue(all(result.done() for batch in batches for result in batch))
            executor.close()
            with self.assertRaisesRegex(RuntimeError, "closed"):
                executor.cancel("late")
            with path.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))

        self.assertEqual(len(rows), 24)
        times = [int(row["monotonic_ns"]) for row in rows]
        self.assertEqual(times, sorted(times))
        for first, eleventh in zip(times, times[LIMIT:]):
            self.assertGreaterEqual(eleventh - first, WINDOW_NS)
        for index in range(8):
            self.assertEqual(
                [row["action"] for row in rows if row["order_id"] == str(index)],
                ["place", "modify", "cancel"],
            )

    def test_failed_attempt_consumes_capacity_and_worker_continues(self):
        attempts = []

        def exchange(dispatch):
            attempts.append(dispatch.monotonic_ns)
            if len(attempts) == 1:
                raise OSError("simulated failure")

        with TradeOrderExecutor(exchange) as executor:
            results = [executor.cancel(str(index)) for index in range(11)]
        with self.assertRaisesRegex(OSError, "simulated failure"):
            results[0].result()
        self.assertEqual(results[-1].result().action.order_id, "10")
        self.assertGreaterEqual(attempts[10] - attempts[0], WINDOW_NS)

    def test_cancelled_pending_request_never_dispatches(self):
        entered = threading.Event()
        release = threading.Event()
        dispatched = []

        def exchange(dispatch):
            entered.set()
            release.wait(timeout=3)
            dispatched.append(dispatch.action.order_id)

        with TradeOrderExecutor(exchange) as executor:
            executor.cancel("first")
            try:
                self.assertTrue(entered.wait(timeout=3))
                pending = executor.cancel("pending")
                self.assertTrue(pending.cancel())
            finally:
                release.set()
        self.assertEqual(dispatched, ["first"])
