"""Thread-safe FIFO order submission through one rolling-window dispatcher."""

import csv
import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from queue import Queue
from typing import Literal

LIMIT = 10
WINDOW_NS = 1_000_000_000
ActionKind = Literal["place", "modify", "cancel"]


@dataclass(frozen=True, slots=True)
class Action:
    kind: ActionKind
    order_id: str
    quantity: int | None = None
    limit_price: str | None = None


@dataclass(frozen=True, slots=True)
class Dispatch:
    action: Action
    monotonic_ns: int
    utc_timestamp: str


class SimulatedExchange:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        fields = ("monotonic_ns", "timestamp_utc", "action", "order_id", "quantity", "limit_price")
        with path.open("w", newline="", encoding="utf-8") as stream:
            csv.writer(stream).writerow(fields)

    def __call__(self, dispatch: Dispatch) -> None:
        action = dispatch.action
        with self.path.open("a", newline="", encoding="utf-8") as stream:
            csv.writer(stream).writerow((
                dispatch.monotonic_ns, dispatch.utc_timestamp, action.kind,
                action.order_id, action.quantity, action.limit_price,
            ))


class TradeOrderExecutor:
    def __init__(self, exchange: Callable[[Dispatch], None]):
        self._exchange = exchange
        self._requests: Queue[tuple[Action, Future[Dispatch]] | None] = Queue()
        self._state_lock = threading.Lock()
        self._closed = False
        self._worker = threading.Thread(target=self._run, name="order-dispatch")
        self._worker.start()

    def place(self, order_id: str, quantity: int, limit_price: str) -> Future[Dispatch]:
        return self._submit(Action("place", order_id, quantity, limit_price))

    def modify(self, order_id: str, quantity: int, limit_price: str) -> Future[Dispatch]:
        return self._submit(Action("modify", order_id, quantity, limit_price))

    def cancel(self, order_id: str) -> Future[Dispatch]:
        return self._submit(Action("cancel", order_id))

    def _submit(self, action: Action) -> Future[Dispatch]:
        result: Future[Dispatch] = Future()
        with self._state_lock:
            if self._closed:
                raise RuntimeError("executor is closed")
            self._requests.put((action, result))
        return result

    def _run(self) -> None:
        recent: deque[int] = deque()
        while (request := self._requests.get()) is not None:
            action, result = request
            while True:
                now = time.monotonic_ns()
                while recent and now - recent[0] >= WINDOW_NS:
                    recent.popleft()
                if len(recent) < LIMIT:
                    break
                time.sleep((recent[0] + WINDOW_NS - now) / WINDOW_NS)

            if not result.set_running_or_notify_cancel():
                continue
            utc_timestamp = datetime.now(UTC).isoformat()
            dispatched = Dispatch(action, time.monotonic_ns(), utc_timestamp)
            # Completion is an upper bound on when the synchronous call began.
            # This stays conservative even if the worker is preempted at dispatch.
            try:
                self._exchange(dispatched)
            except Exception as exc:
                recent.append(time.monotonic_ns())
                result.set_exception(exc)
            else:
                recent.append(time.monotonic_ns())
                result.set_result(dispatched)

    def close(self) -> None:
        if threading.current_thread() is self._worker:
            raise RuntimeError("close must be called outside the dispatch thread")
        with self._state_lock:
            if not self._closed:
                self._closed = True
                self._requests.put(None)
        self._worker.join()

    def __enter__(self) -> "TradeOrderExecutor":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
