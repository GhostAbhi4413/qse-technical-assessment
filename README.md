# Python Technical Assessment - Quant Systems Engineering

Two standalone Python programs demonstrate live market data processing and exchange order throttling. Both run locally on Windows or Linux with Python 3.11 or later. The order executor uses only the standard library. The market data module uses one pinned dependency, websockets 15.0.1.

## Setup

From the project root, create a virtual environment and install the pinned dependency.

**Windows PowerShell**

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**Linux**

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

In the remaining commands, `python` means the environment's Python. Activate it with `source .venv/bin/activate` on Linux or use the full executable path shown above on either platform. Check `python --version` before running. No external accounts, services, credentials, or databases are needed.

## Live market data

```text
python -m market_data --seconds 3 --csv output/candles.csv
```

This starts a loopback WebSocket publisher and one consumer. The publisher streams INFY and TCS trades in accelerated exchange time. One sample strategy receives INFY and another receives both instruments. Run continuously by omitting `--seconds` and stop with Ctrl+C. To connect to an already running compatible publisher, add `--url ws://127.0.0.1:8765`.

Example first two lines from a fresh run:

```csv
instrument,start_utc,open,high,low,close,volume
INFY,2026-11-17T09:15:00+00:00,100,103,100,103,40
```

- **Ingestion and routing.** One asyncio connection validates each frame and parses its timestamp and price once. Timestamps require an explicit offset and normalize to UTC. Prices use Decimal and quantities are positive integers. Invalid ticks are counted and skipped. Subscribers register an instrument set and an async handler before the feed starts. Instrument lookup routes the same immutable tick to matching subscribers. Each has its own bounded FIFO and handler task. When a queue fills, it drops the incoming tick and increments that subscriber's drop counter. Handlers should yield during I/O and offload blocking work. Connection failures reconnect with capped exponential backoff and retain in-memory aggregation state. Trades missed during an outage require upstream replay.
- **Aggregation.** Each instrument owns one active UTC minute, with intervals inclusive at the start and exclusive at the end. The first trade in a newer minute completes that instrument's previous candle. Within an open minute, timestamps determine open and close. For equal timestamps, the first arrival wins for open and the last arrival wins for close. Earlier minutes are already closed, so late ticks are counted and excluded from candles while still being routed to strategies. Identical ticks are counted individually because the feed schema lacks a trade ID. Empty intervals produce no rows. An idle instrument's final candle remains in memory until its own stream advances, and shutdown discards active candles. This keeps completion based on exchange events even during a disconnect or historical replay.
- **Persistence and lifecycle.** The CSV sink creates parent directories, writes a header once, and checks existing headers before appending. One off-thread append runs per completed candle. Closing the file flushes each row. Slow storage applies backpressure at candle boundaries. Graceful shutdown finishes any current write and drains queued strategy work. Use one writer per file and use a fresh output path for independent demo runs, since the local publisher starts with the same timestamps each run.
- **Cost.** Candle updates are constant time with one active candle per instrument. Routing work is proportional to the number of subscribers for the incoming symbol. Strategy queues bound pending work independently of the publisher's rate.

## Exchange rate limiter

```text
python -m order_executor --orders 8 --csv output/orders.csv
```

Four producer threads submit three actions for each order. The command prints `Dispatched 24 actions to output/orders.csv`. Each run creates a fresh CSV with the columns below. Values are illustrative.

```csv
monotonic_ns,timestamp_utc,action,order_id,quantity,limit_price
781234567890,2026-11-17T09:15:00.125000+00:00,place,order-0,10,100.00
```

- **Execution.** `place`, `modify`, and `cancel` put requests in the same thread-safe FIFO and return a Future. One dedicated worker owns the limiter and calls the synchronous simulated exchange. The simulator records actions in CSV and has no order book. Calls are dispatched in queue acceptance order. Use one executor for each shared exchange quota.
- **Rate guarantee.** The worker holds up to ten recent timestamps in a deque, prunes entries at least 1,000,000,000 ns old, and sleeps until the oldest timestamp can expire when full. It uses a monotonic clock, so wall-clock adjustments cannot affect eligibility. The CSV records the dispatch timestamp immediately before the exchange call. The limiter starts aging the attempt when the synchronous call returns or fails. Using this upper bound preserves the 10-in-any-rolling-second guarantee even if the worker is paused before it enters the exchange call. It also makes scheduling slightly conservative when the exchange is slow. An action exactly one second after an earlier one can occupy the next half-open window.
- **Lifecycle and cost.** Closing atomically stops admission and drains accepted work. A cancelled pending Future is skipped. Failed exchange calls count toward the quota, surface on their Futures, and are never retried automatically. The worker sleeps without blocking producers and keeps constant limiter state. The demo uses an unbounded FIFO. Callers with sustained input above 10 actions per second should bound outstanding Futures at their submission boundary. Future callbacks run in the completing thread and should return promptly.

## Tests

```text
python -m unittest discover -v
python -m unittest tests.test_market_data -v
python -m unittest tests.test_order_executor -v
```

The tests cover tick validation, minute boundaries, independent candles, out-of-order trades, duplicate handling, real WebSocket reconnect, subscription routing and overflow, and completed CSV output after shutdown. The executor tests start eight producers together, verify the action sequence per order, and validate every possible violation in the CSV by checking that dispatches ten indices apart are at least one second apart. The tests also cover pending cancellation, exchange failure, queue draining, and rejection after close.
