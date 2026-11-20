"""Run the market-data consumer with an optional local WebSocket publisher."""

import argparse
import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from .feed import CandleCsv, MarketData
from .model import Tick


async def publish(socket) -> None:
    # Accelerated, deterministic exchange time produces observable candles quickly.
    start = datetime(2026, 11, 17, 9, 15, tzinfo=UTC)
    step = 0
    try:
        while True:
            timestamp = start + timedelta(seconds=15 * step)
            for index, instrument in enumerate(("INFY", "TCS")):
                await socket.send(json.dumps({
                    "instrument": instrument,
                    "timestamp": timestamp.isoformat(),
                    "ltp": 100 + index * 50 + step % 7,
                    "ltq": 10 + index,
                }))
            step += 1
            await asyncio.sleep(0.2)
    except ConnectionClosed:
        return


def sample_strategy(name: str):
    async def handle(tick: Tick) -> None:
        logging.info("%s received %s %s x %s", name, tick.instrument, tick.ltq, tick.ltp)

    return handle


async def run(args: argparse.Namespace) -> None:
    if args.url:
        await consume(args.url, args.csv, args.seconds)
    else:
        async with serve(publish, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            await consume(f"ws://127.0.0.1:{port}", args.csv, args.seconds)


async def consume(url: str, path: Path, duration: float | None) -> None:
    feed = MarketData(url, CandleCsv(path))
    feed.subscribe({"INFY"}, sample_strategy("INFY strategy"))
    feed.subscribe({"INFY", "TCS"}, sample_strategy("multi-instrument strategy"))
    try:
        async with asyncio.timeout(duration):
            await feed.run()
    except TimeoutError:
        pass
    finally:
        logging.info(
            "Candle CSV %s (invalid messages %d, late ticks %d)",
            path, feed.invalid_messages, feed.candles.late_ticks,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", help="connect to an existing WebSocket publisher")
    parser.add_argument("--csv", type=Path, default=Path("output/candles.csv"))
    parser.add_argument("--seconds", type=float, help="stop after this many seconds")
    args = parser.parse_args()
    if args.seconds is not None and args.seconds <= 0:
        parser.error("--seconds must be positive")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

