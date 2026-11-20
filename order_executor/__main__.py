"""Submit mixed order actions concurrently and record their dispatches."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .executor import SimulatedExchange, TradeOrderExecutor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=Path("output/orders.csv"))
    parser.add_argument("--orders", type=int, default=8, help="orders with three actions each")
    args = parser.parse_args()
    if args.orders < 1:
        parser.error("--orders must be positive")

    with TradeOrderExecutor(SimulatedExchange(args.csv)) as executor:
        def submit(index: int):
            order_id = f"order-{index}"
            return (
                executor.place(order_id, 10, "100.00"),
                executor.modify(order_id, 12, "101.00"),
                executor.cancel(order_id),
            )

        with ThreadPoolExecutor(max_workers=4) as producers:
            batches = list(producers.map(submit, range(args.orders)))
        for batch in batches:
            for result in batch:
                result.result()
    print(f"Dispatched {3 * args.orders} actions to {args.csv}")


if __name__ == "__main__":
    main()
