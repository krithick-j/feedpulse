from __future__ import annotations

import argparse
import socket
import sys
import time


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait for a TCP endpoint to accept connections.")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--name", required=True)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--interval", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    deadline = time.monotonic() + args.timeout

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((args.host, args.port), timeout=2):
                print(f"{args.name} is reachable at {args.host}:{args.port}")
                return 0
        except OSError:
            time.sleep(args.interval)

    print(
        f"Timed out waiting for {args.name} at {args.host}:{args.port}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

