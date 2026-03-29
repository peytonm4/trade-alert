"""
Export market data from Redis Streams to CSV or JSON.

Usage:
    python3 export.py                          # prints all streams as CSV to stdout
    python3 export.py --symbol QQQ             # single symbol
    python3 export.py --symbol QQQ --fmt json  # JSON output
    python3 export.py --symbol QQQ --out qqq.csv
"""

import argparse
import csv
import json
import sys

import redis

REDIS_URL = "redis://localhost:6379"


def fetch_bars(r: redis.Redis, key: str) -> list[dict]:
    entries = r.xrange(key)
    return [fields for _, fields in entries]


def list_streams(r: redis.Redis) -> list[str]:
    return sorted(r.keys("market_data:*"))


def export_csv(rows: list[dict], out):
    if not rows:
        return
    writer = csv.DictWriter(out, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)


def export_json(rows: list[dict], out):
    json.dump(rows, out, indent=2)
    out.write("\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", help="Symbol to export, e.g. QQQ. Omit for all.")
    parser.add_argument("--asset-class", default="equity", help="equity | futures | crypto")
    parser.add_argument("--fmt", choices=["csv", "json"], default="csv")
    parser.add_argument("--out", help="Output file path. Defaults to stdout.")
    args = parser.parse_args()

    r = redis.from_url(REDIS_URL, decode_responses=True)

    if args.symbol:
        keys = [f"market_data:{args.asset_class}:{args.symbol.upper()}"]
    else:
        keys = list_streams(r)

    all_rows: list[dict] = []
    for key in keys:
        all_rows.extend(fetch_bars(r, key))

    out = open(args.out, "w", newline="") if args.out else sys.stdout
    try:
        if args.fmt == "csv":
            export_csv(all_rows, out)
        else:
            export_json(all_rows, out)
    finally:
        if args.out:
            out.close()
            print(f"Exported {len(all_rows)} bars to {args.out}")
        else:
            print(f"\n# {len(all_rows)} bars", file=sys.stderr)


if __name__ == "__main__":
    main()
