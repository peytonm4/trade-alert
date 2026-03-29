# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Setup

```bash
pip install ccxt yfinance redis flask
docker run -d -p 6379:6379 redis
```

## Running

```bash
python ingest.py       # Starts polling loop (fetches every 60s)
python webhook.py      # Starts Flask webhook receiver on port 5111
```

Verify data flow:
```bash
redis-cli XLEN market_data:crypto:BTC_USDT
redis-cli XRANGE market_data:equity:SPY - + COUNT 3
redis-cli XRANGE signals:tradingview - + COUNT 5
```

## Architecture

Two entry points that both publish to Redis Streams:

- **`ingest.py`** — polling loop using an abstract `BaseFetcher` with three concrete subclasses (`CryptoFetcher` via CCXT/Binance, `EquityFetcher` via yfinance, `FuturesFetcher` via yfinance proxy symbols like `NQ=F`)
- **`webhook.py`** — Flask endpoint `/webhook` receiving TradingView JSON alerts

All bars are normalized to `OHLCVBar` dataclass before publishing. Redis key pattern:
- `market_data:{asset_class}:{symbol}` — canonical bar streams
- `signals:tradingview` — raw TradingView alert payloads

The downstream signal detection, TimescaleDB sink, and trade execution layers are not yet built — Redis Streams are the handoff point.

## Key Constraints

- Futures data is daily bars via yfinance proxy (not true tick data) — a Databento/Rithmic adapter is the intended production replacement
- Webhook has no authentication (POC limitation)
- yfinance rate limits are safe at 60s polling interval; don't reduce significantly
