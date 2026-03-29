# POC Data Ingestion — Quick Start

## What this does
Pulls free OHLCV data into your Redis Streams pipeline:
- **Crypto** → CCXT (Binance, no API key needed)
- **Equities** → yfinance (SPY, QQQ, etc.)
- **Futures** → yfinance proxy (NQ=F → MNQ) + TradingView webhook for real-time

## Setup

```bash
pip install ccxt yfinance redis flask
```

Make sure Redis is running locally:
```bash
# Docker one-liner
docker run -d --name redis-poc -p 6379:6379 redis:7-alpine
```

## Run the ingestion loop
```bash
python ingest.py
```
This polls every 60s. Edit `__main__` block to change symbols/intervals.

## Run the webhook receiver (for MNQ via TradingView)
```bash
python webhook.py
```

Expose it to the internet for TradingView:
```bash
ngrok http 5111
```

Then in TradingView:
1. Create alert on your Channel Low Sweep & Reclaim indicator
2. Set webhook URL to `https://<your-ngrok-url>/webhook`
3. Paste the JSON template from webhook.py docstring into the alert message

## Verify data is flowing
```bash
# Check stream keys
redis-cli KEYS "market_data:*"

# Read latest entries
redis-cli XREVRANGE market_data:crypto:BTC_USDT + - COUNT 5

# Read webhook signals
redis-cli XREVRANGE signals:tradingview + - COUNT 5
```

## Known POC limitations
- **Futures**: NQ=F via yfinance is daily bars only, not true MNQ. Webhook fills the gap for real-time but you only get 1 alert on free TradingView.
- **Rate limits**: yfinance will throttle you if you poll too aggressively. 60s interval is safe.
- **No backfill**: This fetches rolling windows, not gap-aware historical backfill. Fine for POC.
- **No auth on webhook**: Add a shared secret header before exposing to the internet for real.

## Next steps (when POC validates)
1. Add TimescaleDB sink (consume from Redis Streams → INSERT into hypertables)
2. Swap yfinance futures proxy for Databento or Rithmic adapter
3. Add backfill logic with gap detection
4. Add signal detection consumer that reads from these streams
