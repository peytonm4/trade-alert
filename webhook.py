"""
TradingView Webhook Receiver

Receives JSON alert payloads from TradingView and pushes them into
Redis Streams. This is your bridge for MNQ real-time signals on the
free TradingView plan (1 alert slot).

Setup:
  1. pip install flask redis
  2. python webhook.py
  3. Expose via ngrok: ngrok http 5111
  4. Set TradingView alert webhook URL to your ngrok HTTPS URL + /webhook
  5. In the alert message body, use this JSON template:

     {
       "symbol": "MNQ",
       "open": {{open}},
       "high": {{high}},
       "low": {{low}},
       "close": {{close}},
       "volume": {{volume}},
       "time": {{time}},
       "timeframe": "{{interval}}",
       "signal": "channel_low_sweep",
       "extras": {
         "delta": {{plot("cumDelta")}},
         "rsi": {{plot("RSI")}}
       }
     }

  Note: {{plot("indicator_name")}} only works if the indicator is on
  the same chart pane. For POC, even just OHLCV + time is enough.

Security:
  POC-grade — no auth. For production, add a shared secret header
  that TradingView sends and you validate.
"""

import json
import logging
from datetime import datetime, timezone

import redis
from flask import Flask, request, jsonify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [webhook] %(levelname)s: %(message)s",
)
logger = logging.getLogger("webhook")

app = Flask(__name__)

# --- Config ---
REDIS_URL = "redis://localhost:6379"
STREAM_PREFIX = "market_data:futures"
SIGNAL_STREAM = "signals:tradingview"

r = redis.from_url(REDIS_URL, decode_responses=True)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})


@app.route("/webhook", methods=["POST"])
def webhook():
    """
    Receives TradingView alert payload.
    Publishes to two streams:
      1. market_data:futures:{SYMBOL} — raw OHLCV bar
      2. signals:tradingview — full payload including any signal metadata
    """
    try:
        # TradingView sends JSON in the request body
        payload = request.get_json(force=True)
    except Exception:
        logger.warning("Failed to parse JSON from request")
        return jsonify({"error": "invalid JSON"}), 400

    logger.info("Received webhook: %s", json.dumps(payload, indent=2))

    symbol = payload.get("symbol", "UNKNOWN")

    # --- Publish raw bar to market data stream ---
    bar_data = {
        "symbol": symbol,
        "timestamp": str(int(payload.get("time", 0) * 1000)),  # TV sends epoch seconds
        "open": str(payload.get("open", 0)),
        "high": str(payload.get("high", 0)),
        "low": str(payload.get("low", 0)),
        "close": str(payload.get("close", 0)),
        "volume": str(payload.get("volume", 0)),
        "source": "tradingview_webhook",
        "timeframe": payload.get("timeframe", "unknown"),
    }

    bar_stream = f"{STREAM_PREFIX}:{symbol}"
    bar_id = r.xadd(bar_stream, bar_data)
    logger.info("Published bar → %s [%s]", bar_stream, bar_id)

    # --- Publish full signal payload (includes extras like delta, RSI) ---
    signal_data = {
        "symbol": symbol,
        "signal": payload.get("signal", "none"),
        "payload": json.dumps(payload),
        "received_at": datetime.now(timezone.utc).isoformat(),
    }

    sig_id = r.xadd(SIGNAL_STREAM, signal_data)
    logger.info("Published signal → %s [%s]", SIGNAL_STREAM, sig_id)

    return jsonify({"status": "ok", "bar_id": bar_id, "signal_id": sig_id}), 200


if __name__ == "__main__":
    logger.info("Starting webhook receiver on :5111")
    app.run(host="0.0.0.0", port=5111, debug=False)
