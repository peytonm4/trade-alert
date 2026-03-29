"""
POC Market Data Ingestion Service
Feeds OHLCV data into Redis Streams for downstream signal detection.

Asset coverage:
  - Crypto: via CCXT (Binance, Coinbase, etc.) — free, no key
  - Equities: via yfinance — free, no key
  - Futures: via yfinance (NQ=F proxy) + TradingView webhook receiver

Architecture:
  ingest.py (this file)
    ├── CryptoFetcher    → CCXT → Redis Stream
    ├── EquityFetcher    → yfinance → Redis Stream
    ├── FuturesFetcher   → yfinance (daily proxy) → Redis Stream
    └── WebhookReceiver  → Flask endpoint ← TradingView alerts → Redis Stream

Redis Stream key convention: market_data:{asset_class}:{symbol}
"""

import asyncio
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import ccxt
import redis
import yfinance as yf

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("ingest")


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------

@dataclass
class OHLCVBar:
    """Canonical bar representation pushed to Redis Streams."""
    symbol: str
    timestamp: int          # Unix ms
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str             # e.g. "binance", "yfinance", "tradingview_webhook"
    timeframe: str          # e.g. "1m", "1d"

    def to_stream_dict(self) -> dict[str, str]:
        """Redis Streams requires flat str→str mappings."""
        return {k: str(v) for k, v in asdict(self).items()}


# ---------------------------------------------------------------------------
# Base fetcher
# ---------------------------------------------------------------------------

class BaseFetcher(ABC):
    """All fetchers push bars into a Redis Stream."""

    def __init__(self, redis_client: redis.Redis):
        self.r = redis_client

    def _stream_key(self, asset_class: str, symbol: str) -> str:
        clean = symbol.replace("/", "_").replace("=", "").upper()
        return f"market_data:{asset_class}:{clean}"

    def publish(self, bar: OHLCVBar, asset_class: str) -> str:
        key = self._stream_key(asset_class, bar.symbol)
        msg_id = self.r.xadd(key, bar.to_stream_dict())
        logger.debug("Published %s → %s [%s]", bar.symbol, key, msg_id)
        return msg_id

    @abstractmethod
    def fetch(self) -> list[OHLCVBar]:
        ...


# ---------------------------------------------------------------------------
# Crypto fetcher — CCXT
# ---------------------------------------------------------------------------

class CryptoFetcher(BaseFetcher):
    """
    Pulls OHLCV from any CCXT-supported exchange.
    No API key needed for public market data.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        symbols: list[str],
        exchange_id: str = "binance",
        timeframe: str = "1m",
        limit: int = 100,
    ):
        super().__init__(redis_client)
        self.symbols = symbols
        self.timeframe = timeframe
        self.limit = limit

        exchange_class = getattr(ccxt, exchange_id)
        self.exchange: ccxt.Exchange = exchange_class({"enableRateLimit": True})
        logger.info(
            "CryptoFetcher initialized: exchange=%s, symbols=%s, tf=%s",
            exchange_id, symbols, timeframe,
        )

    def fetch(self) -> list[OHLCVBar]:
        bars: list[OHLCVBar] = []
        for symbol in self.symbols:
            try:
                raw = self.exchange.fetch_ohlcv(
                    symbol, self.timeframe, limit=self.limit
                )
                for ts, o, h, l, c, v in raw:
                    bar = OHLCVBar(
                        symbol=symbol,
                        timestamp=ts,
                        open=o, high=h, low=l, close=c, volume=v,
                        source=self.exchange.id,
                        timeframe=self.timeframe,
                    )
                    bars.append(bar)
                    self.publish(bar, asset_class="crypto")

                logger.info(
                    "Crypto: fetched %d bars for %s", len(raw), symbol
                )
            except ccxt.BaseError as e:
                logger.error("CCXT error fetching %s: %s", symbol, e)
        return bars


# ---------------------------------------------------------------------------
# Equity fetcher — yfinance
# ---------------------------------------------------------------------------

class EquityFetcher(BaseFetcher):
    """
    Pulls OHLCV via yfinance. Free, no key.
    Limitations:
      - 1m data: ~30 days history max
      - Daily: decades of history
      - Rate limits: undocumented, be polite
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        symbols: list[str],
        period: str = "5d",
        interval: str = "1m",
    ):
        super().__init__(redis_client)
        self.symbols = symbols
        self.period = period
        self.interval = interval
        logger.info(
            "EquityFetcher initialized: symbols=%s, period=%s, interval=%s",
            symbols, period, interval,
        )

    def fetch(self) -> list[OHLCVBar]:
        bars: list[OHLCVBar] = []
        for symbol in self.symbols:
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(period=self.period, interval=self.interval)

                if df.empty:
                    logger.warning("No data returned for %s", symbol)
                    continue

                for idx, row in df.iterrows():
                    ts = int(idx.timestamp() * 1000)
                    bar = OHLCVBar(
                        symbol=symbol,
                        timestamp=ts,
                        open=row["Open"],
                        high=row["High"],
                        low=row["Low"],
                        close=row["Close"],
                        volume=row["Volume"],
                        source="yfinance",
                        timeframe=self.interval,
                    )
                    bars.append(bar)
                    self.publish(bar, asset_class="equity")

                logger.info(
                    "Equity: fetched %d bars for %s", len(df), symbol
                )
            except Exception as e:
                logger.error("yfinance error fetching %s: %s", symbol, e)
        return bars


# ---------------------------------------------------------------------------
# Futures fetcher — yfinance (NQ=F proxy for POC)
# ---------------------------------------------------------------------------

class FuturesFetcher(BaseFetcher):
    """
    Uses yfinance NQ=F as a proxy for MNQ.
    POC-grade only — no tick data, daily bars, no true MNQ contract.

    For production: replace with Databento, Rithmic adapter, or
    TradingView webhook for real-time MNQ signals.
    """

    FUTURES_SYMBOL_MAP: dict[str, str] = {
        "MNQ": "NQ=F",   # Micro E-mini Nasdaq → NQ continuous
        "MES": "ES=F",   # Micro E-mini S&P → ES continuous
        "MGC": "GC=F",   # Micro Gold → Gold continuous
    }

    def __init__(
        self,
        redis_client: redis.Redis,
        symbols: list[str],
        period: str = "1mo",
        interval: str = "1d",
    ):
        super().__init__(redis_client)
        self.symbols = symbols
        self.period = period
        self.interval = interval
        logger.info(
            "FuturesFetcher initialized: symbols=%s (mapped via NQ=F proxy)",
            symbols,
        )

    def fetch(self) -> list[OHLCVBar]:
        bars: list[OHLCVBar] = []
        for sym in self.symbols:
            yf_sym = self.FUTURES_SYMBOL_MAP.get(sym, sym)
            try:
                ticker = yf.Ticker(yf_sym)
                df = ticker.history(period=self.period, interval=self.interval)

                if df.empty:
                    logger.warning("No futures data for %s (%s)", sym, yf_sym)
                    continue

                for idx, row in df.iterrows():
                    ts = int(idx.timestamp() * 1000)
                    bar = OHLCVBar(
                        symbol=sym,            # Use our symbol, not yfinance's
                        timestamp=ts,
                        open=row["Open"],
                        high=row["High"],
                        low=row["Low"],
                        close=row["Close"],
                        volume=row["Volume"],
                        source="yfinance_proxy",
                        timeframe=self.interval,
                    )
                    bars.append(bar)
                    self.publish(bar, asset_class="futures")

                logger.info(
                    "Futures: fetched %d bars for %s (via %s)",
                    len(df), sym, yf_sym,
                )
            except Exception as e:
                logger.error("Futures fetch error %s: %s", sym, e)
        return bars


# ---------------------------------------------------------------------------
# TradingView Webhook Receiver (for real-time MNQ signals)
# ---------------------------------------------------------------------------
# This runs as a separate Flask app. Your 1 free TradingView alert
# fires a JSON payload here with Pine-computed values.
#
#   Alert message format (set in TradingView):
#   {
#     "symbol": "MNQ",
#     "close": {{close}},
#     "open": {{open}},
#     "high": {{high}},
#     "low": {{low}},
#     "volume": {{volume}},
#     "time": {{time}},
#     "timeframe": "1",
#     "signal": "sweep_detected",
#     "delta": {{plot("cumDelta")}}
#   }
#
# To run: python webhook.py (see webhook.py)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_once(
    redis_url: str = "redis://localhost:6379",
    crypto_symbols: Optional[list[str]] = None,
    equity_symbols: Optional[list[str]] = None,
    futures_symbols: Optional[list[str]] = None,
):
    """Single fetch cycle — call on a cron or in a loop with sleep."""

    r = redis.from_url(redis_url, decode_responses=True)

    if crypto_symbols:
        CryptoFetcher(r, crypto_symbols, timeframe="1d", limit=365).fetch()

    if equity_symbols:
        EquityFetcher(r, equity_symbols, period="1y", interval="1d").fetch()

    if futures_symbols:
        FuturesFetcher(r, futures_symbols, period="1y", interval="1d").fetch()

    logger.info("Fetch cycle complete.")


def run_loop(interval_seconds: int = 60, **kwargs):
    """Continuous polling loop. Ctrl+C to stop."""
    logger.info("Starting ingestion loop (interval=%ds)", interval_seconds)
    while True:
        try:
            run_once(**kwargs)
        except Exception as e:
            logger.error("Fetch cycle failed: %s", e)
        time.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_loop(
        interval_seconds=60,
        crypto_symbols=["BTC/USDT", "ETH/USDT", "SOL/USDT"],
        equity_symbols=["SPY", "QQQ", "IBIT"],
        futures_symbols=["MNQ", "MES"],
    )
