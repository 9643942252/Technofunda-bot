#!/usr/bin/env python3
"""
technofunda_bot.py
===================
TechnoFunda scanning bot with:
  - Multi-broker adapter layer (Zerodha / Groww / Fyers) for data + order execution
  - TechnoFundaAgent: fundamental + technical multi-condition screener
  - NewsAgent: FREE news scan (RSS: Moneycontrol, Economic Times, Google News)
    + keyword-based sentiment (no paid NLP API)
  - VolumeSurgeAgent: multi-timeframe volume surge + large "order"/bulk-deal detection
    using FREE sources (yfinance intraday candles + NSE public bulk/block deal pages)
  - Orchestrator: council-of-agents pattern matching groww_swing_live.py / orb_agentic.py

DESIGN NOTES / ASSUMPTIONS (stated up front, no clarifying-question round trip):
  - Zerodha Kite Connect API access itself is a PAID subscription (~Rs 2000/mo) even
    though this script's calls to it are free of extra cost beyond that. Groww API
    and Fyers API are free. All three are wired through one BrokerAdapter interface
    so you can enable/disable per broker in CONFIG without touching agent logic.
  - Scan/technical/fundamental DATA comes from yfinance (free, delayed) rather than
    broker feeds, since brokers differ in symbol formats and rate limits for bulk
    screening. Brokers are used for: live LTP confirmation, live positions, and
    ORDER PLACEMENT only.
  - "Large orders" = NSE bulk/block deal disclosures (free, public, but end-of-day /
    T+ delayed, NOT real-time tick-by-tick large-order flow — true real-time order
    book depth requires a paid feed). Flagged clearly in output as EOD-lag data.
  - Volume surge is computed on FREE yfinance intraday bars: 5m, 15m, 1h, 1d.
  - No live order is placed automatically; ORDER_MODE defaults to "PAPER". Flip to
    "LIVE" only after you've reviewed signals — matches the guardrail pattern used
    in groww_swing_live.py (GuardianAgent / circuit breaker).
  - Single file, stdlib + yfinance + feedparser + requests + pandas. Everything else
    (SQLite persistence, rate limiting) is inline, no external service needed.

Install:
    pip install yfinance pandas feedparser requests --break-system-packages

Run:
    python3 technofunda_bot.py
"""

import os
import re
import sys
import time
import json
import sqlite3
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

import http.server
import socketserver

import requests

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import feedparser
except ImportError:
    feedparser = None

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────

CONFIG = {
    "ORDER_MODE": os.getenv("ORDER_MODE", "PAPER"),   # "PAPER" or "LIVE"
    "PORT": int(os.getenv("PORT", "8080")),           # Railway injects PORT
    "DB_PATH": "/tmp/technofunda_bot.db",
    "UNIVERSE": [                        # seed universe; extend as needed
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "SBIN", "AXISBANK",
        "LT", "ITC", "KOTAKBANK", "BHARTIARTL", "MARUTI", "SYRMA", "OMAXAUTO",
        "POWERGRID", "TATAMOTORS", "SUNPHARMA", "ADANIENT", "BAJFINANCE",
    ],
    "SCAN_INTERVAL_SECONDS": 900,        # 15 min scan cycle
    "VOLUME_TIMEFRAMES": ["5m", "15m", "1h", "1d"],
    "VOLUME_SURGE_MULTIPLIER": 2.5,      # current bar vol vs rolling avg
    "VOLUME_LOOKBACK_BARS": 20,
    "NEWS_FEEDS": {
        "moneycontrol": "https://www.moneycontrol.com/rss/marketreports.xml",
        "economictimes": "https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
        "google_news_nse": "https://news.google.com/rss/search?q={symbol}+NSE&hl=en-IN&gl=IN&ceid=IN:en",
    },
    "NEWS_LOOKBACK_HOURS": 24,
    "FUNDAMENTAL_THRESHOLDS": {
        "max_peg": 1.5,
        "min_roe": 0.15,
        "max_debt_to_equity": 1.0,
        "min_piotroski_proxy": 5,   # 0-9 proxy score, see FundamentalAgent
    },
    "TECHNICAL_THRESHOLDS": {
        "rsi_low": 40,
        "rsi_high": 65,
        "min_adx": 20,
    },
    "BROKERS": {
        # Fill credentials via env vars, never hardcode. Each broker can be
        # independently enabled; missing/blank creds => adapter auto-disables.
        "zerodha": {
            "enabled": bool(os.getenv("ZERODHA_API_KEY")),
            "api_key": os.getenv("ZERODHA_API_KEY", ""),
            "access_token": os.getenv("ZERODHA_ACCESS_TOKEN", ""),
        },
        "groww": {
            "enabled": bool(os.getenv("GROWW_API_KEY")),
            "api_key": os.getenv("GROWW_API_KEY", ""),
            "totp_secret": os.getenv("GROWW_TOTP_SECRET", ""),
        },
        "fyers": {
            "enabled": bool(os.getenv("FYERS_APP_ID")),
            "app_id": os.getenv("FYERS_APP_ID", ""),
            "access_token": os.getenv("FYERS_ACCESS_TOKEN", ""),
        },
    },
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("technofunda_bot")


# ──────────────────────────────────────────────────────────────────────────
# PERSISTENCE
# ──────────────────────────────────────────────────────────────────────────

class Store:
    """Thin SQLite wrapper, mirrors the persistence pattern used in the swing bot."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._init_schema()

    def _conn(self):
        return sqlite3.connect(self.path, timeout=30)

    def _init_schema(self):
        with self._lock, self._conn() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT, ts TEXT,
                    fundamental_pass INTEGER, technical_pass INTEGER,
                    volume_surge_tf TEXT, volume_surge_ratio REAL,
                    news_sentiment REAL, news_headline TEXT,
                    bulk_deal_flag INTEGER, composite_score REAL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT, ts TEXT, side TEXT, qty INTEGER,
                    broker TEXT, mode TEXT, status TEXT, ref TEXT
                )
            """)

    def save_signal(self, row: dict):
        with self._lock, self._conn() as c:
            c.execute("""
                INSERT INTO signals (symbol, ts, fundamental_pass, technical_pass,
                    volume_surge_tf, volume_surge_ratio, news_sentiment, news_headline,
                    bulk_deal_flag, composite_score)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                row["symbol"], row["ts"], int(row["fundamental_pass"]), int(row["technical_pass"]),
                row.get("volume_surge_tf", ""), row.get("volume_surge_ratio", 0.0),
                row.get("news_sentiment", 0.0), row.get("news_headline", ""),
                int(row.get("bulk_deal_flag", False)), row.get("composite_score", 0.0),
            ))

    def save_order(self, row: dict):
        with self._lock, self._conn() as c:
            c.execute("""
                INSERT INTO orders (symbol, ts, side, qty, broker, mode, status, ref)
                VALUES (?,?,?,?,?,?,?,?)
            """, (row["symbol"], row["ts"], row["side"], row["qty"],
                  row["broker"], row["mode"], row["status"], row.get("ref", "")))


# ──────────────────────────────────────────────────────────────────────────
# BROKER ADAPTER LAYER (unified interface over Zerodha / Groww / Fyers)
# ──────────────────────────────────────────────────────────────────────────

class BrokerAdapter:
    """Base interface every broker adapter implements. Keep signatures identical
    across brokers so the Orchestrator never branches on broker type."""

    name = "base"

    def __init__(self, creds: dict):
        self.creds = creds
        self.connected = False

    def connect(self) -> bool:
        raise NotImplementedError

    def get_ltp(self, symbol: str) -> Optional[float]:
        raise NotImplementedError

    def get_positions(self) -> list:
        raise NotImplementedError

    def place_order(self, symbol: str, side: str, qty: int, order_type: str = "MARKET") -> dict:
        raise NotImplementedError


class ZerodhaAdapter(BrokerAdapter):
    name = "zerodha"

    def connect(self) -> bool:
        if not self.creds.get("api_key") or not self.creds.get("access_token"):
            log.info("Zerodha: no credentials, adapter disabled")
            return False
        try:
            from kiteconnect import KiteConnect  # pip install kiteconnect
            self.kite = KiteConnect(api_key=self.creds["api_key"])
            self.kite.set_access_token(self.creds["access_token"])
            self.connected = True
            log.info("Zerodha adapter connected")
            return True
        except Exception as e:
            log.warning(f"Zerodha connect failed: {e}")
            return False

    def get_ltp(self, symbol: str) -> Optional[float]:
        if not self.connected:
            return None
        try:
            q = self.kite.ltp([f"NSE:{symbol}"])
            return q[f"NSE:{symbol}"]["last_price"]
        except Exception as e:
            log.warning(f"Zerodha LTP failed for {symbol}: {e}")
            return None

    def get_positions(self) -> list:
        if not self.connected:
            return []
        try:
            return self.kite.positions().get("net", [])
        except Exception as e:
            log.warning(f"Zerodha positions failed: {e}")
            return []

    def place_order(self, symbol: str, side: str, qty: int, order_type: str = "MARKET") -> dict:
        if not self.connected:
            return {"status": "SKIPPED", "reason": "not connected"}
        try:
            from kiteconnect import KiteConnect
            order_id = self.kite.place_order(
                variety="regular", exchange="NSE", tradingsymbol=symbol,
                transaction_type=side.upper(), quantity=qty,
                order_type="MARKET" if order_type == "MARKET" else "LIMIT",
                product="CNC",
            )
            return {"status": "PLACED", "ref": order_id}
        except Exception as e:
            log.warning(f"Zerodha order failed for {symbol}: {e}")
            return {"status": "FAILED", "reason": str(e)}


class GrowwAdapter(BrokerAdapter):
    name = "groww"

    def connect(self) -> bool:
        if not self.creds.get("api_key"):
            log.info("Groww: no credentials, adapter disabled")
            return False
        try:
            from growwapi import GrowwAPI  # pip install growwapi
            self.client = GrowwAPI(self.creds["api_key"])
            self.connected = True
            log.info("Groww adapter connected")
            return True
        except Exception as e:
            log.warning(f"Groww connect failed: {e}")
            return False

    def get_ltp(self, symbol: str) -> Optional[float]:
        if not self.connected:
            return None
        try:
            q = self.client.get_quote(exchange="NSE", trading_symbol=symbol, segment="CASH")
            return q.get("last_price")
        except Exception as e:
            log.warning(f"Groww LTP failed for {symbol}: {e}")
            return None

    def get_positions(self) -> list:
        if not self.connected:
            return []
        try:
            return self.client.get_positions() or []
        except Exception as e:
            log.warning(f"Groww positions failed: {e}")
            return []

    def place_order(self, symbol: str, side: str, qty: int, order_type: str = "MARKET") -> dict:
        if not self.connected:
            return {"status": "SKIPPED", "reason": "not connected"}
        try:
            resp = self.client.place_order(
                trading_symbol=symbol, exchange="NSE", segment="CASH",
                transaction_type=side.upper(), quantity=qty,
                order_type="MARKET", product="CNC", validity="DAY",
            )
            return {"status": "PLACED", "ref": resp.get("order_id", "")}
        except Exception as e:
            log.warning(f"Groww order failed for {symbol}: {e}")
            return {"status": "FAILED", "reason": str(e)}


class FyersAdapter(BrokerAdapter):
    name = "fyers"

    def connect(self) -> bool:
        if not self.creds.get("app_id") or not self.creds.get("access_token"):
            log.info("Fyers: no credentials, adapter disabled")
            return False
        try:
            from fyers_apiv3 import fyersModel  # pip install fyers-apiv3
            self.client = fyersModel.FyersModel(
                client_id=self.creds["app_id"], token=self.creds["access_token"], is_async=False
            )
            self.connected = True
            log.info("Fyers adapter connected")
            return True
        except Exception as e:
            log.warning(f"Fyers connect failed: {e}")
            return False

    def get_ltp(self, symbol: str) -> Optional[float]:
        if not self.connected:
            return None
        try:
            resp = self.client.quotes({"symbols": f"NSE:{symbol}-EQ"})
            return resp["d"][0]["v"]["lp"]
        except Exception as e:
            log.warning(f"Fyers LTP failed for {symbol}: {e}")
            return None

    def get_positions(self) -> list:
        if not self.connected:
            return []
        try:
            resp = self.client.positions()
            return resp.get("netPositions", [])
        except Exception as e:
            log.warning(f"Fyers positions failed: {e}")
            return []

    def place_order(self, symbol: str, side: str, qty: int, order_type: str = "MARKET") -> dict:
        if not self.connected:
            return {"status": "SKIPPED", "reason": "not connected"}
        try:
            resp = self.client.place_order(data={
                "symbol": f"NSE:{symbol}-EQ", "qty": qty, "type": 2,  # 2 = market
                "side": 1 if side.upper() == "BUY" else -1,
                "productType": "CNC", "validity": "DAY",
            })
            return {"status": "PLACED", "ref": resp.get("id", "")}
        except Exception as e:
            log.warning(f"Fyers order failed for {symbol}: {e}")
            return {"status": "FAILED", "reason": str(e)}


class BrokerPool:
    """Holds all enabled broker adapters, used for cross-broker confirmation
    (e.g. LTP sanity check) and for routing order execution."""

    ADAPTER_CLASSES = {"zerodha": ZerodhaAdapter, "groww": GrowwAdapter, "fyers": FyersAdapter}

    def __init__(self, broker_config: dict):
        self.adapters: dict[str, BrokerAdapter] = {}
        for name, creds in broker_config.items():
            if not creds.get("enabled"):
                continue
            cls = self.ADAPTER_CLASSES[name]
            adapter = cls(creds)
            if adapter.connect():
                self.adapters[name] = adapter

    def any_connected(self) -> bool:
        return len(self.adapters) > 0

    def get_ltp_consensus(self, symbol: str) -> Optional[float]:
        """Average LTP across connected brokers; falls back to yfinance if none connected."""
        prices = [a.get_ltp(symbol) for a in self.adapters.values()]
        prices = [p for p in prices if p]
        if prices:
            return sum(prices) / len(prices)
        return YFinanceSource.get_ltp(symbol)

    def route_order(self, symbol: str, side: str, qty: int, preferred: Optional[str] = None) -> dict:
        if not self.adapters:
            return {"status": "SKIPPED", "reason": "no broker connected"}
        broker = self.adapters.get(preferred) or next(iter(self.adapters.values()))
        return {**broker.place_order(symbol, side, qty), "broker": broker.name}


# ──────────────────────────────────────────────────────────────────────────
# FREE DATA SOURCE: yfinance (fundamentals + OHLCV, all timeframes)
# ──────────────────────────────────────────────────────────────────────────

class YFinanceSource:
    """All calls free / no API key. NSE symbols need the .NS suffix."""

    @staticmethod
    def _ticker(symbol: str):
        return yf.Ticker(f"{symbol}.NS")

    @staticmethod
    def get_ltp(symbol: str) -> Optional[float]:
        if yf is None:
            return None
        try:
            hist = YFinanceSource._ticker(symbol).history(period="1d", interval="1m")
            return float(hist["Close"].iloc[-1]) if not hist.empty else None
        except Exception as e:
            log.warning(f"yfinance LTP failed for {symbol}: {e}")
            return None

    @staticmethod
    def get_fundamentals(symbol: str) -> dict:
        if yf is None:
            return {}
        try:
            info = YFinanceSource._ticker(symbol).info
            return {
                "peg": info.get("pegRatio"),
                "roe": info.get("returnOnEquity"),
                "debt_to_equity": (info.get("debtToEquity") or 0) / 100.0 if info.get("debtToEquity") else None,
                "revenue_growth": info.get("revenueGrowth"),
                "earnings_growth": info.get("earningsGrowth"),
                "gross_margin": info.get("grossMargins"),
                "current_ratio": info.get("currentRatio"),
            }
        except Exception as e:
            log.warning(f"yfinance fundamentals failed for {symbol}: {e}")
            return {}

    @staticmethod
    def get_candles(symbol: str, interval: str, period: str) -> Optional[pd.DataFrame]:
        if yf is None:
            return None
        try:
            df = YFinanceSource._ticker(symbol).history(period=period, interval=interval)
            return df if not df.empty else None
        except Exception as e:
            log.warning(f"yfinance candles failed for {symbol} [{interval}]: {e}")
            return None


# period needed per interval so yfinance doesn't reject the request
_PERIOD_FOR_INTERVAL = {"5m": "5d", "15m": "5d", "1h": "1mo", "1d": "6mo"}


# ──────────────────────────────────────────────────────────────────────────
# AGENT: TechnoFundaAgent (fundamental + technical multi-condition screen)
# ──────────────────────────────────────────────────────────────────────────

class TechnoFundaAgent:
    """Mirrors the 15-criteria Screener.in-style filter used in the swing bot's
    FundamentalAgent, plus a technical layer (golden crossover, RSI, ADX)."""

    def __init__(self, thresholds: dict, tech_thresholds: dict):
        self.f = thresholds
        self.t = tech_thresholds

    def screen_fundamentals(self, symbol: str) -> tuple[bool, dict]:
        data = YFinanceSource.get_fundamentals(symbol)
        if not data:
            return False, data
        checks = []
        if data.get("peg") is not None:
            checks.append(data["peg"] <= self.f["max_peg"])
        if data.get("roe") is not None:
            checks.append(data["roe"] >= self.f["min_roe"])
        if data.get("debt_to_equity") is not None:
            checks.append(data["debt_to_equity"] <= self.f["max_debt_to_equity"])
        piotroski_proxy = sum([
            (data.get("roe") or 0) > 0,
            (data.get("earnings_growth") or 0) > 0,
            (data.get("revenue_growth") or 0) > 0,
            (data.get("gross_margin") or 0) > 0.2,
            (data.get("current_ratio") or 0) > 1,
            (data.get("debt_to_equity") or 99) < 1,
        ])
        checks.append(piotroski_proxy >= min(self.f["min_piotroski_proxy"], 6))
        passed = bool(checks) and (sum(checks) / len(checks)) >= 0.6
        data["piotroski_proxy"] = piotroski_proxy
        return passed, data

    @staticmethod
    def _rsi(series: pd.Series, period: int = 14) -> float:
        delta = series.diff()
        gain = delta.clip(lower=0).rolling(period).mean()
        loss = (-delta.clip(upper=0)).rolling(period).mean()
        rs = gain / loss.replace(0, 1e-9)
        return float((100 - 100 / (1 + rs)).iloc[-1])

    @staticmethod
    def _adx(df: pd.DataFrame, period: int = 14) -> float:
        high, low, close = df["High"], df["Low"], df["Close"]
        plus_dm = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        tr = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(period).mean()
        plus_di = 100 * (plus_dm.rolling(period).mean() / atr.replace(0, 1e-9))
        minus_di = 100 * (minus_dm.rolling(period).mean() / atr.replace(0, 1e-9))
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1e-9)
        return float(dx.rolling(period).mean().iloc[-1])

    def screen_technicals(self, symbol: str) -> tuple[bool, dict]:
        df = YFinanceSource.get_candles(symbol, "1d", "1y")
        if df is None or len(df) < 60:
            return False, {}
        close = df["Close"]
        sma50 = close.rolling(50).mean().iloc[-1]
        sma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else None
        rsi = self._rsi(close)
        adx = self._adx(df)
        golden_cross = sma200 is not None and sma50 > sma200
        checks = [
            self.t["rsi_low"] <= rsi <= self.t["rsi_high"],
            adx >= self.t["min_adx"],
        ]
        if sma200 is not None:
            checks.append(golden_cross)
        passed = sum(checks) >= 2
        return passed, {"rsi": round(rsi, 1), "adx": round(adx, 1), "golden_cross": golden_cross}


# ──────────────────────────────────────────────────────────────────────────
# AGENT: VolumeSurgeAgent (multi-timeframe surge + free large-order proxy)
# ──────────────────────────────────────────────────────────────────────────

class VolumeSurgeAgent:
    def __init__(self, timeframes: list, multiplier: float, lookback: int):
        self.timeframes = timeframes
        self.multiplier = multiplier
        self.lookback = lookback

    def check_surge(self, symbol: str) -> dict:
        """Returns {timeframe: ratio} for every timeframe where current volume
        exceeds `multiplier` x the rolling average of the prior `lookback` bars."""
        surges = {}
        for tf in self.timeframes:
            period = _PERIOD_FOR_INTERVAL.get(tf, "5d")
            df = YFinanceSource.get_candles(symbol, tf, period)
            if df is None or len(df) < self.lookback + 1:
                continue
            vol = df["Volume"]
            avg = vol.iloc[-(self.lookback + 1):-1].mean()
            current = vol.iloc[-1]
            if avg > 0 and current >= self.multiplier * avg:
                surges[tf] = round(current / avg, 2)
        return surges

    @staticmethod
    def check_bulk_block_deals(symbol: str) -> bool:
        """FREE NSE public bulk/block deal disclosure check. This is EOD/T+lag
        data, NOT live tick-by-tick large-order flow (that needs a paid feed)."""
        try:
            url = "https://www.nseindia.com/api/historical/bulk-deals"
            headers = {"User-Agent": "Mozilla/5.0"}
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers, timeout=5)  # sets cookies
            resp = session.get(url, headers=headers, timeout=5)
            if resp.status_code != 200:
                return False
            data = resp.json().get("data", [])
            return any(symbol.upper() in (row.get("BD_SYMBOL", "") or "").upper() for row in data)
        except Exception as e:
            log.warning(f"Bulk deal check failed for {symbol} (NSE endpoint often blocks bots): {e}")
            return False


# ──────────────────────────────────────────────────────────────────────────
# AGENT: NewsAgent (free RSS + keyword sentiment)
# ──────────────────────────────────────────────────────────────────────────

_POSITIVE_WORDS = {
    "surge", "rally", "beat", "beats", "upgrade", "upgraded", "outperform",
    "profit", "growth", "record", "bullish", "buy", "strong", "expansion",
    "wins", "order win", "raises guidance", "positive", "jump", "soar",
}
_NEGATIVE_WORDS = {
    "fall", "falls", "downgrade", "downgraded", "miss", "misses", "loss",
    "bearish", "sell", "weak", "decline", "probe", "fraud", "default",
    "resign", "resignation", "penalty", "fine", "negative", "plunge", "crash",
}


class NewsAgent:
    def __init__(self, feeds: dict, lookback_hours: int):
        self.feeds = feeds
        self.lookback_hours = lookback_hours

    @staticmethod
    def _score(text: str) -> float:
        text_l = text.lower()
        pos = sum(1 for w in _POSITIVE_WORDS if w in text_l)
        neg = sum(1 for w in _NEGATIVE_WORDS if w in text_l)
        if pos + neg == 0:
            return 0.0
        return round((pos - neg) / (pos + neg), 2)

    def scan(self, symbol: str) -> list:
        if feedparser is None:
            log.warning("feedparser not installed; news scan skipped")
            return []
        cutoff = datetime.utcnow() - timedelta(hours=self.lookback_hours)
        hits = []
        # symbol-specific Google News search always run; sector feeds filtered by name match
        feed_urls = dict(self.feeds)
        feed_urls["google_news_symbol"] = self.feeds["google_news_nse"].format(symbol=symbol)
        for source, url in feed_urls.items():
            try:
                parsed = feedparser.parse(url)
            except Exception as e:
                log.warning(f"News feed {source} failed: {e}")
                continue
            for entry in parsed.entries[:20]:
                title = entry.get("title", "")
                if source != "google_news_symbol" and symbol.upper() not in title.upper():
                    continue
                published = entry.get("published_parsed")
                if published:
                    pub_dt = datetime(*published[:6])
                    if pub_dt < cutoff:
                        continue
                hits.append({
                    "source": source, "headline": title,
                    "sentiment": self._score(title),
                    "link": entry.get("link", ""),
                })
        return hits


# ──────────────────────────────────────────────────────────────────────────
# STATUS SERVER (Railway health check + JSON endpoint for PWA polling)
# ──────────────────────────────────────────────────────────────────────────

class StatusServer:
    """Minimal stdlib HTTP server. Railway (and similar PaaS) expects a bound
    port to consider the service healthy. Also gives the [[financial-app]]
    PWA a JSON endpoint to poll for latest signals -> browser notifications."""

    def __init__(self, store: "Store", port: int):
        self.store = store
        self.port = port
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass  # silence default access logs, we use `log` instead

            def do_GET(self):
                if self.path == "/health":
                    self._json(200, {"status": "ok", "ts": datetime.utcnow().isoformat()})
                elif self.path.startswith("/signals"):
                    with outer.store._lock, outer.store._conn() as c:
                        rows = c.execute(
                            "SELECT symbol, ts, composite_score, fundamental_pass, "
                            "technical_pass, volume_surge_tf, bulk_deal_flag, news_headline "
                            "FROM signals ORDER BY id DESC LIMIT 50"
                        ).fetchall()
                    cols = ["symbol", "ts", "composite_score", "fundamental_pass",
                            "technical_pass", "volume_surge_tf", "bulk_deal_flag", "news_headline"]
                    self._json(200, {"signals": [dict(zip(cols, r)) for r in rows]})
                else:
                    self._json(404, {"error": "not found", "routes": ["/health", "/signals"]})

            def _json(self, code, payload):
                body = json.dumps(payload).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._handler = Handler

    def start_background(self):
        httpd = socketserver.ThreadingTCPServer(("0.0.0.0", self.port), self._handler)
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        log.info(f"Status server listening on :{self.port} (/health, /signals)")
        return httpd


# ──────────────────────────────────────────────────────────────────────────
# ORCHESTRATOR
# ──────────────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    symbol: str
    fundamental_pass: bool = False
    technical_pass: bool = False
    volume_surges: dict = field(default_factory=dict)
    bulk_deal_flag: bool = False
    news_hits: list = field(default_factory=list)
    composite_score: float = 0.0


class Orchestrator:
    def __init__(self, config: dict):
        self.cfg = config
        self.store = Store(config["DB_PATH"])
        self.broker_pool = BrokerPool(config["BROKERS"])
        self.techno_agent = TechnoFundaAgent(
            config["FUNDAMENTAL_THRESHOLDS"], config["TECHNICAL_THRESHOLDS"]
        )
        self.volume_agent = VolumeSurgeAgent(
            config["VOLUME_TIMEFRAMES"], config["VOLUME_SURGE_MULTIPLIER"], config["VOLUME_LOOKBACK_BARS"]
        )
        self.news_agent = NewsAgent(config["NEWS_FEEDS"], config["NEWS_LOOKBACK_HOURS"])
        log.info(f"Orchestrator ready. Brokers connected: {list(self.broker_pool.adapters.keys()) or 'NONE (paper/data-only mode)'}")

    def _composite_score(self, sig: Signal) -> float:
        score = 0.0
        score += 30 if sig.fundamental_pass else 0
        score += 25 if sig.technical_pass else 0
        score += min(len(sig.volume_surges) * 10, 25)
        score += 10 if sig.bulk_deal_flag else 0
        if sig.news_hits:
            avg_sent = sum(h["sentiment"] for h in sig.news_hits) / len(sig.news_hits)
            score += max(0, avg_sent * 10)
        return round(score, 1)

    def scan_symbol(self, symbol: str) -> Signal:
        sig = Signal(symbol=symbol)
        sig.fundamental_pass, fdata = self.techno_agent.screen_fundamentals(symbol)
        sig.technical_pass, tdata = self.techno_agent.screen_technicals(symbol)
        sig.volume_surges = self.volume_agent.check_surge(symbol)
        sig.bulk_deal_flag = self.volume_agent.check_bulk_block_deals(symbol)
        sig.news_hits = self.news_agent.scan(symbol)
        sig.composite_score = self._composite_score(sig)

        self.store.save_signal({
            "symbol": symbol, "ts": datetime.utcnow().isoformat(),
            "fundamental_pass": sig.fundamental_pass, "technical_pass": sig.technical_pass,
            "volume_surge_tf": json.dumps(sig.volume_surges),
            "volume_surge_ratio": max(sig.volume_surges.values()) if sig.volume_surges else 0.0,
            "news_sentiment": (sum(h["sentiment"] for h in sig.news_hits) / len(sig.news_hits)) if sig.news_hits else 0.0,
            "news_headline": sig.news_hits[0]["headline"] if sig.news_hits else "",
            "bulk_deal_flag": sig.bulk_deal_flag, "composite_score": sig.composite_score,
        })
        return sig

    def act_on_signal(self, sig: Signal, qty: int = 1):
        """Only acts if composite score clears a high bar AND fundamentals+technicals
        both pass. In PAPER mode, logs the intended order instead of routing it."""
        if sig.composite_score < 70 or not (sig.fundamental_pass and sig.technical_pass):
            return
        if self.cfg["ORDER_MODE"] == "PAPER" or not self.broker_pool.any_connected():
            log.info(f"[PAPER] Would BUY {qty} {sig.symbol} (score={sig.composite_score})")
            status = "PAPER_LOGGED"
            broker = "none"
        else:
            result = self.broker_pool.route_order(sig.symbol, "BUY", qty)
            status = result.get("status", "UNKNOWN")
            broker = result.get("broker", "none")
        self.store.save_order({
            "symbol": sig.symbol, "ts": datetime.utcnow().isoformat(), "side": "BUY",
            "qty": qty, "broker": broker, "mode": self.cfg["ORDER_MODE"], "status": status,
        })

    def run_once(self):
        results = []
        for symbol in self.cfg["UNIVERSE"]:
            try:
                sig = self.scan_symbol(symbol)
                results.append(sig)
                if sig.composite_score >= 50:
                    surge_str = ", ".join(f"{tf}:{r}x" for tf, r in sig.volume_surges.items()) or "none"
                    log.info(
                        f"{symbol:12s} score={sig.composite_score:5.1f} "
                        f"fund={sig.fundamental_pass} tech={sig.technical_pass} "
                        f"surge=[{surge_str}] bulk_deal={sig.bulk_deal_flag} "
                        f"news_hits={len(sig.news_hits)}"
                    )
                self.act_on_signal(sig)
            except Exception as e:
                log.error(f"Scan failed for {symbol}: {e}")
            time.sleep(1)  # be polite to free endpoints
        return sorted(results, key=lambda s: s.composite_score, reverse=True)

    def run_forever(self):
        while True:
            log.info("=== Scan cycle start ===")
            top = self.run_once()
            log.info("=== Scan cycle end. Top 5 by composite score: ===")
            for sig in top[:5]:
                log.info(f"  {sig.symbol}: {sig.composite_score}")
            time.sleep(self.cfg["SCAN_INTERVAL_SECONDS"])


# ──────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ──────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    missing = [pkg for pkg, mod in [("yfinance", yf), ("feedparser", feedparser)] if mod is None]
    if missing:
        log.warning(f"Missing optional packages {missing} — install with: "
                    f"pip install {' '.join(missing)} --break-system-packages")

    orch = Orchestrator(CONFIG)
    StatusServer(orch.store, CONFIG["PORT"]).start_background()
    if "--once" in sys.argv:
        orch.run_once()
    else:
        orch.run_forever()
