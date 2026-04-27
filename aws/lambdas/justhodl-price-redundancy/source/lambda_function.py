"""
justhodl-price-redundancy — Stooq + Yahoo fallback price feed

When FMP (premium, but rate-limited) returns 429 or stale data, this Lambda
maintains a parallel price feed from two free sources:
  - Stooq (https://stooq.com/q/d/l/)        — CSV download, no key, free
  - Yahoo Finance (chart API)               — JSON, no key (best-effort), free

Output (data/price-redundancy.json) is consumed by other Lambdas as a
'consensus' price layer when their primary feed (FMP) errors out. It is
NOT meant to replace FMP — FMP has cleaner intraday data — but to provide
a circuit-breaker fallback that prevents downstream agents from getting
stale or zero values.

Tickers are pulled from a maintainable list (mirrors the daily-report-v3
master ticker list). For each ticker, we get:
  - last close price (Stooq)
  - 7d performance
  - 30d performance
  - source diversity (which feeds returned data)

Output schema (data/price-redundancy.json):
  {
    "generated_at": ...,
    "tickers": {
      "SPY":  {"price": 552.34, "change_7d": 0.012, "change_30d": -0.008,
               "sources": ["stooq", "yahoo"]},
      ...
    },
    "stats": {
       "tickers_total": int,
       "tickers_ok": int,
       "tickers_failed": int,
       "stooq_success_rate": float,
       "yahoo_success_rate": float,
    }
  }
"""
from __future__ import annotations
import csv
import io
import json
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/price-redundancy.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "10"))

# Core tickers (the most-watched). Larger sets can be added via env override.
CORE_TICKERS = os.environ.get("TICKERS", "").split(",") or [
    "SPY", "QQQ", "DIA", "IWM",                        # major US indices
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",  # mega-cap
    "GLD", "SLV", "USO", "TLT", "HYG", "LQD",         # commodities + bonds
    "BTC-USD", "ETH-USD",                              # crypto
    "EURUSD=X", "DXY",                                 # FX
    "^VIX", "^TNX",                                    # vol + 10Y yield
]


def _fetch(url: str, timeout: int = 10) -> bytes:
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _stooq_symbol(t: str) -> str:
    """Map our tickers to Stooq's symbol convention."""
    t = t.upper()
    # Stooq requires lowercase + .us suffix for US equities
    overrides = {
        "BTC-USD": "btcusd", "ETH-USD": "ethusd",
        "^VIX": "^vix", "^TNX": "^tnx",
        "EURUSD=X": "eurusd", "DXY": "dxy",
    }
    if t in overrides:
        return overrides[t]
    # ETF/stock: append .us
    return f"{t.lower()}.us"


def fetch_stooq(ticker: str) -> dict:
    """Stooq CSV: returns last 60 days of OHLC data."""
    sym = _stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
    try:
        raw = _fetch(url, timeout=10).decode("utf-8", errors="ignore")
    except Exception as e:
        return {"ok": False, "err": str(e)}

    if "no data" in raw.lower() or len(raw) < 30:
        return {"ok": False, "err": "no_data"}

    rows = list(csv.DictReader(io.StringIO(raw)))
    if not rows:
        return {"ok": False, "err": "empty"}

    # Keep last 35 trading days
    rows = rows[-35:]
    try:
        latest = float(rows[-1]["Close"])
        # 7d (5 trading days)
        if len(rows) >= 6:
            seven_ago = float(rows[-6]["Close"])
            chg7 = (latest / seven_ago) - 1
        else:
            chg7 = None
        # 30d (~22 trading days)
        if len(rows) >= 23:
            thirty_ago = float(rows[-23]["Close"])
            chg30 = (latest / thirty_ago) - 1
        else:
            chg30 = None
        return {
            "ok": True, "price": round(latest, 4),
            "change_7d": round(chg7, 5) if chg7 is not None else None,
            "change_30d": round(chg30, 5) if chg30 is not None else None,
            "as_of": rows[-1].get("Date"),
        }
    except (ValueError, KeyError, IndexError) as e:
        return {"ok": False, "err": f"parse_{type(e).__name__}"}


def fetch_yahoo(ticker: str) -> dict:
    """Yahoo Finance chart API. Best-effort fallback."""
    # Yahoo blocks default UA strings - need realistic browser UA
    sym = ticker
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=2mo"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Linux; x86_64) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as e:
        return {"ok": False, "err": str(e)}

    try:
        result = data["chart"]["result"][0]
        closes = result["indicators"]["quote"][0]["close"]
        closes = [c for c in closes if c is not None]
        if len(closes) < 2:
            return {"ok": False, "err": "too_few_closes"}
        latest = closes[-1]
        chg7 = (latest / closes[-6] - 1) if len(closes) >= 6 else None
        chg30 = (latest / closes[-23] - 1) if len(closes) >= 23 else None
        return {
            "ok": True,
            "price": round(latest, 4),
            "change_7d": round(chg7, 5) if chg7 else None,
            "change_30d": round(chg30, 5) if chg30 else None,
        }
    except (KeyError, IndexError, TypeError) as e:
        return {"ok": False, "err": f"parse_{type(e).__name__}"}


def consensus(stooq: dict, yahoo: dict) -> dict:
    """Combine the two sources. Stooq is preferred for accuracy; Yahoo confirms."""
    if stooq["ok"] and yahoo["ok"]:
        # Both worked — take Stooq's price (more reliable), confirm with Yahoo
        deviation = abs(stooq["price"] - yahoo["price"]) / max(stooq["price"], 1)
        return {
            "price": stooq["price"],
            "change_7d": stooq.get("change_7d"),
            "change_30d": stooq.get("change_30d"),
            "sources": ["stooq", "yahoo"],
            "yahoo_deviation": round(deviation, 5),
            "as_of": stooq.get("as_of"),
        }
    if stooq["ok"]:
        return {**stooq, "sources": ["stooq"]}
    if yahoo["ok"]:
        return {**yahoo, "sources": ["yahoo"]}
    return {"ok": False, "sources": [], "stooq_err": stooq.get("err"), "yahoo_err": yahoo.get("err")}


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()
    tickers = [t.strip() for t in CORE_TICKERS if t.strip()]

    out_tickers = {}
    stooq_ok = 0
    yahoo_ok = 0

    def process(t):
        nonlocal stooq_ok, yahoo_ok
        s = fetch_stooq(t)
        y = fetch_yahoo(t)
        if s["ok"]:
            stooq_ok += 1
        if y["ok"]:
            yahoo_ok += 1
        return t, consensus(s, y)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        for t, res in pool.map(process, tickers):
            out_tickers[t] = res

    successful = sum(1 for v in out_tickers.values() if v.get("price") is not None)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tickers": out_tickers,
        "stats": {
            "tickers_total": len(tickers),
            "tickers_ok": successful,
            "tickers_failed": len(tickers) - successful,
            "stooq_success_rate": round(stooq_ok / len(tickers), 3) if tickers else 0,
            "yahoo_success_rate": round(yahoo_ok / len(tickers), 3) if tickers else 0,
            "fetch_duration_s": round(time.time() - started, 1),
        },
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"price-redundancy: {successful}/{len(tickers)} tickers ok | stooq {stooq_ok} yahoo {yahoo_ok}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "stats": output["stats"]}),
    }
