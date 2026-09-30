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

Output schema v2.0 (data/price-redundancy.json) — ADDITIVE over v1.x:
legacy keys (price, change_7d, change_30d, sources, as_of, yahoo_deviation)
are byte-identical in meaning; new per-ticker keys:
  {
    "generated_at": ...,
    "schema_version": "2.0",
    "tickers": {
      "SPY":  {"price": 552.34, "change_7d": 0.012, "change_30d": -0.008,
               "sources": ["stooq", "yahoo"],
               "served_by": "stooq",
               "served_by_chain": [{"source": "stooq", "latency_ms": 812.3,
                                    "ok": true, "as_of": ...},
                                   {"source": "yahoo", ...}],
               "latency_ms": 812.3,
               "badge": "DELAYED",
               "observed_at": ..., "received_at": ...,
               "stale_after": ..., "cache_hit": false},
      ...
    },
    "stats": {
       "tickers_total": int,
       "tickers_ok": int,
       "tickers_failed": int,
       "stooq_success_rate": float,
       "yahoo_success_rate": float,
       "stooq_median_latency_ms": float,
       "yahoo_median_latency_ms": float,
    }
  }

A second artifact, data/quote-health.json, carries per-source health
(last_ok, median_latency_ms, 24h success rate, badge) for the frontend
health cards (jh-quote-health.js).
"""
from __future__ import annotations
import csv
import io
import json
import os
import statistics
import threading
import datetime as _dt
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

try:
    import quote_meta as qm  # shared freshness/latency contract (8/10)
except ImportError:
    qm = None  # degrade to legacy v1.x output rather than crash

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/price-redundancy.json")
HEALTH_KEY = os.environ.get("HEALTH_KEY", "data/quote-health.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "10"))

# Core tickers (the most-watched). Larger sets can be added via env override.
_DEFAULT_TICKERS = [
    "SPY", "QQQ", "DIA", "IWM",                        # major US indices
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA",  # mega-cap
    "GLD", "SLV", "USO", "TLT", "HYG", "LQD",         # commodities + bonds
    "BTC-USD", "ETH-USD",                              # crypto
    "EURUSD=X", "DXY",                                 # FX
    "^VIX", "^TNX",                                    # vol + 10Y yield
]
# Opt-in broad universe: TICKERS=finviz-universe
_FINVIZ_UNIVERSE = list(dict.fromkeys(_DEFAULT_TICKERS + [
    "JPM", "V", "XOM", "UNH", "JNJ", "WMT", "MA", "PG", "ORCL", "COST",
    "HD", "BAC", "NFLX", "CRM", "AMD", "ABBV", "LLY", "AVGO", "DIS", "KO",
    "ADBE", "PEP", "TMO", "ACN", "NKE", "QCOM", "TXN", "HON", "AMGN", "IBM",
    "SBUX", "GE", "CAT", "RTX", "SPGI", "MS", "GS", "AXP", "BLK", "C",
    "LMT", "AMAT", "MU", "INTC", "CSCO", "VZ", "T", "PM", "UNP", "PLD",
]))
_env_tickers = os.environ.get("TICKERS", "").strip()
if _env_tickers.lower() == "finviz-universe":
    CORE_TICKERS = _FINVIZ_UNIVERSE
else:
    CORE_TICKERS = [t.strip() for t in _env_tickers.split(",") if t.strip()] or _DEFAULT_TICKERS


def _fetch(url: str, timeout: int = 10) -> bytes:
    """Fetch raw bytes from a URL. Raises on network/HTTP errors."""
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _time_call(fn, *args, **kwargs):
    """Local fallback timer used when quote_meta is unavailable."""
    t0 = time.perf_counter()
    return fn(*args, **kwargs), round((time.perf_counter() - t0) * 1000, 1)


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


def _iso_from_epoch_s(ts):
    """Epoch seconds -> ISO-8601 UTC string, or None. Never raises."""
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def fetch_stooq(ticker: str) -> dict:
    """Stooq CSV: returns last 60 days of OHLC data."""
    sym = ticker.upper()
    key = (os.environ.get("POLYGON_KEY") or os.environ.get("POLYGON_API_KEY")
           or os.environ.get("POLY_KEY") or managed_secret(('POLYGON_API_KEY', 'POLYGON_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",)))
    # slot repurposed: Stooq blocks AWS Lambda egress, so the independent
    # cross-check source is Polygon daily aggs (same output contract).
    pmap = {"BTC-USD": "X:BTCUSD", "ETH-USD": "X:ETHUSD", "^VIX": "I:VIX",
            "^TNX": "I:TNX", "EURUSD=X": "C:EURUSD", "DXY": "I:DXY"}
    psym = pmap.get(sym, sym)
    end = _dt.date.today().isoformat()
    start = (_dt.date.today() - _dt.timedelta(days=60)).isoformat()
    url = (f"https://api.polygon.io/v2/aggs/ticker/{psym}/range/1/day/{start}/{end}"
           f"?adjusted=true&sort=asc&limit=120&apiKey={key}")
    try:
        j = json.loads(_fetch(url, timeout=10).decode("utf-8", errors="ignore"))
    except Exception as e:
        return {"ok": False, "err": str(e)}
    res = j.get("results") or []
    if not res:
        return {"ok": False, "err": "no_data"}
    rows = [{"Close": str(r.get("c")), "t": r.get("t")}
            for r in res if r.get("c") is not None]
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
        # Polygon agg "t" is ms epoch; previously this read rows[-1]["Date"]
        # which no longer exists on the row dict (always None).
        last_t = rows[-1].get("t")
        as_of = _iso_from_epoch_s(last_t / 1000) if last_t else None
        return {
            "ok": True, "price": round(latest, 4),
            "change_7d": round(chg7, 5) if chg7 is not None else None,
            "change_30d": round(chg30, 5) if chg30 is not None else None,
            "as_of": as_of,
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
        # as_of from Yahoo's own bar timestamps (was Polygon-only before 8/10)
        stamps = result.get("timestamp") or []
        as_of = _iso_from_epoch_s(stamps[-1]) if stamps else None
        return {
            "ok": True,
            "price": round(latest, 4),
            "change_7d": round(chg7, 5) if chg7 else None,
            "change_30d": round(chg30, 5) if chg30 else None,
            "as_of": as_of,
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
            "as_of": stooq.get("as_of") or yahoo.get("as_of"),
        }
    if stooq["ok"]:
        return {**stooq, "sources": ["stooq"]}
    if yahoo["ok"]:
        return {**yahoo, "sources": ["yahoo"]}
    return {"ok": False, "sources": [], "stooq_err": stooq.get("err"), "yahoo_err": yahoo.get("err")}


def _enrich_record(base: dict, winner: str | None, hops: list, now_iso: str) -> dict:
    """Additive v2.0 quote metadata over the legacy consensus record.

    Legacy keys are untouched; served_by / served_by_chain / latency_ms /
    badge / observed_at / received_at / stale_after / cache_hit are new.
    Requires quote_meta; returns base unchanged when it is unavailable.
    """
    if qm is None:
        return base
    as_of = base.get("as_of")
    chain_hops = qm.chain(hops)
    winner_ms = next((h["latency_ms"] for h in chain_hops
                      if h["source"] == winner), None)
    meta = qm.quote_record(
        price=base.get("price"),
        as_of=as_of,
        observed_at=as_of,
        received_at=now_iso,
        source=winner,
        served_by=winner,
        served_by_chain=chain_hops,
        latency_ms=winner_ms,
        badge=qm.classify_badge(as_of, "delayed"),
        stale_after=qm.stale_after_for(as_of),
        cache_hit=False,
    )
    return {**base, **meta}


def _median(values):
    """Median of a numeric list, or None when empty. Never raises."""
    try:
        return round(statistics.median(values), 1) if values else None
    except statistics.StatisticsError:
        return None


def _publish_quote_health(s3, src_stats: dict, now: datetime) -> dict:
    """Write data/quote-health.json: per-source last_ok, median latency,
    24h success rate and badge. Carries forward sample history from the
    previous artifact; fail-soft (returns {} and skips the write on error)."""
    now_iso = now.isoformat(timespec="seconds")
    now_epoch = now.timestamp()
    prev = None
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=HEALTH_KEY)["Body"].read()
        prev = json.loads(raw)
    except Exception:
        prev = None
    prev_sources = (prev or {}).get("sources") or {}

    sources = {}
    for name in ("stooq", "yahoo"):
        st = src_stats.get(name) or {}
        lat = st.get("lat") or []
        # history: previous samples + this run's, trimmed to 48h / 1000
        samples = list((prev_sources.get(name) or {}).get("samples") or [])
        for ok in st.get("oks") or []:
            samples.append([now_epoch, 1 if ok else 0])
        samples = [s for s in samples
                   if isinstance(s, list) and len(s) == 2
                   and now_epoch - s[0] <= 48 * 3600][-1000:]
        recent = [s for s in samples if now_epoch - s[0] <= 24 * 3600]
        success_24h = (round(sum(s[1] for s in recent) / len(recent), 3)
                       if recent else None)
        last_ok = None
        if st.get("ok", 0) > 0:
            last_ok = now_iso
        else:
            last_ok = (prev_sources.get(name) or {}).get("last_ok")
        badge = (qm.classify_badge(last_ok, "delayed") if qm
                 else ("OFFLINE" if not last_ok else "DELAYED"))
        sources[name] = {
            "last_ok": last_ok,
            "median_latency_ms": _median(lat),
            "success_24h": success_24h,
            "samples_24h": len(recent),
            "badge": badge,
            "samples": samples,
        }
    doc = {"generated_at": now_iso, "window": "24h", "sources": sources}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=HEALTH_KEY,
                      Body=json.dumps(doc).encode(),
                      ContentType="application/json", CacheControl="no-cache")
    except Exception as e:
        print(f"quote-health publish failed: {type(e).__name__}")
        return {}
    return doc


def lambda_handler(event, context):
    """Fetch both fallback feeds per ticker, publish consensus + health."""
    s3 = boto3.client("s3")
    started = time.time()
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat(timespec="seconds")
    tickers = [t.strip() for t in CORE_TICKERS if t.strip()]
    timer = qm.time_fetch if qm else _time_call

    out_tickers = {}
    src_stats = {"stooq": {"lat": [], "oks": [], "ok": 0},
                 "yahoo": {"lat": [], "oks": [], "ok": 0}}
    lock = threading.Lock()

    def process(t):
        """Fetch both sources for one ticker, time them, and build the v2.0 record."""
        s, s_ms = timer(fetch_stooq, t)
        y, y_ms = timer(fetch_yahoo, t)
        with lock:
            src_stats["stooq"]["lat"].append(s_ms)
            src_stats["stooq"]["oks"].append(bool(s["ok"]))
            src_stats["yahoo"]["lat"].append(y_ms)
            src_stats["yahoo"]["oks"].append(bool(y["ok"]))
            if s["ok"]:
                src_stats["stooq"]["ok"] += 1
            if y["ok"]:
                src_stats["yahoo"]["ok"] += 1
        base = consensus(s, y)
        winner = "stooq" if s["ok"] else ("yahoo" if y["ok"] else None)
        hops = [
            {"source": "stooq", "latency_ms": s_ms,
             "ok": bool(s.get("ok")), "as_of": s.get("as_of")},
            {"source": "yahoo", "latency_ms": y_ms,
             "ok": bool(y.get("ok")), "as_of": y.get("as_of")},
        ]
        return t, _enrich_record(base, winner, hops, now_iso)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        for t, res in pool.map(process, tickers):
            out_tickers[t] = res

    successful = sum(1 for v in out_tickers.values() if v.get("price") is not None)
    stooq_ok = src_stats["stooq"]["ok"]
    yahoo_ok = src_stats["yahoo"]["ok"]
    output = {
        "generated_at": now_iso,
        "schema_version": "2.0",
        "tickers": out_tickers,
        "stats": {
            "tickers_total": len(tickers),
            "tickers_ok": successful,
            "tickers_failed": len(tickers) - successful,
            "stooq_success_rate": round(stooq_ok / len(tickers), 3) if tickers else 0,
            "yahoo_success_rate": round(yahoo_ok / len(tickers), 3) if tickers else 0,
            "stooq_median_latency_ms": _median(src_stats["stooq"]["lat"]),
            "yahoo_median_latency_ms": _median(src_stats["yahoo"]["lat"]),
            "fetch_duration_s": round(time.time() - started, 1),
        },
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    _publish_quote_health(s3, src_stats, now)

    print(f"price-redundancy: {successful}/{len(tickers)} tickers ok | stooq {stooq_ok} yahoo {yahoo_ok}")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "stats": output["stats"]}),
    }
