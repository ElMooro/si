"""justhodl-polygon-futures-curves

F01 fix: NEVER request bare equity tickers (CL, ES, SI, HG, NG) through
/v2/aggs. Those are stocks. Futures Starter dated contracts live under
/futures/v1 (ticker like ESU6, CLZ5). CBOE VIX futures are outside Starter.

If the futures API is missing, 403, or a price fails the identity/band check,
the product is QUARANTINED (empty series) — never replaced with an equity print.
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from managed_secret import managed_secret

VERSION = "2.0.1"
S3_BUCKET = "justhodl-dashboard-live"
POLYGON_KEY = managed_secret(
    ("POLYGON_KEY", "POLYGON_API_KEY", "POLY_KEY", "MASSIVE_API_KEY"),
    ("/justhodl/polygon/api-key", "/justhodl/massive-api-key"),
)
HOSTS = ("https://api.massive.com", "https://api.polygon.io")
UA = {"User-Agent": "JustHodl-FuturesCurves/2.0"}

# Starter covers CME / CBOT / NYMEX / COMEX. VX is CBOE — not entitled.
PRODUCTS = {
    "S&P":    {"code": "ES", "venue": "XCME",  "entitled": True,  "band": (2000.0, 15000.0)},
    "NDX":    {"code": "NQ", "venue": "XCME",  "entitled": True,  "band": (5000.0, 40000.0)},
    "CRUDE":  {"code": "CL", "venue": "XNYM",  "entitled": True,  "band": (15.0, 250.0)},
    "GOLD":   {"code": "GC", "venue": "XCEC",  "entitled": True,  "band": (800.0, 8000.0)},
    "SILVER": {"code": "SI", "venue": "XCEC",  "entitled": True,  "band": (8.0, 120.0)},
    "COPPER": {"code": "HG", "venue": "XCEC",  "entitled": True,  "band": (1.5, 15.0), "band_cents": (150.0, 1500.0)},
    "NATGAS": {"code": "NG", "venue": "XNYM",  "entitled": True,  "band": (1.0, 30.0)},
    "VIX":    {"code": "VX", "venue": "XCBF",  "entitled": False, "band": (8.0, 90.0)},
}
# Bare 1–3 letter tickers that collided with equities in v1.
BANNED_EQUITY = {"CL", "ES", "SI", "HG", "NG", "GC", "VX", "NQ", "YM", "RTY"}
CONTRACT_RE = re.compile(
    r"^[A-Z]{1,4}[FGHJKMNQUVXZ](?:\d{1,2}|\d{4})$"
)

s3 = boto3.client("s3", region_name="us-east-1")


def _http(url, timeout=18):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read().decode("utf-8", "replace"))


def _get(path, params, timeout=18):
    q = dict(params)
    q["apiKey"] = POLYGON_KEY
    last = (0, {"error": "no host"}, HOSTS[0])
    for host in HOSTS:
        url = host + path + "?" + urllib.parse.urlencode(q)
        try:
            status, body = _http(url, timeout=timeout)
            if status == 200:
                return 200, body, host
            last = (status, body, host)
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")[:240]
            last = (e.code, {"error": raw, "status": "HTTP_%s" % e.code}, host)
            if e.code in (401, 403):
                return last
        except Exception as e:
            last = (0, {"error": str(e)[:180]}, host)
    return last[0], last[1], last[2]


def _is_dated_contract(ticker: str) -> bool:
    t = (ticker or "").upper()
    if t in BANNED_EQUITY:
        return False
    return bool(CONTRACT_RE.match(t))


def _price_ok(product: str, px) -> bool:
    if px is None:
        return False
    try:
        px = float(px)
    except (TypeError, ValueError):
        return False
    meta = PRODUCTS[product]
    lo, hi = meta["band"]
    if lo <= px <= hi:
        return True
    extra = meta.get("band_cents")
    return bool(extra and extra[0] <= px <= extra[1])


def list_active_contracts(product_code: str, n: int = 3):
    """Nearest dated contracts for a product. Empty on entitlement/identity failure."""
    params_list = [
        {"product_code": product_code, "active": "true", "limit": "50", "sort": "ticker.asc"},
        {"product_code": product_code, "limit": "50", "sort": "date.asc"},
        {"product_code": product_code, "limit": "50"},
    ]
    last_st, last_body, last_host, last_err = 0, {}, HOSTS[0], None
    rows = []
    for params in params_list:
        st, body, host = _get("/futures/v1/contracts", params)
        last_st, last_body, last_host = st, body, host
        if st in (401, 403):
            return st, [], (body or {}).get("error") or "not_entitled"
        if st == 200:
            rows = (body or {}).get("results") or []
            if rows:
                break
        last_err = (body or {}).get("error") or "contracts_http_%s" % st
    out = []
    for r in rows or []:
        ticker = str(r.get("ticker") or "").upper()
        if not _is_dated_contract(ticker):
            continue
        pc = str(r.get("product_code") or "").upper()
        if pc and pc != product_code:
            continue
        out.append({
            "ticker": ticker,
            "product_code": r.get("product_code") or product_code,
            "name": r.get("name"),
            "trading_venue": r.get("trading_venue"),
            "last_trade_date": r.get("last_trade_date") or r.get("settlement_date") or r.get("date"),
            "days_to_maturity": r.get("days_to_maturity"),
            "type": r.get("type"),
            "host": last_host,
        })
    out.sort(key=lambda c: str(c.get("last_trade_date") or c.get("ticker") or ""))
    return last_st, out[:n], last_err if not out else None


def fetch_session_aggs(ticker: str, days: int = 40):
    start = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    st, body, host = _get("/futures/v1/aggs/" + urllib.parse.quote(ticker), {
        "resolution": "1session",
        "window_start.gte": start,
        "limit": "50",
        "sort": "window_start.asc",
    })
    rows = (body or {}).get("results") if st == 200 else []
    bars = []
    for r in rows or []:
        close = r.get("close") if r.get("close") is not None else r.get("c")
        if close is None:
            continue
        bars.append({
            "t": r.get("window_start") or r.get("t"),
            "o": r.get("open", r.get("o")),
            "h": r.get("high", r.get("h")),
            "l": r.get("low", r.get("l")),
            "c": close,
            "v": r.get("volume", r.get("v")),
            "settlement": r.get("settlement_price") or r.get("settlement"),
        })
    return st, bars, (body or {}).get("error")


def compute_returns(bars):
    if len(bars) < 2:
        return {"error": "insufficient_data"}
    closes = [b["c"] for b in bars if b.get("c") is not None]
    if len(closes) < 2:
        return {"error": "no_closes"}
    last = closes[-1]

    def pct(idx):
        if abs(idx) >= len(closes):
            return None
        prior = closes[-1 - abs(idx)]
        if not prior:
            return None
        return round((closes[-1] - prior) / prior * 100, 2)

    return {
        "latest_price": round(float(last), 4),
        "return_1d_pct": pct(1),
        "return_5d_pct": pct(5),
        "return_20d_pct": pct(20),
        "n_bars": len(bars),
    }


def detect_curve_signals(product_data):
    signals = []
    oil = product_data.get("CRUDE") or []
    if len(oil) >= 2:
        p0 = oil[0].get("latest_price")
        p1 = oil[1].get("latest_price")
        if p0 and p1 and p0 > p1 + 0.5:
            signals.append("OIL_BACKWARDATION (CL1=%.2f > CL2=%.2f) — tight supply" % (p0, p1))
    for product in ("GOLD", "COPPER", "SILVER", "NATGAS", "S&P", "NDX"):
        data = product_data.get(product) or []
        if not data:
            continue
        r20 = data[0].get("return_20d_pct")
        if r20 is None:
            continue
        if r20 > 5:
            signals.append("%s_BREAKOUT (+%.1f%% 20d)" % (product, r20))
        elif r20 < -5:
            signals.append("%s_BREAKDOWN (%.1f%% 20d)" % (product, r20))
    return signals


def lambda_handler(event, context):
    t0 = time.time()
    product_data = {}
    identity = {}
    n_ok = 0
    rejected_equity = []

    if not POLYGON_KEY:
        output = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "engine": "justhodl-polygon-futures-curves",
            "version": VERSION,
            "status": "NO_KEY",
            "identity_ok": False,
            "signals": [],
            "product_data": {},
            "note": "No Massive key; equity fallback is forbidden (F01).",
        }
        s3.put_object(
            Bucket=S3_BUCKET, Key="data/polygon-futures-curves.json",
            Body=json.dumps(output, default=str).encode(),
            ContentType="application/json", CacheControl="public, max-age=300",
        )
        return {"statusCode": 200, "body": json.dumps({"ok": False, "status": "NO_KEY"})}

    for product, meta in PRODUCTS.items():
        rec = {
            "product": product,
            "product_code": meta["code"],
            "entitled": meta["entitled"],
            "status": "EMPTY",
            "contracts": [],
        }
        if not meta["entitled"]:
            rec["status"] = "NOT_ENTITLED"
            rec["note"] = "CBOE VIX futures are outside Futures Starter (CME/CBOT/NYMEX/COMEX)."
            identity[product] = rec
            product_data[product] = []
            continue

        st, contracts, err = list_active_contracts(meta["code"], n=3)
        rec["contracts_http"] = st
        if err:
            rec["error"] = str(err)[:160]
        if st in (401, 403):
            rec["status"] = "NOT_ENTITLED"
            identity[product] = rec
            product_data[product] = []
            continue
        if not contracts:
            rec["status"] = "QUARANTINED"
            rec["note"] = "No dated /futures/v1 contracts. Equity ticker %s is banned." % meta["code"]
            identity[product] = rec
            product_data[product] = []
            continue

        series = []
        for c in contracts:
            ticker = c["ticker"]
            if ticker in BANNED_EQUITY:
                rejected_equity.append(ticker)
                continue
            ast, bars, aerr = fetch_session_aggs(ticker)
            analysis = compute_returns(bars)
            analysis.update({
                "ticker": ticker,
                "product_code": c.get("product_code"),
                "trading_venue": c.get("trading_venue"),
                "last_trade_date": c.get("last_trade_date"),
                "days_to_maturity": c.get("days_to_maturity"),
                "aggs_http": ast,
                "source": "futures/v1",
            })
            px = analysis.get("latest_price")
            if analysis.get("error") or not _price_ok(product, px):
                analysis["identity_ok"] = False
                analysis["quarantine_reason"] = analysis.get("error") or (
                    "price %.4f outside %s band" % (px if px is not None else -1, product)
                )
                continue
            analysis["identity_ok"] = True
            series.append(analysis)
            rec["contracts"].append({
                "ticker": ticker,
                "venue": c.get("trading_venue"),
                "last_trade_date": c.get("last_trade_date"),
                "price": px,
            })
        if series:
            rec["status"] = "LIVE"
            n_ok += 1
        else:
            rec["status"] = "QUARANTINED"
            rec["note"] = "Dated contracts found but no identity-clean bars."
        identity[product] = rec
        product_data[product] = series

    signals = detect_curve_signals(product_data)
    identity_ok = n_ok > 0
    status = "LIVE" if identity_ok else "QUARANTINED"
    elapsed = round(time.time() - t0, 1)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "justhodl-polygon-futures-curves",
        "version": VERSION,
        "status": status,
        "identity_ok": identity_ok,
        "elapsed_s": elapsed,
        "n_products": len(PRODUCTS),
        "n_products_with_data": n_ok,
        "signals": signals if identity_ok else [],
        "product_data": product_data,
        "identity": identity,
        "rejected_equity_tickers": rejected_equity,
        "api": "/futures/v1/contracts + /futures/v1/aggs/{dated}",
        "note": (
            "F01: equity /v2/aggs tickers (CL, ES, SI, HG, NG) are banned. "
            "Empty product_data means unavailable, not a zero price. "
            "Futures Starter is scheduled to cancel 2026-10-10."
        ),
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key="data/polygon-futures-curves.json",
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json", CacheControl="public, max-age=300",
    )
    print("[futures-curves] status=%s n_ok=%s signals=%s %ss" % (status, n_ok, len(output["signals"]), elapsed))
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "status": status, "identity_ok": identity_ok,
            "n_products_with_data": n_ok, "n_signals": len(output["signals"]),
            "elapsed_s": elapsed, "version": VERSION,
        }),
    }
