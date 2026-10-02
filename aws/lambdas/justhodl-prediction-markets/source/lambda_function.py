"""
justhodl-prediction-markets — macro prediction-market odds aggregator

Pulls public (keyless) prediction-market data from two sources and turns
it into macro signals for the terminal:

  • Kalshi public REST (no key for public data)
      GET /trade-api/v2/events?limit=200            -> event list
      GET /trade-api/v2/markets?event_ticker=<t>    -> per-event market prices
  • Polymarket Gamma API (free, no key)
      GET /events?limit=100&closed=false           -> events, each with markets

Only macro/political events are kept: Fed decisions, rate cuts, recession,
CPI/inflation, elections, unemployment, GDP, treasuries, tariffs.

For each relevant market: question/title, yes_prob (0-1 probability),
volume, close_date.

macro_signals pulls the highest-volume matching market for:
  fed_cut_odds, recession_odds, fed_hike_odds, inflation_above_target_odds,
  election_winner, election_odds, unemployment_odds

History: the previous file in S3 is read; its current snapshot is appended
to a history array (capped at 500 entries) so odds can be charted over time.

FAIL-SOFT: everything is wrapped in try/except; the handler never crashes
and always writes data/prediction-markets.json, even if partial/empty.

OUTPUT: data/prediction-markets.json
"""
import io
import json
import os
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/prediction-markets.json"

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
POLY_BASE = "https://gamma-api.polymarket.com"

UA = {"User-Agent": "JustHodl-PredictionMarkets/1.0 (+https://justhodl.ai)"}
HTTP_TIMEOUT = 15
MAX_KALSHI_EVENTS = 40          # bound per-event market fetches
HISTORY_CAP = 500

S3 = boto3.client("s3", region_name=REGION)

# Keywords that mark an event/market as macro-relevant.
MACRO_KEYWORDS = [
    "fed", "fomc", "rate cut", "interest rate", "powell",
    "recession", "cpi", "inflation", "pce", "deflation",
    "election", "president", "congress", "senate", "house",
    "unemployment", "jobs report", "nonfarm", "nfp",
    "gdp", "treasury", "yield", "tariff", "debt ceiling",
]

# Polymarket event categories are free-form; these tag/category hints help.
POLY_TAG_HINTS = [
    "politics", "election", "economy", "economics", "macro",
    "fed", "finance", "trump", "congress", "senate",
]


def iso_now():
    return time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())


def log(msg):
    print("[pred-mkt] " + str(msg))


def fetch_json(url, timeout=HTTP_TIMEOUT):
    """GET url, parse JSON. Raises on any failure (callers catch)."""
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def is_macro(text):
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in MACRO_KEYWORDS)


# ---------------------------------------------------------------- Kalshi ---

def kalshi_events():
    """Return macro-relevant Kalshi events (event_ticker, title)."""
    try:
        url = KALSHI_BASE + "/events?limit=200"
        d = fetch_json(url)
        events = d.get("events") or []
    except Exception as e:
        log("kalshi events fetch failed: " + str(e))
        return []

    out = []
    for ev in events:
        ticker = ev.get("event_ticker") or ""
        title = ev.get("title") or ""
        if is_macro(ticker.replace("_", " ")) or is_macro(title):
            out.append({"event_ticker": ticker, "title": title})
        if len(out) >= MAX_KALSHI_EVENTS:
            break
    log("kalshi macro events: %d/%d" % (len(out), len(events)))
    return out


def _f(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def kalshi_market_price(m):
    """Kalshi *_dollars fields are quoted in dollars (0.00-1.00) -> probability."""
    lp = _f(m.get("last_price_dollars")) or _f(m.get("last_price"))
    if lp is not None:
        return max(0.0, min(1.0, lp if lp <= 1.0 else lp / 100.0))
    bid = _f(m.get("yes_bid_dollars")) or _f(m.get("yes_bid"))
    ask = _f(m.get("yes_ask_dollars")) or _f(m.get("yes_ask"))
    if bid is not None and ask is not None:
        if bid > 1.0:  # legacy cents quoting
            bid, ask = bid / 100.0, ask / 100.0
        return max(0.0, min(1.0, (bid + ask) / 2.0))
    return None


def kalshi_market_volume(m):
    return _f(m.get("volume_fp")) or _f(m.get("volume_24h_fp")) or _f(m.get("volume"))


def kalshi_event_markets(ev):
    """Fetch markets for one Kalshi event; return macro-relevant market dicts."""
    ticker = ev["event_ticker"]
    out = []
    try:
        url = KALSHI_BASE + "/markets?event_ticker=" + urllib.parse.quote(ticker)
        d = fetch_json(url)
        markets = d.get("markets") or []
    except Exception as e:
        log("kalshi markets fetch failed for %s: %s" % (ticker, str(e)))
        return out

    for m in markets:
        # Skip settled/closed markets; only live ones carry odds.
        if (m.get("status") or "").lower() not in ("active", "open", "initialized"):
            continue
        q = m.get("title") or m.get("yes_sub_title") or ev.get("title") or ticker
        # Only keep macro-relevant questions inside the event.
        if not is_macro(q) and not is_macro(ticker.replace("_", " ")):
            continue
        prob = kalshi_market_price(m)
        if prob is None:
            continue
        vol = kalshi_market_volume(m)
        out.append({
            "question": q,
            "yes_prob": round(prob, 4),
            "volume": int(vol) if vol is not None else None,
            "close_date": m.get("close_time"),
            "source": "kalshi",
            "ticker": m.get("ticker"),
        })
    return out


def fetch_kalshi():
    events = kalshi_events()
    markets = []
    if not events:
        return markets
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(kalshi_event_markets, ev): ev for ev in events}
        for fut in as_completed(futs):
            try:
                markets.extend(fut.result())
            except Exception as e:
                log("kalshi worker error: " + str(e))
    log("kalshi macro markets: %d" % len(markets))
    return markets


# ------------------------------------------------------------ Polymarket ---

def fetch_polymarket():
    """Polymarket Gamma API: events embed their markets (outcomePrices)."""
    out = []
    try:
        url = POLY_BASE + "/events?limit=100&closed=false"
        events = fetch_json(url)
        if not isinstance(events, list):
            log("polymarket: unexpected payload type")
            return out
    except Exception as e:
        log("polymarket events fetch failed: " + str(e))
        return out

    for ev in events:
        title = ev.get("title") or ""
        desc = ev.get("description") or ""
        tags = " ".join(
            [t.get("label") or t.get("slug") or "" for t in (ev.get("tags") or [])]
        )
        haystack = title + " " + desc + " " + tags
        if not (is_macro(haystack) or any(h in haystack.lower() for h in POLY_TAG_HINTS)):
            continue
        for m in (ev.get("markets") or []):
            q = m.get("question") or title
            prices = m.get("outcomePrices")
            prob = None
            try:
                if isinstance(prices, list) and prices:
                    prob = float(prices[0])
                elif isinstance(prices, str):
                    prob = float(json.loads(prices)[0])
            except (TypeError, ValueError, IndexError):
                prob = None
            if prob is None or not (0.0 <= prob <= 1.0):
                continue
            vol = _f(m.get("volume"))
            out.append({
                "question": q,
                "yes_prob": round(prob, 4),
                "volume": float(vol) if vol is not None else None,
                "close_date": m.get("endDate") or ev.get("endDate"),
                "source": "polymarket",
                "ticker": m.get("id") or m.get("slug"),
            })
    log("polymarket macro markets: %d" % len(out))
    return out


# --------------------------------------------------------- macro signals ---

# signal_key -> list of keyword groups (all groups must match at least one kw)
SIGNAL_SPECS = {
    "fed_cut_odds": [["fed", "fomc", "rate"], ["cut"]],
    "fed_hike_odds": [["fed", "fomc", "rate"], ["hike", "raise", "increase"]],
    "recession_odds": [["recession"]],
    "inflation_above_target_odds": [["inflation", "cpi"], ["above", "exceed", "rise", "higher"]],
    "unemployment_odds": [["unemployment", "jobs report", "nonfarm"]],
    "election_odds": [["election", "president"]],
}


def _matches(text, groups):
    t = text.lower()
    return all(any(kw in t for kw in g) for g in groups)


def build_macro_signals(kalshi, polymarket):
    """For each signal, pick the highest-volume matching market."""
    all_m = [dict(m, source="kalshi") for m in kalshi] + \
            [dict(m, source="polymarket") for m in polymarket]
    signals = {}
    for key, groups in SIGNAL_SPECS.items():
        best = None
        best_vol = -1
        for m in all_m:
            if not _matches(m.get("question") or "", groups):
                continue
            vol = m.get("volume")
            vol_num = vol if isinstance(vol, (int, float)) else 0
            if best is None or vol_num > best_vol:
                best, best_vol = m, vol_num
        if best is not None:
            signals[key] = {
                "question": best["question"],
                "yes_prob": best["yes_prob"],
                "volume": best["volume"],
                "close_date": best["close_date"],
                "source": best["source"],
            }
    return signals


# ------------------------------------------------------------------ S3 ---

def load_previous():
    """Read the existing S3 file so its snapshot can roll into history."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=S3_KEY)
        return json.loads(obj["Body"].read().decode("utf-8", "replace"))
    except Exception as e:
        log("no previous file (or unreadable): " + str(e))
        return None


def compact_snapshot(prev):
    """Compact copy of a full snapshot for the history array."""
    if not isinstance(prev, dict):
        return None
    return {
        "t": prev.get("generated_at"),
        "kalshi": [
            {"question": m.get("question"), "yes_prob": m.get("yes_prob"),
             "volume": m.get("volume")}
            for m in (prev.get("kalshi") or []) if isinstance(m, dict)
        ],
        "polymarket": [
            {"question": m.get("question"), "yes_prob": m.get("yes_prob"),
             "volume": m.get("volume")}
            for m in (prev.get("polymarket") or []) if isinstance(m, dict)
        ],
        "macro_signals": prev.get("macro_signals") or {},
    }


def write_output(payload):
    raw = json.dumps(payload, ensure_ascii=False)
    S3.put_object(
        Bucket=BUCKET,
        Key=S3_KEY,
        Body=raw.encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800",
    )
    log("wrote s3://%s/%s (%d bytes)" % (BUCKET, S3_KEY, len(raw)))


# -------------------------------------------------------------- handler ---

def lambda_handler(event=None, context=None):
    out = {
        "generated_at": iso_now(),
        "kalshi": [],
        "polymarket": [],
        "macro_signals": {},
        "history": [],
        "sources": {"kalshi": "ok", "polymarket": "ok"},
        "errors": [],
    }
    try:
        # --- history: roll the previous snapshot forward ---
        try:
            prev = load_previous()
            if prev:
                prev_hist = prev.get("history") or []
                hist = [h for h in prev_hist if isinstance(h, dict)]
                snap = compact_snapshot(prev)
                if snap and snap.get("t"):
                    # avoid double-appending the same timestamp
                    if not hist or hist[-1].get("t") != snap["t"]:
                        hist.append(snap)
                out["history"] = hist[-HISTORY_CAP:]
        except Exception as e:
            out["errors"].append("history: " + str(e))
            log("history error: " + str(e))

        # --- Kalshi (fail-soft) ---
        try:
            out["kalshi"] = fetch_kalshi()
        except Exception as e:
            out["sources"]["kalshi"] = "error"
            out["errors"].append("kalshi: " + str(e))
            log("kalshi pipeline failed: " + str(e))

        # --- Polymarket (fail-soft) ---
        try:
            out["polymarket"] = fetch_polymarket()
        except Exception as e:
            out["sources"]["polymarket"] = "error"
            out["errors"].append("polymarket: " + str(e))
            log("polymarket pipeline failed: " + str(e))

        # --- macro signals from whatever we got ---
        try:
            out["macro_signals"] = build_macro_signals(out["kalshi"], out["polymarket"])
        except Exception as e:
            out["errors"].append("macro_signals: " + str(e))
            log("macro signals failed: " + str(e))

    except Exception as e:
        # Absolute last resort: never crash, report the error.
        out["errors"].append("fatal: " + str(e))
        log("fatal: " + str(e))

    # Always write, even if partial/empty.
    try:
        write_output(out)
    except Exception as e:
        log("S3 write failed: " + str(e))
        out["errors"].append("s3_write: " + str(e))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "generated_at": out["generated_at"],
            "kalshi_markets": len(out["kalshi"]),
            "polymarket_markets": len(out["polymarket"]),
            "signals": list(out["macro_signals"].keys()),
            "history_entries": len(out["history"]),
            "errors": out["errors"],
        }),
    }
