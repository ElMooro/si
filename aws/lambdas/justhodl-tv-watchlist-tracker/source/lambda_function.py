"""justhodl-tv-watchlist-tracker v1.0 — ops 3159.

Khalid's thesis: "different watchlists predict different things."
This engine makes that measurable. Each TradingView watchlist (synced
via the extension into data/tv-watchlists.json) becomes a labeled
equal-weight basket:

  • 90d price backfill per unique symbol (Polygon, US equities), then
    daily incremental via grouped-daily — state persists so history
    accumulates cheaply.
  • Per list: equal-weight index, 5/21/63d returns, excess vs SPY,
    top/bottom contributors.
  • Mondays: one signal per list into justhodl-signals as
    signal_type "tvwl_<slug>" (predicted UP vs SPY benchmark) —
    outcome-checker grades 5/21/63d → the scorecard grows ONE ROW PER
    WATCHLIST with hit-rates. Bearish lists reveal themselves as
    sub-50% rows: information either way.

WAITING_FIRST_SYNC state when no lists exist yet.
Output: data/tv-watchlist-tracker.json  State: data/tv-watchlist-state.json
"""

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "")
OUT_KEY = "data/tv-watchlist-tracker.json"
STATE_KEY = "data/tv-watchlist-state.json"
LISTS_KEY = "data/tv-watchlists.json"
MAX_SYMBOLS = 250
MAX_LISTS = 30

S3 = boto3.client("s3", region_name="us-east-1")


def _get(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


def _put(key, doc):
    S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(doc).encode(),
                  ContentType="application/json")


def _http(url):
    req = urllib.request.Request(url, headers={"User-Agent": "jh-wl/1.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read().decode())


def bare(sym):
    """'NASDAQ:AAPL' -> 'AAPL'; reject non-US-equity forms for v1."""
    s = str(sym).upper().strip()
    if ":" in s:
        ex, t = s.split(":", 1)
        if ex in ("BINANCE", "COINBASE", "BITSTAMP", "FX", "OANDA",
                  "FOREXCOM", "CRYPTO", "INDEX", "TVC", "FRED"):
            return None
        s = t
    if not re.fullmatch(r"[A-Z]{1,5}(\.[A-Z])?", s):
        return None
    return s.replace(".", "")


def slug(name):
    s = re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")[:32]
    return s or "list"


def poly_range(tk, d0, d1):
    try:
        d = _http(f"https://api.polygon.io/v2/aggs/ticker/{tk}/range/1/day/"
                  f"{d0}/{d1}?adjusted=true&sort=asc&limit=200&apiKey={POLY}")
        return {r["t"] // 86400000: r["c"] for r in d.get("results") or []}
    except Exception:
        return {}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    t0 = time.time()
    src = _get(LISTS_KEY) or {}
    lists = [l for l in (src.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")][:MAX_LISTS]

    if not lists:
        _put(OUT_KEY, {"generated_at": now.isoformat(), "version": "1.0",
                       "status": "WAITING_FIRST_SYNC",
                       "how": "Install the JH TradingView extension and "
                              "press Sync — this board lights up on the "
                              "next run after your lists arrive."})
        return {"ok": True, "status": "WAITING_FIRST_SYNC"}

    # ── resolve membership ───────────────────────────────────────────
    members = {}
    for l in lists:
        syms = []
        for s in l.get("symbols") or []:
            b = bare(s)
            if b and b not in syms:
                syms.append(b)
        if syms:
            members[slug(l["name"])] = {"name": l["name"],
                                        "symbols": syms[:80]}
    uniq = sorted({s for m in members.values() for s in m["symbols"]}
                  | {"SPY"})[:MAX_SYMBOLS]

    # ── price history: state + backfill + incremental ────────────────
    state = _get(STATE_KEY) or {"prices": {}}
    prices = state.get("prices") or {}
    d1 = now.date().isoformat()
    d0 = (now.date() - timedelta(days=100)).isoformat()
    need_full = [t for t in uniq if len(prices.get(t) or {}) < 40]
    for tk in need_full[:MAX_SYMBOLS]:
        got = poly_range(tk, d0, d1)
        if got:
            prices[tk] = {str(k): v for k, v in got.items()}
        time.sleep(0.05)
    # incremental top-up for the rest (last 7 days)
    d7 = (now.date() - timedelta(days=7)).isoformat()
    for tk in [t for t in uniq if t not in need_full]:
        got = poly_range(tk, d7, d1)
        for k, v in got.items():
            prices.setdefault(tk, {})[str(k)] = v
    # trim to 130d
    cut = (now - timedelta(days=130)).timestamp() // 86400
    for tk in list(prices):
        prices[tk] = {k: v for k, v in prices[tk].items() if int(k) >= cut}
    state["prices"] = prices
    state["updated_at"] = now.isoformat()
    _put(STATE_KEY, state)

    def series(tk):
        p = prices.get(tk) or {}
        return [v for _, v in sorted(((int(k), v) for k, v in p.items()))]

    def ret(seq, days):
        if len(seq) <= days or not seq[-1 - days]:
            return None
        return round((seq[-1] / seq[-1 - days] - 1) * 100, 2)

    spy = series("SPY")
    spy_r = {d: ret(spy, d) for d in (5, 21, 63)}

    # ── per-list equal-weight basket ─────────────────────────────────
    rows = []
    for sl, m in members.items():
        parts = {s: series(s) for s in m["symbols"]}
        parts = {s: q for s, q in parts.items() if len(q) >= 22}
        if not parts:
            rows.append({"slug": sl, "name": m["name"],
                         "n_symbols": len(m["symbols"]), "n_priced": 0,
                         "note": "no US-equity prices resolved"})
            continue
        out = {"slug": sl, "name": m["name"],
               "n_symbols": len(m["symbols"]), "n_priced": len(parts)}
        contrib = []
        for d in (5, 21, 63):
            rs = [r for r in (ret(q, d) for q in parts.values())
                  if r is not None]
            if rs:
                r = round(sum(rs) / len(rs), 2)
                out[f"ret_{d}d"] = r
                if spy_r[d] is not None:
                    out[f"excess_{d}d"] = round(r - spy_r[d], 2)
        for s, q in parts.items():
            r = ret(q, 21)
            if r is not None:
                contrib.append((s, r))
        contrib.sort(key=lambda x: -x[1])
        out["top"] = [{"t": t, "r21": r} for t, r in contrib[:3]]
        out["bottom"] = [{"t": t, "r21": r} for t, r in contrib[-3:]]
        rows.append(out)
    rows.sort(key=lambda r: -(r.get("excess_21d")
                              if r.get("excess_21d") is not None else -999))

    # ── Monday signal emission: one per list ─────────────────────────
    logged = 0
    if now.weekday() == 0 or (event or {}).get("force_emit"):
        try:
            from decimal import Decimal
            tbl = boto3.resource("dynamodb", "us-east-1") \
                .Table("justhodl-signals")
            for r in rows:
                if r.get("n_priced", 0) < 3:
                    continue
                lead = r["top"][0]["t"] if r.get("top") else None
                px = None
                if lead:
                    q = series(lead)
                    px = q[-1] if q else None
                tbl.put_item(Item={
                    "signal_id": f"tvwl-{r['slug']}#{now.date().isoformat()}",
                    "signal_type": f"tvwl_{r['slug']}"[:48],
                    "predicted_direction": "UP",
                    "signal_value": str(r.get("excess_21d")),
                    "confidence": Decimal("0.5"),
                    "measure_against": "basket_vs_benchmark",
                    "baseline_price": str(px or 0),
                    "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "outcomes": {}, "accuracy_scores": {},
                    "status": "pending",
                    "logged_at": now.isoformat(),
                    "logged_epoch": int(now.timestamp()),
                    "horizon_days_primary": 21, "schema_version": "2",
                    "ttl": int(now.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "tv-watchlist-tracker",
                                 "list_name": r["name"],
                                 "n_members": r.get("n_priced"),
                                 "members_sample":
                                     [t["t"] for t in (r.get("top") or [])]},
                    "rationale": (f"Khalid watchlist '{r['name']}' as "
                                  f"equal-weight basket vs SPY "
                                  f"(n={r.get('n_priced')})"),
                })
                logged += 1
        except Exception as e:
            print(f"[tvwl] emit failed: {str(e)[:120]}")

    doc = {"generated_at": now.isoformat(), "version": "1.0",
           "status": "LIVE", "n_lists": len(rows),
           "spy": spy_r, "signals_logged": logged,
           "how_to_read": ("Each watchlist = equal-weight basket. "
                           "excess_21d vs SPY is the tell; the scorecard "
                           "row tvwl_<slug> accumulates hit-rates so the "
                           "system learns WHICH list predicts."),
           "lists": rows, "elapsed_s": round(time.time() - t0, 1)}
    _put(OUT_KEY, doc)
    print(json.dumps({"ok": True, "n_lists": len(rows),
                      "signals_logged": logged}))
    return {"ok": True, "n_lists": len(rows), "signals_logged": logged}
