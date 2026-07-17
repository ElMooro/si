"""justhodl-proven-portfolio v1.0 — THE COMPOSER (ops 3402).

The 2026-07-17 audit's verdict: the fleet grades signals (scorecard), sizes
them (sizing-engine, quarter-Kelly from measured edge), and trusts them
(engine_trust in master-ranker) — but NOTHING composes proven signals into a
book with a NAV. This engine is that spine.

Daily:
  1. Read data/signal-scorecard.json → the set of signal_types whose graded
     record clears the bar (explicit proven/alpha status when present, else
     n>=20 & hit>=0.55 & IR>0 — labeled PROVISIONAL until day_7 grading
     matures, self-upgrading).
  2. Pull the last 10 days of live signals of those types from the
     justhodl-signals table (pending or recently complete), dedupe to the
     strongest per ticker, direction-aware.
  3. Weight via sizing-engine's quarter-Kelly when it has the (type, ticker)
     pair, else equal-weight fallback; caps: 8% per name, 100% gross.
  4. Mark yesterday's book at today's Yahoo closes → NAV step; append the
     ledger (data/proven-portfolio-history.json); then rebalance to today's
     book. Paper, frictionless v1 (stated on the page).
  5. Attribute NAV and today's book by signal_type — the lens-P&L layer the
     flywheel needs.

Feed: data/proven-portfolio.json · Ledger: data/proven-portfolio-history.json
"""

import json
import os
import re
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

VERSION = "1.1.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/proven-portfolio.json"
HIST_KEY = "data/proven-portfolio-history.json"
MAX_W = 8.0
MAX_POSITIONS = 40
LOOKBACK_D = 10
UA = {"User-Agent": "Mozilla/5.0 (JustHodl proven-portfolio)"}

s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def rj(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def yprice(sym):
    try:
        u = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
             "?range=5d&interval=1d")
        with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=12) as r:
            j = json.loads(r.read())
        res = j["chart"]["result"][0]
        p = (res.get("meta") or {}).get("regularMarketPrice")
        if p:
            return float(p)
        cl = [c for c in res["indicators"]["quote"][0]["close"] if c]
        return float(cl[-1]) if cl else None
    except Exception:
        return None


def qualifying_types(sc):
    """(type -> tier) from the scorecard; explicit status wins, derived else."""
    out = {}
    rows = sc.get("scorecard") or sc.get("signal_types") or sc.get("rows") or []
    if isinstance(rows, dict):
        rows = [dict(v, signal_type=k) for k, v in rows.items()]
    for r in rows:
        st = r.get("signal_type")
        if not st:
            continue
        status = str(r.get("alpha_status") or r.get("status") or "").lower()
        n = r.get("n_scored") or r.get("n") or 0
        hit = r.get("alpha_hit_rate") or r.get("hit_rate") or 0
        ir = r.get("info_ratio") or 0
        if "proven" in status or status == "alpha":
            out[st] = "PROVEN"
        elif n >= 20 and hit >= 0.55 and ir > 0:
            out[st] = "PROVISIONAL"
    return out


def live_signals(types):
    tbl = ddb.Table("justhodl-signals")
    cutoff = int(time.time()) - LOOKBACK_D * 86400
    items, lek = [], None
    from boto3.dynamodb.conditions import Attr
    fe = Attr("logged_epoch").gte(cutoff) & Attr("schema_version").eq("2")
    while True:
        kw = {"FilterExpression": fe}
        if lek:
            kw["ExclusiveStartKey"] = lek
        resp = tbl.scan(**kw)
        items.extend(resp.get("Items") or [])
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
    out = []
    for it in items:
        st = it.get("signal_type")
        if st not in types:
            continue
        tk = it.get("measure_against") or it.get("ticker")
        if not (tk and re.fullmatch(r"[A-Z0-9.\-\^=]{1,10}", str(tk))):
            continue
        conf = float(it.get("confidence") or 0.5)
        out.append({"signal_type": st, "ticker": str(tk),
                    "direction": it.get("predicted_direction") or "UP",
                    "confidence": conf, "logged_at": it.get("logged_at"),
                    "tier": types[st]})
    return out


def compose(signals, sizing):
    smap = {}
    for r in (sizing.get("recommendations") or sizing.get("rows") or []):
        k = (r.get("signal_type"), r.get("ticker"))
        w = r.get("quarter_kelly_w_pct")
        if k[1] and isinstance(w, (int, float)):
            smap[k] = float(w)
    best = {}
    for sg in signals:
        cur = best.get(sg["ticker"])
        if not cur or sg["confidence"] > cur["confidence"]:
            best[sg["ticker"]] = sg
    book = []
    for tk, sg in best.items():
        w = smap.get((sg["signal_type"], tk))
        if w is None:
            w = 2.0 + 4.0 * (sg["confidence"] - 0.5)
        w = max(1.0, min(MAX_W, w))
        book.append(dict(sg, weight_pct=round(w, 2)))
    gross = sum(p["weight_pct"] for p in book)
    if gross > 100.0 and gross > 0:
        for p in book:
            p["weight_pct"] = round(p["weight_pct"] * 100.0 / gross, 2)
    book = sorted(book, key=lambda p: (-1 if p["tier"] == "PROVEN" else 0,
                                        -p["weight_pct"] * p["confidence"]))
    book = book[:MAX_POSITIONS]
    gross = sum(p["weight_pct"] for p in book) or 1.0
    for p in book:
        p["weight_pct"] = round(p["weight_pct"] * 100.0 / gross, 2)
    return sorted(book, key=lambda p: -p["weight_pct"])


def lambda_handler(event, context):
    t0 = time.time()
    today = datetime.now(timezone.utc).date().isoformat()

    sc = rj("data/signal-scorecard.json")
    types = qualifying_types(sc)
    mode = ("PROVEN" if any(v == "PROVEN" for v in types.values())
            else "PROVISIONAL" if types else "WAITING")
    signals = live_signals(types) if types else []
    book = compose(signals, rj("data/sizing.json"))

    # marks (book + carry-over tickers + SPY)
    ledger = rj(HIST_KEY) or {"rows": []}
    rows = ledger.get("rows") or []
    prev = rows[-1] if rows else None
    need = {p["ticker"] for p in book} | {"SPY"}
    if prev:
        need |= {p["ticker"] for p in (prev.get("positions") or [])}
    marks = {}
    for tk in sorted(need):
        marks[tk] = yprice(tk)
        time.sleep(0.12)

    nav = 100.0
    spy0 = None
    day_ret = None
    if prev:
        nav = float(prev.get("nav") or 100.0)
        spy0 = prev.get("spy_close")
        num = 0.0
        for p in (prev.get("positions") or []):
            e, m = p.get("mark"), marks.get(p["ticker"])
            if e and m:
                r = (m / e - 1.0) * (1 if p.get("direction") != "DOWN" else -1)
                num += r * float(p.get("weight_pct") or 0)
        day_ret = num / 100.0
        nav = round(nav * (1.0 + day_ret), 4)
    spy_now = marks.get("SPY")
    spy_nav = None
    if rows and rows[0].get("spy_close") and spy_now:
        spy_nav = round(100.0 * spy_now / float(rows[0]["spy_close"]), 4)

    for p in book:
        p["mark"] = marks.get(p["ticker"])

    attrib = {}
    for p in book:
        a = attrib.setdefault(p["signal_type"], {"weight_pct": 0.0, "n": 0,
                                                 "tier": p["tier"]})
        a["weight_pct"] = round(a["weight_pct"] + p["weight_pct"], 2)
        a["n"] += 1

    row = {"date": today, "nav": nav, "spy_close": spy_now,
           "day_ret_pct": (round(day_ret * 100, 3) if day_ret is not None else None),
           "mode": mode, "n_positions": len(book),
           "positions": [{k: p[k] for k in ("ticker", "signal_type", "direction",
                                            "weight_pct", "mark", "tier")}
                         for p in book]}
    if rows and rows[-1].get("date") == today:
        rows[-1] = row
    else:
        rows.append(row)
    rows = rows[-750:]
    s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                  Body=json.dumps({"rows": rows}, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")

    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "mode": mode,
           "mode_note": {"PROVEN": "book built only from signal_types with graded, friction-surviving alpha",
                         "PROVISIONAL": "day_7 grading still maturing — using best graded tiers; auto-upgrades to PROVEN",
                         "WAITING": "no signal_type has enough graded history yet — book empty by design"}[mode],
           "qualifying_types": types, "n_signals_seen": len(signals),
           "book": book, "attribution": attrib,
           "nav": {"nav": nav, "spy_nav": spy_nav,
                   "day_ret_pct": row["day_ret_pct"],
                   "inception": (rows[0]["date"] if rows else today),
                   "n_days": len(rows)},
           "methodology": ("Paper book, frictionless v1. Signals: last 10d of "
                           "graded-qualifying types from justhodl-signals; "
                           "strongest per ticker; direction-aware. Sizing: "
                           "sizing-engine quarter-Kelly when available, else "
                           "confidence-scaled; 8% cap, 100% gross. NAV: "
                           "yesterday's book marked at today's closes.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[proven-portfolio] mode={mode} types={len(types)} book={len(book)} "
          f"nav={nav} spy_nav={spy_nav} {round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "mode": mode,
                                                   "n": len(book), "nav": nav})}
