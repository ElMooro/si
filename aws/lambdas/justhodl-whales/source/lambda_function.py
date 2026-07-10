#!/usr/bin/env python3
"""justhodl-whales -- WHALES ARE HOLDING (Khalid 2026-07-10).

Per-STOCK dollar flows across the whale roster: what the Berkshires and
the mega managers actually bought and sold last quarter, in dollars.
Composer over the SAME probe-proven FMP endpoint the smart-money fleet
uses (/stable/institutional-ownership/extract, validated ops-452 lineage
in justhodl-smart-money-holdings) -- this engine adds the piece nothing
else computes: latest-vs-prior quarter DIFF per ticker per whale ->
trading dollars (Delta-shares x latest price-per-share, so mark-to-market
drift doesn't masquerade as flow), NEW accumulation / full EXIT
classification, buyer & seller names per stock, dollar leaderboards.

Roster = the 30-fund validated CONCENTRATED_FUNDS list (copied verbatim
from smart-money-holdings incl its dropped-CIK annotations) + a BANKS
tier (Morgan Stanley et al) flagged custodial=True -- bank 13Fs mix
client custody and market-making with prop, so their flows display
separately and honestly. 13F facts of life, stated in the output: 45-day
filing lag, long-only US-listed equity, quarterly resolution.

Output: data/whales.json. Weekly (amendments trickle in). Real data only.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/whales.json"
FMP = (os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY") or "")

# ── Whale roster: conviction managers (provenance: justhodl-smart-money-
# holdings CONCENTRATED_FUNDS, CIKs validated against holder-performance-
# summary; DROPPED lines = FMP-stale filers, kept as documentation) ──
WHALES = [
    ("0001067983", "Berkshire Hathaway"),
    ("0001336528", "Pershing Square Capital"),
    ("0000921669", "Icahn Capital Management"),
    ("0001517137", "Starboard Value"),
    ("0001345471", "Anchorage Capital"),
    ("0001112520", "Akre Capital Management"),
    ("0001541617", "Millennium Management"),
    ("0001418814", "ValueAct Capital"),
    ("0001061768", "Lone Pine Capital"),
    ("0001167483", "Tiger Global Management"),
    ("0001031972", "Baupost Group"),
    ("0001040273", "Third Point"),
    ("0001135730", "Coatue Management"),
    ("0001103804", "Viking Global Investors"),
    ("0001346824", "ARK Investment Management"),
    ("0001020066", "Sands Capital Management"),
    ("0001536411", "Duquesne Family Office"),
    ("0000732905", "Tweedy, Browne"),
    ("0001313893", "Maple Capital"),
    ("0001036325", "Davis Selected Advisers"),
    ("0001029160", "Soros Fund Management"),
    ("0001350694", "Bridgewater Associates"),
    ("0001037389", "Renaissance Technologies"),
    ("0001179392", "Two Sigma Investments"),
    ("0001009207", "D.E. Shaw"),
    ("0000820027", "Tudor Investment"),
    ("0001423053", "Citadel Advisors"),
    ("0001167557", "AQR Capital Management"),
    ("0001603466", "Schonfeld Strategic"),
    # DROPPED (FMP-stale, per smart-money-holdings Stage 16.6):
    # Davidson Kempner 0001047644 (2015), Discovery 0001321655 (2023),
    # Greenlight 0001079114 (2023Q4), Elliott 0001048445 (2019Q4)
]
BANKS = [  # custodial=True tier -- flows shown separately, honesty note
    ("0000895421", "Morgan Stanley"),
    ("0000886982", "Goldman Sachs Group"),
    ("0000019617", "JPMorgan Chase"),
    ("0000070858", "Bank of America"),
]
MIN_FLOW_USD = 2_000_000     # ignore dust deltas
MAX_PAGES = 40


def _get(url, timeout=30):
    for attempt in (0, 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":
                                                       "justhodl-whales"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as he:
            if attempt == 0 and he.code in (429, 500, 502, 503):
                time.sleep(2.5)
                continue
            print("[whales] %s: %s" % (url[:100], he))
            return None
        except Exception as e:
            if attempt == 0:
                time.sleep(2)
                continue
            print("[whales] %s: %s" % (url[:100], e))
            return None


def latest_filed_quarter():
    """Most recent quarter >=60 days past (13F 45-day deadline + buffer).
    Same rule as smart-money-holdings."""
    now = datetime.now(timezone.utc)
    quarters = []
    for yy in (now.year, now.year - 1):
        for q in (4, 3, 2, 1):
            qend = datetime(yy, q * 3, 1, tzinfo=timezone.utc)
            days = (now - qend).days
            if days >= 60:
                quarters.append((yy, q))
    quarters.sort(key=lambda yq: (-yq[0], -yq[1]))
    return quarters[0] if quarters else (now.year - 1, 4)


def yq_prev(year, quarter):
    return (year - 1, 4) if quarter == 1 else (year, quarter - 1)


def fetch_positions(cik, year, quarter):
    """{symbol: {shares, value}} for one filer-quarter, paginated.
    Empty dict = no filing found for that quarter."""
    out = {}
    prev_sig = None
    for page in range(MAX_PAGES):
        url = ("https://financialmodelingprep.com/stable/"
               "institutional-ownership/extract?cik=%s&year=%d"
               "&quarter=%d&page=%d&apikey=%s"
               % (cik, year, quarter, page, FMP))
        rows = _get(url)
        if not rows or not isinstance(rows, list):
            break
        # duplicate-page guard: out-of-range pages on big filers can
        # re-serve data -> share counts multiply -> $260B phantom flows
        # (2026Q1 first-run lesson). Signature = first+last row + len.
        sig = (len(rows),
               json.dumps(rows[0], sort_keys=True)[:120],
               json.dumps(rows[-1], sort_keys=True)[:120])
        if sig == prev_sig:
            break
        prev_sig = sig
        for h in rows:
            sym = h.get("symbol")
            if not sym or len(sym) > 6:
                continue
            sh = h.get("shares") or h.get("sharesNumber") or 0
            val = h.get("value") or h.get("marketValue") or 0
            try:
                sh, val = float(sh), float(val)
            except (TypeError, ValueError):
                continue
            if sym in out:            # multiple lots -> sum
                out[sym]["shares"] += sh
                out[sym]["value"] += val
            else:
                out[sym] = {"shares": sh, "value": val}
        if len(rows) < 100:      # short page = last page (any size)
            break
    return out


def fund_diff(args):
    cik, name, custodial, y, q = args
    py, pq = yq_prev(y, q)
    cur = fetch_positions(cik, y, q)
    if not cur:                        # late filer: step one back
        y, q = py, pq
        py, pq = yq_prev(y, q)
        cur = fetch_positions(cik, y, q)
        if not cur:
            return {"cik": cik, "name": name, "ok": False}
    prev = fetch_positions(cik, py, pq)
    moves = []
    for sym in set(cur) | set(prev):
        c, p = cur.get(sym), prev.get(sym)
        csh = (c or {}).get("shares", 0.0)
        psh = (p or {}).get("shares", 0.0)
        dsh = csh - psh
        px = None
        if c and c["shares"] > 0 and c["value"] > 0:
            px = c["value"] / c["shares"]
        elif p and p["shares"] > 0 and p["value"] > 0:
            px = p["value"] / p["shares"]
        if px is None or abs(dsh) < 1:
            continue
        flow = dsh * px
        if abs(flow) < MIN_FLOW_USD:
            continue
        action = ("NEW" if psh <= 0 and csh > 0 else
                  "EXIT" if csh <= 0 and psh > 0 else
                  "ADD" if dsh > 0 else "TRIM")
        moves.append({"symbol": sym, "flow_usd": round(flow),
                      "action": action,
                      "shares_now": round(csh),
                      "value_now": round((c or {}).get("value", 0.0))})
    fund_total = sum(v["value"] for v in cur.values()) or 1.0
    dropped = [m for m in moves if abs(m["flow_usd"]) > 1.5 * fund_total]
    if dropped:
        print("[whales] %s: dropped %d implausible rows (>1.5x book)"
              % (name, len(dropped)))
    moves = [m for m in moves if abs(m["flow_usd"]) <= 1.5 * fund_total]
    moves.sort(key=lambda m: -abs(m["flow_usd"]))
    return {"cik": cik, "name": name, "custodial": custodial, "ok": True,
            "quarter": "%dQ%d" % (y, q),
            "prior_quarter": "%dQ%d" % (py, pq),
            "n_positions": len(cur),
            "total_value_usd": round(sum(v["value"]
                                         for v in cur.values())),
            "n_moves": len(moves), "moves": moves}


def lambda_handler(event=None, context=None):
    started = time.time()
    if not FMP:
        raise RuntimeError("FMP key missing")
    y, q = latest_filed_quarter()
    jobs = ([(c, n, False, y, q) for c, n in WHALES]
            + [(c, n, True, y, q) for c, n in BANKS])
    with ThreadPoolExecutor(max_workers=6) as exe:
        results = list(exe.map(fund_diff, jobs))
    funds = [r for r in results if r.get("ok")]
    failed = [r["name"] for r in results if not r.get("ok")]

    stocks = {}
    for f in funds:
        for m in f["moves"]:
            s = stocks.setdefault(m["symbol"], {
                "net_flow_usd": 0, "conviction_flow_usd": 0,
                "buyers": [], "sellers": [], "new_by": [], "exit_by": [],
                "whale_value_usd": 0, "n_holders": 0})
            s["net_flow_usd"] += m["flow_usd"]
            if not f["custodial"]:
                s["conviction_flow_usd"] += m["flow_usd"]
            tag = f["name"] + (" (bank)" if f["custodial"] else "")
            if m["flow_usd"] > 0:
                s["buyers"].append(tag)
            else:
                s["sellers"].append(tag)
            if m["action"] == "NEW":
                s["new_by"].append(tag)
            if m["action"] == "EXIT":
                s["exit_by"].append(tag)
        for m in f["moves"]:
            if m["value_now"] > 0:
                st = stocks[m["symbol"]]
                st["whale_value_usd"] += m["value_now"]
                st["n_holders"] += 1
    for s in stocks.values():
        s["net_flow_usd"] = round(s["net_flow_usd"])
        s["conviction_flow_usd"] = round(s["conviction_flow_usd"])

    def board(keyfn, filt, n=20):
        rows = [{"symbol": sym, **{k: v[k] for k in (
            "net_flow_usd", "conviction_flow_usd", "n_holders")},
            "buyers": v["buyers"][:6], "sellers": v["sellers"][:6],
            "new_by": v["new_by"][:6], "exit_by": v["exit_by"][:6]}
            for sym, v in stocks.items() if filt(v)]
        rows.sort(key=keyfn)
        return rows[:n]

    boards = {
        "whale_inflow_leaders": board(
            lambda r: -r["conviction_flow_usd"],
            lambda v: v["conviction_flow_usd"] > 0),
        "whale_outflow_leaders": board(
            lambda r: r["conviction_flow_usd"],
            lambda v: v["conviction_flow_usd"] < 0),
        "fresh_accumulation": board(
            lambda r: -r["conviction_flow_usd"],
            lambda v: v["new_by"] and v["conviction_flow_usd"] > 0),
        "full_distribution": board(
            lambda r: r["conviction_flow_usd"],
            lambda v: v["exit_by"] and v["conviction_flow_usd"] < 0)}

    whale_cards = [{k: f[k] for k in ("name", "cik", "custodial",
                                      "quarter", "n_positions",
                                      "total_value_usd", "n_moves")}
                   | {"top_moves": f["moves"][:8]} for f in funds]
    whale_cards.sort(key=lambda w: -w["total_value_usd"])

    out = {"engine": "justhodl-whales", "schema": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "quarter": "%dQ%d" % (y, q),
           "n_whales_ok": len(funds), "n_failed": len(failed),
           "failed": failed, "n_stocks_moved": len(stocks),
           "boards": boards, "whales": whale_cards,
           "stocks": {sym: v for sym, v in sorted(
               stocks.items(),
               key=lambda kv: -abs(kv[1]["conviction_flow_usd"]))[:400]},
           "method": ("Flow $ = Delta-shares x latest quarter price-per-"
                      "share (value/shares), so price drift is not "
                      "counted as trading. Conviction flow excludes the "
                      "custodial bank tier. Moves under $%dM ignored."
                      % (MIN_FLOW_USD // 1_000_000)),
           "honesty": ("13F: 45-day lag, long-only US-listed equity, "
                       "quarterly resolution. Bank 13Fs (Morgan Stanley "
                       "et al) mix client custody and market-making -- "
                       "flagged (bank), excluded from conviction "
                       "boards."),
           "duration_s": round(time.time() - started, 1)}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=1800")
    print("[whales] %s: %d whales ok, %d stocks moved, %.0fs"
          % (out["quarter"], len(funds), len(stocks),
             out["duration_s"]))
    return {"ok": True, "quarter": out["quarter"],
            "n_whales_ok": len(funds), "n_stocks": len(stocks)}


if __name__ == "__main__":
    print(json.dumps(lambda_handler())[:400])
