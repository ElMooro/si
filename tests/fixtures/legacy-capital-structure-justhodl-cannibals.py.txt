"""justhodl-cannibals v1.0 — buybacks + insider conviction, forensically
clean, as a graded family (creative #9, ops 3447).

The documented factor: companies genuinely retiring shares (3y share CAGR
<= -2%/yr) where insiders are ALSO buying, excluding every forensic trap the
desk already flags (SBC_WASH, MGMT_SELLING_INTO_BUYBACK, DEATH_SPIRAL).
Each qualifier is emitted UP [21,63] vs its sector ETF — beta-stripped —
and the PROVEN gate decides promotion.

Feed: data/cannibals.json · Signals: type "cannibals".
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.1.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC_KEY = "data/share-flows.json"
OUT_KEY = "data/cannibals.json"
BAD = {"SBC_WASH", "MGMT_SELLING_INTO_BUYBACK", "DEATH_SPIRAL"}
ETF_OF = {"Technology": "XLK", "Financials": "XLF", "Financial Services": "XLF",
          "Healthcare": "XLV", "Consumer Discretionary": "XLY",
          "Consumer Cyclical": "XLY", "Consumer Staples": "XLP",
          "Consumer Defensive": "XLP", "Energy": "XLE", "Industrials": "XLI",
          "Materials": "XLB", "Basic Materials": "XLB", "Utilities": "XLU",
          "Real Estate": "XLRE", "Communication Services": "XLC"}
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def rj(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def plan(rows, sec_of):
    """v1.1: the feed's real vocabulary — buyback_net_yield_pct is the
    cannibal criterion; INSIDER_CONVICTION (rare) boosts, never gates."""
    plans, seen = [], set()
    for r in rows:
        tk = str(r.get("ticker") or "").upper()
        if not tk or tk in seen:
            continue
        y = r.get("buyback_net_yield_pct")
        fl = r.get("flags") or []
        mc = r.get("market_cap") or 0
        if not isinstance(y, (int, float)) or y < 4.0:
            continue
        if (BAD & set(fl)) or r.get("data_suspect"):
            continue
        if not isinstance(mc, (int, float)) or mc < 2e9:
            continue
        seen.add(tk)
        sec = sec_of.get(tk)
        plans.append({"ticker": tk,
                      "net_buyback_yield_pct": round(float(y), 2),
                      "insider_conviction": "INSIDER_CONVICTION" in fl,
                      "sh_3y_cagr_pct": r.get("sh_3y_cagr_pct"),
                      "sector": sec, "pair_etf": ETF_OF.get(sec, "SPY"),
                      "market_cap": mc})
    plans.sort(key=lambda p: (-p["insider_conviction"],
                              -p["net_buyback_yield_pct"]))
    return plans[:10]


def lambda_handler(event, context):
    t0 = time.time()
    if (event or {}).get("_probe"):
        rows = event["_probe"].get("rows") or []
        sec_of = event["_probe"].get("sec_of") or {}
        return {"statusCode": 200,
                "body": json.dumps({"plans": plan(rows, sec_of)})}
    doc = rj(SRC_KEY)
    rows = []

    import re as _re

    def walk(o):
        if isinstance(o, dict):
            if o.get("ticker") and "buyback_net_yield_pct" in o:
                rows.append(o)
            for k, v in o.items():
                # keyed-by-symbol maps (share-flows "tickers": {SYM: {...}})
                if (isinstance(v, dict) and "buyback_net_yield_pct" in v
                        and not v.get("ticker")
                        and _re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,6}", str(k))):
                    rows.append({**v, "ticker": k})
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(doc)
    uni = rj("data/universe.json")
    ul = uni if isinstance(uni, list) else (uni.get("stocks")
                                            or uni.get("universe") or [])
    sec_of = {str(u.get("symbol") or u.get("ticker")).upper(): u.get("sector")
              for u in ul if isinstance(u, dict) and u.get("sector")}
    plans = plan(rows, sec_of)
    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for p in plans:
        mark = yprice(p["ticker"])
        time.sleep(0.15)
        p["mark"] = mark
        if mark and log_signal(
                tbl, "cannibals", p["ticker"], "UP", [21, 63], mark,
                confidence=0.70 if p.get("insider_conviction") else 0.62,
                rationale=(f"cannibal: net buyback yield "
                           f"{p['net_buyback_yield_pct']}%"
                           + (" + INSIDER_CONVICTION" if p.get("insider_conviction") else "")
                           + f", forensically clean → UP vs {p['pair_etf']}"),
                benchmark=p["pair_etf"],
                metadata={"engine": "cannibals",
                          "net_by_pct": p["net_buyback_yield_pct"]}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_rows_scanned": len(rows), "plans": plans, "logged": logged,
           "methodology": ("buyback_net_yield_pct >= 4% ∩ clean flags ∩ "
                           "!data_suspect ∩ mcap>=2B; INSIDER_CONVICTION "
                           "boosts confidence 0.62→0.70 and sort priority; "
                           "top-10; UP [21,63] vs sector ETF. PROVEN gate "
                           "controls promotion.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[cannibals] scanned={len(rows)} plans={len(plans)} logged={logged}")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "plans": len(plans), "logged": logged})}
