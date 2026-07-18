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

VERSION = "1.0.0"
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
    plans, seen = [], set()
    for r in rows:
        tk = str(r.get("ticker") or "").upper()
        fl = r.get("flags") or []
        cg = r.get("sh_3y_cagr_pct")
        if (not tk or tk in seen or not isinstance(cg, (int, float))
                or not isinstance(fl, list)):
            continue
        if cg <= -2.0 and "INSIDER_CONVICTION" in fl and not (BAD & set(fl)):
            seen.add(tk)
            sec = sec_of.get(tk)
            plans.append({"ticker": tk, "sh_3y_cagr_pct": round(cg, 2),
                          "sector": sec, "pair_etf": ETF_OF.get(sec, "SPY"),
                          "flags": [f for f in fl if f != "INSIDER_CONVICTION"][:3]})
    plans.sort(key=lambda p: p["sh_3y_cagr_pct"])
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

    def walk(o):
        if isinstance(o, dict):
            if o.get("ticker") and "flags" in o:
                rows.append(o)
            for v in o.values():
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
                confidence=0.62,
                rationale=(f"cannibal: 3y share CAGR {p['sh_3y_cagr_pct']}%/yr "
                           "+ INSIDER_CONVICTION, forensically clean → UP vs "
                           f"{p['pair_etf']}"),
                benchmark=p["pair_etf"],
                metadata={"engine": "cannibals",
                          "sh_3y_cagr_pct": p["sh_3y_cagr_pct"]}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_rows_scanned": len(rows), "plans": plans, "logged": logged,
           "methodology": ("sh_3y_cagr_pct <= -2%/yr ∩ INSIDER_CONVICTION ∩ "
                           "no {SBC_WASH, MGMT_SELLING_INTO_BUYBACK, "
                           "DEATH_SPIRAL}; top-10 by shrink rate; UP [21,63] "
                           "vs sector ETF. PROVEN gate controls promotion.")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[cannibals] scanned={len(rows)} plans={len(plans)} logged={logged}")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "plans": len(plans), "logged": logged})}
