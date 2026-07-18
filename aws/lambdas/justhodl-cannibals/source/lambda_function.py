"""justhodl-cannibals v1.0 — cannibals-with-conviction family (ops 3449).

The classic documented factor, composed from feeds you already forensically
clean: companies EATING their own float (3y share CAGR <= -2%/yr, true
retirement — the SBC_WASH / MGMT_SELLING_INTO_BUYBACK / DEATH_SPIRAL flags
disqualify fakes) where insiders are ALSO buying with size (INSIDER_CONVICTION:
>=$1M net insider buys, 90d). Top names graded UP over 21/63d vs their
sector ETF pair — beta-stripped, PROVEN-gated like every family.

Feed: data/cannibals.json · Signals: type "cannibal-conviction".
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3

from signals_emit import log_signal, yprice

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT = "data/cannibals.json"
BAD = {"SBC_WASH", "MGMT_SELLING_INTO_BUYBACK", "DEATH_SPIRAL"}
ETF_OF = {"Technology": "XLK", "Financials": "XLF", "Financial Services": "XLF",
          "Healthcare": "XLV", "Consumer Discretionary": "XLY",
          "Consumer Cyclical": "XLY", "Consumer Staples": "XLP",
          "Consumer Defensive": "XLP", "Energy": "XLE", "Industrials": "XLI",
          "Materials": "XLB", "Basic Materials": "XLB", "Utilities": "XLU",
          "Real Estate": "XLRE", "Communication Services": "XLC"}
s3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")


def rj(k):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=k)["Body"].read())
    except Exception:
        return {}


def lambda_handler(event, context):
    t0 = time.time()
    sf = rj("data/share-flows.json")
    rows = {}

    def walk(o):
        if isinstance(o, dict):
            tk = o.get("ticker") or o.get("symbol")
            cg = o.get("sh_3y_cagr_pct")
            fl = o.get("flags")
            if tk and isinstance(cg, (int, float)) and isinstance(fl, list):
                k = str(tk).upper()
                if k not in rows:
                    by = None
                    for kk, vv in o.items():
                        if ("buyback" in str(kk).lower()
                                and "yield" in str(kk).lower()
                                and isinstance(vv, (int, float))):
                            by = float(vv)
                            break
                    rows[k] = {"ticker": k, "sh_3y_cagr_pct": float(cg),
                               "flags": fl, "buyback_yield": by}
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(sf)

    uni = rj("data/universe.json")
    ul = uni if isinstance(uni, list) else (uni.get("stocks")
                                            or uni.get("universe") or [])
    sec_of = {str(u.get("symbol") or u.get("ticker")).upper(): u.get("sector")
              for u in ul if isinstance(u, dict) and u.get("sector")}

    screen = []
    for r in rows.values():
        fl = set(r["flags"])
        if r["sh_3y_cagr_pct"] <= -2.0 and "INSIDER_CONVICTION" in fl \
                and not (fl & BAD):
            r["sector"] = sec_of.get(r["ticker"])
            r["pair_etf"] = ETF_OF.get(r["sector"], "SPY")
            screen.append(r)
    screen.sort(key=lambda x: x["sh_3y_cagr_pct"])
    screen = screen[:15]

    tbl = ddb.Table("justhodl-signals")
    logged = 0
    for r in screen:
        mark = yprice(r["ticker"])
        time.sleep(0.15)
        r["mark"] = mark
        if mark and log_signal(
                tbl, "cannibal-conviction", r["ticker"], "UP", [21, 63], mark,
                confidence=min(0.78, 0.55 + abs(r["sh_3y_cagr_pct"]) / 40.0),
                rationale=(f"cannibal: shares {r['sh_3y_cagr_pct']:+.1f}%/yr "
                           f"3y + insider conviction (>=$1M net buys, 90d), "
                           "forensically clean"),
                benchmark=r["pair_etf"],
                signal_value=str(r["sh_3y_cagr_pct"]),
                metadata={"engine": "cannibals"}):
            logged += 1
    out = {"ok": True, "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 2),
           "n_universe": len(rows), "n_screen": len(screen), "logged": logged,
           "screen": screen,
           "rules": ("sh_3y_cagr_pct <= -2%/yr AND INSIDER_CONVICTION AND no "
                     "SBC_WASH/MGMT_SELLING/DEATH_SPIRAL -> UP [21,63] vs "
                     "sector ETF")}
    s3.put_object(Bucket=S3_BUCKET, Key=OUT,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    print(f"[cannibals] universe={len(rows)} screen={len(screen)} "
          f"logged={logged} {round(time.time() - t0, 1)}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "n": len(screen), "logged": logged})}
