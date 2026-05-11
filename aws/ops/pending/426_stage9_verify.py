#!/usr/bin/env python3
"""Step 426 — Verify Stage 9 field coverage after first run with the new
6 endpoints. Shows: data freshness, coverage of all new fields, top
politician-bought stocks, top target upside, top DCF deep value."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/426_stage9_verify.json"
NAME = "justhodl-tmp-s9-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        stocks = d.get("stocks") or []
        out["data"] = {
            "generated_at": d.get("generated_at"),
            "size_kb": round(len(body)/1024, 1),
            "n_stocks": len(stocks),
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        # Coverage of Stage 9 fields
        cov = {}
        for f in ["politicalBuyersN90d","politicalNet90dUsd","politicalSignal",
                  "senateBuysN90d","houseBuysN90d",
                  "priceTargetMean","priceTargetUpsidePct","priceTargetCount30d",
                  "gradesConsensus","gradesScore","upgradeNet30d","upgradeNet90d",
                  "dcfFairValue","dcfUpsidePct",
                  "esgRating","esgScoreNumeric"]:
            cov[f] = sum(1 for s in stocks if s.get(f) is not None)
        out["coverage"] = cov

        # Top 15 by politician-buying
        pol = [s for s in stocks if (s.get("politicalBuyersN90d") or 0) > 0]
        pol.sort(key=lambda x: -(x.get("politicalBuyersN90d") or 0))
        out["top_political"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                   "sector": s.get("sector"),
                                   "buyers": s.get("politicalBuyersN90d"),
                                   "senate": s.get("senateBuysN90d"),
                                   "house": s.get("houseBuysN90d"),
                                   "net_usd": s.get("politicalNet90dUsd"),
                                   "signal": s.get("politicalSignal"),
                                   "score": s.get("stealScore"),
                                   "bucket": s.get("stealBucket")}
                                  for s in pol[:15]]

        # Top 15 by target upside
        pt = [s for s in stocks if (s.get("priceTargetUpsidePct") or -999) > -50]
        pt.sort(key=lambda x: -(x.get("priceTargetUpsidePct") or 0))
        out["top_target_upside"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                       "sector": s.get("sector"),
                                       "price": s.get("price"),
                                       "target": s.get("priceTargetMean"),
                                       "upside_pct": s.get("priceTargetUpsidePct"),
                                       "consensus": s.get("gradesConsensus"),
                                       "score": s.get("stealScore"),
                                       "bucket": s.get("stealBucket")}
                                      for s in pt[:15]]

        # Top 15 by DCF upside
        dcf = [s for s in stocks if s.get("dcfUpsidePct") is not None]
        dcf.sort(key=lambda x: -(x.get("dcfUpsidePct") or 0))
        out["top_dcf_value"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                    "sector": s.get("sector"),
                                    "price": s.get("price"),
                                    "dcf": s.get("dcfFairValue"),
                                    "upside_pct": s.get("dcfUpsidePct"),
                                    "score": s.get("stealScore"),
                                    "bucket": s.get("stealBucket")}
                                   for s in dcf[:15]]

        # Top analyst upgrade momentum
        up = [s for s in stocks if (s.get("upgradeNet30d") or 0) >= 1]
        up.sort(key=lambda x: -(x.get("upgradeNet30d") or 0))
        out["top_upgrades"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                  "up_net_30d": s.get("upgradeNet30d"),
                                  "up_net_90d": s.get("upgradeNet90d"),
                                  "consensus": s.get("gradesConsensus"),
                                  "score": s.get("stealScore"),
                                  "bucket": s.get("stealBucket")}
                                 for s in up[:15]]

        # Spot-check famous tickers
        for ticker in ("AAPL","NVDA","META","GOOGL","TSLA","CF","NEM","EQT","INCY","MSFT","PLTR","INTC"):
            s = next((x for x in stocks if x["symbol"] == ticker), None)
            if s:
                out["spot_" + ticker] = {
                    "stealScore": s.get("stealScore"),
                    "bucket": s.get("stealBucket"),
                    "politicalBuyers": s.get("politicalBuyersN90d"),
                    "politicalSignal": s.get("politicalSignal"),
                    "targetUpside": s.get("priceTargetUpsidePct"),
                    "gradesConsensus": s.get("gradesConsensus"),
                    "gradesScore": s.get("gradesScore"),
                    "upgrades30d": s.get("upgrades30d"),
                    "downgrades30d": s.get("downgrades30d"),
                    "dcfFairValue": s.get("dcfFairValue"),
                    "dcfUpsidePct": s.get("dcfUpsidePct"),
                    "esgRating": s.get("esgRating"),
                }

        # Steal Score distribution (after Stage 9 factor additions)
        scored = [s for s in stocks if s.get("stealScore") is not None]
        scs = [s["stealScore"] for s in scored]
        if scs:
            out["steal_dist"] = {
                "n_scored": len(scored),
                "ge_90": sum(1 for x in scs if x >= 90),
                "ge_80": sum(1 for x in scs if x >= 80),
                "ge_70": sum(1 for x in scs if x >= 70),
                "max": max(scs), "min": min(scs),
                "mean": round(sum(scs)/len(scs), 1),
            }
    except Exception as e:
        out["err"] = str(e)[:300]

    # Lambda log tail (look for Stage 9 endpoint errors + history events)
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=120)
            for e in ev.get("events", []):
                m = e["message"].strip()
                if any(k in m for k in ("DONE:","[history]","[just-crossed]",
                                          "senate-trades","house-trades","price-target",
                                          "grades-consensus","discounted-cash-flow",
                                          "esg-ratings","REPORT RequestId")):
                    lines.append((e["timestamp"], m))
        lines.sort()
        out["log_relevant"] = [{"ts": ts, "msg": m[:200]} for ts, m in lines[-25:]]
    except Exception as e:
        out["log_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
