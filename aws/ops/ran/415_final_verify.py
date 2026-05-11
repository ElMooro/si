#!/usr/bin/env python3
"""Step 415 — Final verify: data populated correctly with fixed margins
+ rescaled Steal Scores. Show Top 15 + buckets."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/415_final_verify.json"
NAME = "justhodl-tmp-final-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        stocks = d.get("stocks") or []
        out["data"] = {
            "generated_at": d.get("generated_at"),
            "size_kb": round(len(body) / 1024, 1),
            "n_stocks": len(stocks),
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        cov = {}
        for field in ("revenue","netIncome","freeCashFlow","fcfYieldCalc",
                       "rev3yCAGR","sustainable3y","buybackYield",
                       "insiderNet90dUsd","insiderSignal","beatStreak",
                       "stealScore","stealBucket","stealScoreRaw",
                       "operatingMargin","netMargin","roe","revenueGrowth"):
            cov[field] = sum(1 for s in stocks if s.get(field) is not None)
        out["coverage"] = cov

        scored = [s for s in stocks if s.get("stealScore") is not None]
        if scored:
            scs = [s["stealScore"] for s in scored]
            out["steal_dist"] = {
                "n_scored": len(scored),
                "ge_90": sum(1 for x in scs if x >= 90),
                "ge_80": sum(1 for x in scs if x >= 80),
                "ge_70": sum(1 for x in scs if x >= 70),
                "mean": round(sum(scs)/len(scs), 1),
                "median": sorted(scs)[len(scs)//2],
                "max": max(scs), "min": min(scs),
            }
            top = sorted(scored, key=lambda x: -x["stealScore"])[:20]
            out["top20"] = [{
                "sym": s["symbol"], "name": (s.get("name") or "")[:25],
                "sector": s.get("sector"), "score": s["stealScore"],
                "rawScore": s.get("stealScoreRaw"),
                "bucket": s.get("stealBucket"),
                "pe": s.get("peRatio"), "rev_g": s.get("revenueGrowth"),
                "op_m": s.get("operatingMargin"), "roic": s.get("roic"),
                "roe": s.get("roe"),
                "fcf_y": s.get("fcfYieldCalc"),
                "ni": s.get("netIncome"),
                "ins_buys": s.get("insiderBuys90dUsd"),
                "ins_net": s.get("insiderNet90dUsd"),
                "ins_buyers": s.get("insiderBuyersN90d"),
                "beat_streak": s.get("beatStreak"),
                "chg6m": s.get("chg6m"),
                "chg1y": s.get("chg1y"),
            } for s in top]

        # Spot-check: AAPL, NVDA, META — should have visible margins now
        for ticker in ("AAPL","NVDA","META","TSLA","GOOGL"):
            s = next((x for x in stocks if x["symbol"] == ticker), None)
            if s:
                out["spot_" + ticker] = {
                    "sector": s.get("sector"),
                    "marketCap_B": round((s.get("marketCap") or 0)/1e9, 1),
                    "revenue_B": round((s.get("revenue") or 0)/1e9, 1),
                    "netIncome_B": round((s.get("netIncome") or 0)/1e9, 1),
                    "fcf_B": round((s.get("freeCashFlow") or 0)/1e9, 1),
                    "roe": s.get("roe"),
                    "opMargin": s.get("operatingMargin"),
                    "netMargin": s.get("netMargin"),
                    "revGrowth": s.get("revenueGrowth"),
                    "stealScore": s.get("stealScore"),
                    "stealRank": s.get("stealRank"),
                    "bucket": s.get("stealBucket"),
                }
    except Exception as e:
        out["err"] = str(e)[:300]

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
                            MemorySize=256, Timeout=60, Code={"ZipFile": zb})
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
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
