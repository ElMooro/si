#!/usr/bin/env python3
"""Step 413 — Verify the clean force-refresh produced populated data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/413_verify_refresh.json"
NAME = "justhodl-tmp-verify"
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
            "size_kb": round(len(body) / 1024, 1),
            "n_stocks": len(stocks),
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        # Coverage
        cov = {}
        for field in ("revenue", "netIncome", "freeCashFlow", "fcfYieldCalc",
                       "rev3yCAGR", "sustainable3y",
                       "insiderNet90dUsd", "insiderSignal", "beatStreak",
                       "stealScore", "stealBucket"):
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
            top = sorted(scored, key=lambda x: -x["stealScore"])[:15]
            out["top15"] = [{
                "sym": s["symbol"], "name": (s.get("name") or "")[:25],
                "sector": s.get("sector"), "score": s["stealScore"],
                "bucket": s.get("stealBucket"),
                "pe": s.get("peRatio"), "rev_g": s.get("revenueGrowth"),
                "op_m": s.get("operatingMargin"), "roic": s.get("roic"),
                "fcf_y": s.get("fcfYieldCalc"),
                "ni": s.get("netIncome"),
                "ins_buys": s.get("insiderBuys90dUsd"),
                "ins_net": s.get("insiderNet90dUsd"),
                "beat_streak": s.get("beatStreak"),
                "chg6m": s.get("chg6m"),
            } for s in top]
    except Exception as e:
        out["err"] = str(e)[:300]

    # Last log stream tail
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        latest_lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=30)
            latest_lines.extend([(e["timestamp"], e["message"].strip())
                                   for e in ev.get("events", [])])
        latest_lines.sort()
        out["log_tail"] = [{"ts": ts, "msg": m[:200]} for ts, m in latest_lines[-25:]]
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
