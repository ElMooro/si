#!/usr/bin/env python3
"""Step 429 — Verify Stage 10 field coverage + show top hedge fund
accumulating + top forward growth + cheap forward P/E + famous tickers."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/429_stage10_verify.json"
NAME = "justhodl-tmp-s10-verify"
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
        cov = {}
        for f in ["instInvestorsHolding","instInvestorsChange","instInvestorsChgPct",
                  "instSharesChangePct","instSignal","instQuarter",
                  "forwardRevenue","forwardRevenueGrowth","forwardEbitda","forwardPE","forwardYear",
                  "freeFloatPct","floatShares","outstandingShares",
                  "ceoName","ceoPay","nExecutives"]:
            cov[f] = sum(1 for s in stocks if s.get(f) is not None)
        out["coverage"] = cov

        # Top hedge-fund accumulating
        hf = sorted([s for s in stocks if (s.get("instSharesChangePct") or 0) > 0],
                     key=lambda x: -(x.get("instSharesChangePct") or 0))[:15]
        out["top_hedge_fund"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                    "sector": s.get("sector"),
                                    "holders": s.get("instInvestorsHolding"),
                                    "holders_chg": s.get("instInvestorsChange"),
                                    "shares_chg_pct": s.get("instSharesChangePct"),
                                    "quarter": s.get("instQuarter"),
                                    "score": s.get("stealScore"),
                                    "bucket": s.get("stealBucket")}
                                   for s in hf]

        # Top forward revenue growth
        fwd = sorted([s for s in stocks if (s.get("forwardRevenueGrowth") or -999) > -100],
                      key=lambda x: -(x.get("forwardRevenueGrowth") or 0))[:15]
        out["top_forward_growth"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                        "sector": s.get("sector"),
                                        "rev_now": s.get("revenue"),
                                        "rev_fwd": s.get("forwardRevenue"),
                                        "growth_pct": s.get("forwardRevenueGrowth"),
                                        "fwd_pe": s.get("forwardPE"),
                                        "fwd_year": s.get("forwardYear"),
                                        "score": s.get("stealScore")}
                                       for s in fwd]

        # Cheap forward P/E
        cheap = sorted([s for s in stocks if s.get("forwardPE") and 0 < s.get("forwardPE") < 15],
                        key=lambda x: x.get("forwardPE"))[:15]
        out["top_cheap_fpe"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                   "fwd_pe": s.get("forwardPE"),
                                   "pe_now": s.get("peRatio"),
                                   "fwd_growth": s.get("forwardRevenueGrowth"),
                                   "score": s.get("stealScore"),
                                   "bucket": s.get("stealBucket")}
                                  for s in cheap]

        # Spot checks
        for ticker in ("AAPL","NVDA","META","GOOGL","TSLA","CF","NEM","EQT","MSFT","BRK.B","JPM"):
            s = next((x for x in stocks if x["symbol"] == ticker), None)
            if s:
                out["spot_" + ticker] = {
                    "stealScore": s.get("stealScore"),
                    "instHolders": s.get("instInvestorsHolding"),
                    "instHoldersChg": s.get("instInvestorsChange"),
                    "instSharesChgPct": s.get("instSharesChangePct"),
                    "instSignal": s.get("instSignal"),
                    "instQuarter": s.get("instQuarter"),
                    "forwardRev": s.get("forwardRevenue"),
                    "forwardRevGrowth": s.get("forwardRevenueGrowth"),
                    "forwardPE": s.get("forwardPE"),
                    "forwardYear": s.get("forwardYear"),
                    "freeFloatPct": s.get("freeFloatPct"),
                    "ceoName": s.get("ceoName"),
                    "ceoPay": s.get("ceoPay"),
                }
    except Exception as e:
        out["err"] = str(e)[:300]

    # Lambda log tail
    try:
        lg = "/aws/lambda/justhodl-stock-screener"
        sts = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                          descending=True, limit=2)
        lines = []
        for st in sts.get("logStreams", []):
            ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"],
                                       startFromHead=False, limit=80)
            for e in ev.get("events", []):
                m = e["message"].strip()
                if any(k in m for k in ("DONE:","[inst]","[history]","[just-crossed]",
                                          "Processing","REPORT RequestId")):
                    lines.append((e["timestamp"], m))
        lines.sort()
        out["log_relevant"] = [{"ts": ts, "msg": m[:200]} for ts, m in lines[-20:]]
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
