#!/usr/bin/env python3
"""Step 431 — Verify institutional data is realistic after 13F quarter fix."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/431_stage10_final.json"
NAME = "justhodl-tmp-s10-final"
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
        # Check 13F quarter used (from any stock that has one)
        sample_q = next((s.get("instQuarter") for s in stocks if s.get("instQuarter")), None)
        out["used_quarter"] = sample_q
        # Distribution of instSharesChangePct — should now look NORMAL (mostly near 0)
        chgs = [s.get("instSharesChangePct") for s in stocks if s.get("instSharesChangePct") is not None]
        if chgs:
            chgs.sort()
            n = len(chgs)
            out["inst_shares_dist"] = {
                "n": n,
                "min": chgs[0], "max": chgs[-1],
                "p5":   chgs[n//20], "p25":  chgs[n//4],
                "p50":  chgs[n//2], "p75":  chgs[3*n//4],
                "p95":  chgs[19*n//20],
                "mean": round(sum(chgs)/n, 2),
                "n_positive_5":  sum(1 for x in chgs if x >= 5),
                "n_negative_5":  sum(1 for x in chgs if x <= -5),
            }
        # Top 15 hedge fund accumulating
        hf = sorted([s for s in stocks if (s.get("instSharesChangePct") or 0) > 0],
                     key=lambda x: -(x.get("instSharesChangePct") or 0))[:15]
        out["top_hf"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                            "sector": s.get("sector"),
                            "holders": s.get("instInvestorsHolding"),
                            "h_chg": s.get("instInvestorsChange"),
                            "shares_chg_pct": s.get("instSharesChangePct"),
                            "score": s.get("stealScore")}
                           for s in hf]
        # Top 15 hedge fund EXITING
        hfex = sorted([s for s in stocks if (s.get("instSharesChangePct") or 0) < 0],
                       key=lambda x: x.get("instSharesChangePct") or 0)[:15]
        out["top_hf_exit"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:24],
                                 "sector": s.get("sector"),
                                 "shares_chg_pct": s.get("instSharesChangePct"),
                                 "score": s.get("stealScore")}
                                for s in hfex]
        # Spot-check famous tickers
        for ticker in ("AAPL","NVDA","META","GOOGL","TSLA","CF","NEM","EQT","MSFT"):
            s = next((x for x in stocks if x["symbol"] == ticker), None)
            if s:
                out["spot_" + ticker] = {
                    "instHolders": s.get("instInvestorsHolding"),
                    "instHoldersChg": s.get("instInvestorsChange"),
                    "instHoldersChgPct": s.get("instInvestorsChgPct"),
                    "instSharesChgPct": s.get("instSharesChangePct"),
                    "instSignal": s.get("instSignal"),
                    "instQuarter": s.get("instQuarter"),
                    "stealScore": s.get("stealScore"),
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
                if any(k in m for k in ("DONE:","[inst]","REPORT RequestId","[just-crossed]","[history]")):
                    lines.append((e["timestamp"], m))
        lines.sort()
        out["log_relevant"] = [{"ts": ts, "msg": m[:200]} for ts, m in lines[-15:]]
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
