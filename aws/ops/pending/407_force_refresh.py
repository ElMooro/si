#!/usr/bin/env python3
"""Step 407 — Force screener Lambda refresh (bypass cache) + inspect macro
file schemas so we know the actual key paths for posture + phase."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/407_force_refresh.json"
NAME = "justhodl-tmp-force-refresh"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Inspect LCE file schema
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/liquidity-conditions-engine.json")
        d = json.loads(obj["Body"].read())
        out["lce_keys"] = list(d.keys())[:25]
        # Try common posture-bearing paths
        out["lce_probe"] = {
            "posture": d.get("posture"),
            "regime": d.get("regime"),
            "current_posture": d.get("current_posture"),
            "stance": d.get("stance"),
            "snapshot_posture": (d.get("snapshot") or {}).get("posture"),
            "snapshot_regime": (d.get("snapshot") or {}).get("regime"),
            "current_stance": (d.get("current") or {}).get("stance"),
            "summary_posture": (d.get("summary") or {}).get("posture"),
        }
    except Exception as e:
        out["lce_err"] = str(e)[:200]

    # 2. Inspect GBC file schema
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        out["gbc_keys"] = list(d.keys())[:25]
        out["gbc_probe"] = {
            "global_phase": d.get("global_phase"),
            "phase": d.get("phase"),
            "aggregate_phase": d.get("aggregate_phase"),
            "current_phase": d.get("current_phase"),
            "summary_phase": (d.get("summary") or {}).get("phase"),
        }
    except Exception as e:
        out["gbc_err"] = str(e)[:200]

    # 3. Force-refresh screener Lambda — this BLOCKS until complete (sync)
    print("Triggering screener with force=true...")
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="RequestResponse",  # synchronous so we wait
            Payload=json.dumps({"force": True}).encode(),
        )
        elapsed = round(time.time() - t0, 1)
        body = resp["Payload"].read().decode()
        out["screener_run"] = {
            "elapsed_sec": elapsed,
            "status_code": resp.get("StatusCode"),
            "body": body[:600],
        }
    except Exception as e:
        out["screener_run"] = {"error": str(e)[:300], "elapsed_sec": round(time.time() - t0, 1)}

    # 4. After refresh, re-read data.json to confirm new fields populated
    time.sleep(3)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        stocks = d.get("stocks") or []
        out["fresh_data"] = {
            "generated_at": d.get("generated_at"),
            "size_kb": round(len(body) / 1024, 1),
            "n_stocks": len(stocks),
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        # Take a sample with revenue data
        sample = next((s for s in stocks if s.get("revenue") is not None), {})
        out["fresh_data"]["sample_symbol"] = sample.get("symbol")
        out["fresh_data"]["sample_fields"] = {
            "revenue": sample.get("revenue"),
            "netIncome": sample.get("netIncome"),
            "operatingIncome": sample.get("operatingIncome"),
            "freeCashFlow": sample.get("freeCashFlow"),
            "fcfYieldCalc": sample.get("fcfYieldCalc"),
            "instOwnershipPct": sample.get("instOwnershipPct"),
            "instQoQChgPct": sample.get("instQoQChgPct"),
            "insiderSignal": sample.get("insiderSignal"),
            "beatStreak": sample.get("beatStreak"),
            "stealScore": sample.get("stealScore"),
            "stealBucket": sample.get("stealBucket"),
        }
        # Coverage stats
        with_rev = sum(1 for s in stocks if s.get("revenue") is not None)
        with_inst = sum(1 for s in stocks if s.get("instOwnershipPct") is not None)
        with_steal = sum(1 for s in stocks if s.get("stealScore") is not None)
        out["coverage"] = {
            "n_stocks": len(stocks),
            "revenue_populated": with_rev,
            "inst_populated": with_inst,
            "stealscore_populated": with_steal,
            "pct_revenue": round(with_rev / len(stocks) * 100, 1) if stocks else 0,
            "pct_inst": round(with_inst / len(stocks) * 100, 1) if stocks else 0,
            "pct_steal": round(with_steal / len(stocks) * 100, 1) if stocks else 0,
        }
        # Top stealScore rankings
        scored = sorted([s for s in stocks if s.get("stealScore") is not None],
                         key=lambda x: -x["stealScore"])
        out["top_steal"] = [{
            "symbol": s["symbol"], "name": (s.get("name") or "")[:25],
            "sector": s.get("sector"), "score": s["stealScore"], "bucket": s.get("stealBucket"),
            "pe": s.get("peRatio"), "rev_growth": s.get("revenueGrowth"),
            "op_margin": s.get("operatingMargin"), "roic": s.get("roic"),
            "fcf_yield": s.get("fcfYieldCalc"),
        } for s in scored[:15]]
        # Distribution
        if scored:
            scs = [s["stealScore"] for s in scored]
            out["steal_dist"] = {
                "ge_90": sum(1 for x in scs if x >= 90),
                "ge_80": sum(1 for x in scs if x >= 80),
                "ge_70": sum(1 for x in scs if x >= 70),
                "mean": round(sum(scs) / len(scs), 1),
                "median": scs[len(scs) // 2] if scs else None,
                "max": max(scs), "min": min(scs),
            }
    except Exception as e:
        out["fresh_data_err"] = str(e)[:300]

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
                            MemorySize=512, Timeout=900,  # screener can take 7min+; need long timeout
                            Code={"ZipFile": zb})
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
