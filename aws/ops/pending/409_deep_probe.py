#!/usr/bin/env python3
"""Step 409 — Deep probe: LCE structure + GBC aggregate sub-object,
plus check whether screener Lambda has finished its 408-triggered refresh."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/409_deep_probe.json"
NAME = "justhodl-tmp-deep-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # LCE — list all S3 keys under data/ to find the actual file location
    try:
        resp = s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="data/")
        objs = resp.get("Contents") or []
        lce_keys = [{"key": o["Key"], "size": o["Size"], "modified": str(o["LastModified"])}
                      for o in objs if "liquidity" in o["Key"].lower() or "lce" in o["Key"].lower()]
        out["lce_candidates"] = lce_keys
    except Exception as e:
        out["lce_list_err"] = str(e)[:200]

    # Try the most likely candidate
    for candidate_key in ["data/liquidity-conditions-engine.json",
                            "data/lce.json",
                            "data/liquidity_conditions_engine.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=candidate_key)
            body = obj["Body"].read()
            if body:
                d = json.loads(body)
                out["lce_found"] = {
                    "key": candidate_key,
                    "size_kb": round(len(body) / 1024, 1),
                    "top_keys": list(d.keys())[:30] if isinstance(d, dict) else "non-dict",
                    "raw_excerpt": (json.dumps(d, default=str)[:500] if d else "empty"),
                }
                break
        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            out["lce_try_" + candidate_key] = str(e)[:150]

    # GBC aggregate sub-object
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                             Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate") or {}
        interp = d.get("interpretation") or {}
        out["gbc_aggregate_keys"] = list(agg.keys())[:30] if isinstance(agg, dict) else "non-dict"
        out["gbc_aggregate_sample"] = json.dumps(agg, default=str)[:600] if agg else None
        out["gbc_interpretation_keys"] = list(interp.keys())[:30] if isinstance(interp, dict) else "non-dict"
        out["gbc_interpretation_sample"] = json.dumps(interp, default=str)[:600] if interp else None
        out["gbc_methodology"] = d.get("methodology", "")[:200]
    except Exception as e:
        out["gbc_err"] = str(e)[:200]

    # Screener: check if force refresh completed
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        stocks = d.get("stocks") or []
        with_rev = sum(1 for s in stocks if s.get("revenue") is not None)
        with_steal = sum(1 for s in stocks if s.get("stealScore") is not None)
        out["screener"] = {
            "generated_at": d.get("generated_at"),
            "n_stocks": len(stocks),
            "with_revenue": with_rev,
            "with_steal": with_steal,
            "elapsed_seconds": d.get("elapsed_seconds"),
        }
        if with_steal > 0:
            scored = sorted([s for s in stocks if s.get("stealScore") is not None],
                              key=lambda x: -x["stealScore"])[:15]
            out["top_steals"] = [{"sym": s["symbol"], "name": (s.get("name") or "")[:25],
                                    "sector": s.get("sector"), "score": s["stealScore"],
                                    "bucket": s.get("stealBucket"),
                                    "pe": s.get("peRatio"), "rev_g": s.get("revenueGrowth"),
                                    "op_m": s.get("operatingMargin"),
                                    "fcf_y": s.get("fcfYieldCalc"),
                                    "ins_qoq": s.get("instQoQChgPct")} for s in scored]
            scs = [s["stealScore"] for s in stocks if s.get("stealScore") is not None]
            out["dist"] = {
                "n": len(scs), "max": max(scs), "min": min(scs),
                "mean": round(sum(scs)/len(scs),1),
                "ge_90": sum(1 for x in scs if x >= 90),
                "ge_80": sum(1 for x in scs if x >= 80),
                "ge_70": sum(1 for x in scs if x >= 70),
            }
    except Exception as e:
        out["screener_err"] = str(e)[:200]

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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
