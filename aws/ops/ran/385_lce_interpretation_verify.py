#!/usr/bin/env python3
"""Step 382 — Invoke expanded LCE + verify all 53 series + show category breakdown."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/382_lce_expansion_verify.json"
NAME = "justhodl-tmp-lce-exp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # Force fresh run
    resp = lam.invoke(FunctionName="justhodl-liquidity-credit-engine",
                       InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    out["invoke"] = {"status": resp.get("StatusCode"), "body": body[:500]}
    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/liquidity-credit-engine.json")
    d = json.loads(obj["Body"].read())
    out["regime"] = d.get("regime")
    out["composite"] = d.get("composite")
    out["schema_version"] = d.get("schema_version")
    out["reference_yields"] = d.get("reference")
    by_cat = d.get("by_category", {})
    out["category_counts"] = {c: len(ids) for c, ids in by_cat.items()}
    series = d.get("series", {})
    out["total_series"] = len(series)
    out["available_count"] = sum(1 for s in series.values() if s.get("available"))
    out["unavailable_series"] = {sid: s.get("error") for sid, s in series.items() if not s.get("available")}

    # Sample one signal per category
    summary_by_cat = {}
    for cat, ids in by_cat.items():
        firing = []
        normal = []
        for sid in ids:
            s = series.get(sid, {})
            if not s.get("available"):
                continue
            entry = {"sid": sid, "label": s.get("_label"), "value": s.get("latest_value"),
                       "wow": s.get("wow_pct"), "yoy": s.get("yoy_pct"), "signal": s.get("signal"),
                       "reason": (s.get("signal_reason") or "")[:120]}
            if s.get("signal") in ("WATCH", "ELEVATED", "CRISIS"):
                firing.append(entry)
            else:
                normal.append(entry)
        summary_by_cat[cat] = {"n_firing": len(firing), "n_normal": len(normal),
                                 "firing_sample": firing[:8]}
    out["summary_by_cat"] = summary_by_cat
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=600, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
