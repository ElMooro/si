#!/usr/bin/env python3
"""Step 378 — Diag bonds.html data sources."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/378_bonds_data_diag.json"
NAME = "justhodl-tmp-bonds-data"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error
import boto3

s3 = boto3.client("s3", region_name="us-east-1")

def head(key):
    try:
        h = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
        return {"exists": True, "size": h.get("ContentLength"),
                "last_modified": str(h.get("LastModified"))}
    except Exception as e:
        return {"exists": False, "error": str(e)[:120]}

def http(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "verify"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return {"status": r.status, "size": int(r.headers.get("content-length") or 0)}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "error": e.reason}
    except Exception as e:
        return {"error": str(e)[:120]}

def lambda_handler(event, context):
    out = {"keys": {}, "https": {}, "report_shape": None, "regime_shape": None, "crisis_shape": None}

    keys = ["data/report.json", "regime/current.json", "data/crisis-plumbing.json",
            "data/auction-crisis.json", "data/auction-tenor-signals.json"]
    for k in keys:
        out["keys"][k] = head(k)
        out["https"][k] = http(f"https://justhodl-dashboard-live.s3.amazonaws.com/{k}")

    # Inspect actual JSON shape of report.json (what bonds.html consumes)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["report_shape"] = {
            "size_bytes": len(body),
            "keys": list(d.keys())[:30],
            "has_yields": "yields" in d or "treasury" in d,
            "has_2y_10y": ("yields" in d and ("2y" in d.get("yields", {}) or "y2" in d.get("yields", {})))
                            or "dgs2" in d or "dgs10" in d,
            "has_generated_at": "generated_at" in d,
            "has_khalid_index": "khalid_index" in d,
            # Sample some likely fields
            "sample_top_level": {k: (str(v)[:80] if not isinstance(v, (dict, list)) else type(v).__name__ + f"(len={len(v) if hasattr(v, '__len__') else '?'})")
                                  for k, v in list(d.items())[:15]},
        }
    except Exception as e:
        out["report_shape"] = {"error": str(e)[:200]}

    # Inspect regime/current.json
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="regime/current.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["regime_shape"] = {
            "size_bytes": len(body),
            "keys": list(d.keys()),
            "regime": d.get("regime"),
            "regime_strength": d.get("regime_strength"),
        }
    except Exception as e:
        out["regime_shape"] = {"error": str(e)[:200]}

    # Inspect crisis-plumbing.json
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/crisis-plumbing.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["crisis_shape"] = {
            "size_bytes": len(body),
            "keys": list(d.keys())[:20],
        }
    except Exception as e:
        out["crisis_shape"] = {"error": str(e)[:200]}

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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
