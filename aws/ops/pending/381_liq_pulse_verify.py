#!/usr/bin/env python3
"""Step 381 — Verify liquidity-pulse end-to-end."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/381_liq_pulse_verify.json"
NAME = "justhodl-tmp-liq-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3

s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    # 1. S3 output
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/liquidity-pulse.json")
    body = obj["Body"].read()
    d = json.loads(body)
    out["s3"] = {
        "size_bytes": len(body),
        "last_modified": str(obj.get("LastModified")),
        "schema_version": d.get("schema_version"),
        "n_series": d.get("n_series"),
        "n_series_ok": d.get("n_series_ok"),
        "fetch_errors": d.get("fetch_errors"),
        "summary": d.get("summary"),
        "composites": d.get("composites"),
    }
    # Per-series highlights
    series = d.get("series", {})
    out["series_highlights"] = {}
    for sid, s in series.items():
        out["series_highlights"][sid] = {
            "label": s.get("label"),
            "group": s.get("group"),
            "latest": s.get("latest_value"),
            "date": s.get("latest_date"),
            "wow": (s.get("deltas") or {}).get("wow_pct"),
            "mom": (s.get("deltas") or {}).get("mom_pct"),
            "qoq": (s.get("deltas") or {}).get("qoq_pct"),
            "yoy": (s.get("deltas") or {}).get("yoy_pct"),
            "z": s.get("z_score"),
            "signal": s.get("signal"),
        }

    # 2. JS module live
    try:
        req = urllib.request.Request("https://justhodl.ai/liquidity-pulse.js",
                                       headers={"User-Agent": "v"})
        with urllib.request.urlopen(req, timeout=10) as r:
            js = r.read().decode("utf-8")
            out["js_live"] = {"status": r.status, "size": len(js),
                              "has_pill": "jh-liq-pill" in js}
    except Exception as e:
        out["js_live"] = {"error": str(e)}

    # 3. Verify a few wired pages
    pages = ["liquidity.html", "bonds.html", "crisis.html", "risk.html", "index.html",
             "auctions.html", "macro-data.html"]
    out["pages"] = {}
    for p in pages:
        try:
            req = urllib.request.Request(f"https://justhodl.ai/{p}",
                                           headers={"User-Agent": "v"})
            with urllib.request.urlopen(req, timeout=10) as r:
                page = r.read().decode("utf-8")
                out["pages"][p] = {
                    "status": r.status,
                    "has_script": "liquidity-pulse.js" in page,
                    "has_panel": 'id="liquidity-pulse-panel"' in page,
                }
        except Exception as e:
            out["pages"][p] = {"error": str(e)}

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
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
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
