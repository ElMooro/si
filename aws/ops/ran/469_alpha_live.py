#!/usr/bin/env python3
"""Step 469 — Verify /alpha/ page is live + screener has alpha integration."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/469_alpha_live.json"
NAME = "justhodl-tmp-469"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

def lambda_handler(event, context):
    out = {}
    pages = {
        "alpha_dashboard": "https://justhodl.ai/alpha/",
        "screener":         "https://justhodl.ai/screener/",
        "conviction":       "https://justhodl.ai/conviction/",
        "smart_money":      "https://justhodl.ai/smart-money/",
        "alpha_sidecar":    "https://justhodl-dashboard-live.s3.amazonaws.com/screener/alpha-score.json",
    }
    for name, url in pages.items():
        try:
            r = urllib.request.urlopen(url, timeout=15)
            body = r.read()
            out[name] = {"status": r.status, "size_kb": round(len(body)/1024, 1)}
            if name == "alpha_dashboard":
                # Confirm page has critical strings
                t = body.decode("utf-8", errors="ignore")
                out[name]["has_alpha_url"] = "alpha-score.json" in t
                out[name]["has_tier_S"] = "Tier S" in t
                out[name]["has_signals"] = "top_signals" in t
            elif name == "screener":
                t = body.decode("utf-8", errors="ignore")
                out[name]["has_alpha_col"] = "alphaScore" in t
                out[name]["has_alpha_tier_tabs"] = "alpha_tier_s" in t
                out[name]["has_alpha_url_const"] = "ALPHA_URL" in t
        except Exception as e:
            out[name] = {"err": str(e)[:200]}
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 60s for GitHub Pages deploy...")
    _time.sleep(60)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    _time.sleep(2)
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
