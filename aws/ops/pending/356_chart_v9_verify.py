#!/usr/bin/env python3
"""Step 356 — Verify chart-pro v9 is live on justhodl.ai.

Sandbox can't reach justhodl.ai directly. Spin a temp Lambda that
curls the URL from inside AWS (full internet egress), checks for v9
markers, writes ops/reports/356_chart_v9_verify.json.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/356_chart_v9_verify.json"
NAME = "justhodl-tmp-chart-v9-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error

URLS = [
    "https://justhodl.ai/chart-pro.html",
    "https://www.justhodl.ai/chart-pro.html",
    "https://elmooro.github.io/si/chart-pro.html",
]

V9_MARKERS = [
    "v9",                    # title bump
    "VWAP",                  # new indicator
    "Ichimoku",              # new indicator
    "ATR (14)",              # new indicator
    "ADX (14)",              # new indicator
    "Fibonacci",             # new tool
    "aiPane",                # AI co-pilot DOM id
    "replayBar",             # bar replay DOM id
    "cmdOverlay",            # cmd-K palette DOM id
    "regimeBanner",          # regime overlay DOM id
    "Chart Pro v9",          # console log marker
]

def lambda_handler(event, context):
    out = {"results": []}
    for url in URLS:
        r = {"url": url}
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (justhodl-verify-bot)",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                r["status"] = resp.status
                body = resp.read().decode("utf-8", errors="ignore")
                r["bytes"] = len(body)
                r["headers"] = {
                    k.lower(): v for k, v in resp.headers.items()
                    if k.lower() in ["server", "etag", "last-modified",
                                     "cache-control", "x-served-by",
                                     "cf-cache-status", "cf-ray",
                                     "x-github-request-id", "age"]
                }
                # Look for v9 markers
                found, missing = [], []
                for m in V9_MARKERS:
                    (found if m in body else missing).append(m)
                r["v9_markers_found"] = found
                r["v9_markers_missing"] = missing
                r["v9_score"] = f"{len(found)}/{len(V9_MARKERS)}"
                r["is_v9"] = len(missing) == 0
                # Sniff version-ish content
                import re
                titles = re.findall(r"<title>(.*?)</title>", body)
                r["title"] = titles[0] if titles else None
        except urllib.error.HTTPError as e:
            r["status"] = e.code
            r["error"] = f"HTTPError: {e.reason}"
        except Exception as e:
            r["error"] = f"{type(e).__name__}: {str(e)[:200]}"
        out["results"].append(r)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(
            FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            MemorySize=256, Timeout=60, Code={"ZipFile": zb},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        if "body" in parsed:
            out["test"] = json.loads(parsed["body"])
        else:
            out["raw"] = parsed
    except Exception:
        out["raw"] = body[:2000]
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
