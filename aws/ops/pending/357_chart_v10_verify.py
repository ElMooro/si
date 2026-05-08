#!/usr/bin/env python3
"""Step 357 — Verify chart-pro v10 live on justhodl.ai."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/357_chart_v10_verify.json"
NAME = "justhodl-tmp-chart-v10-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.error, re

URL = "https://justhodl.ai/chart-pro.html"

V10_MARKERS = [
    "v10",                          # title bump
    "Chart Pro v10",                # console banner
    "Hull MA 20",                   # new indicator
    "TEMA 20",                      # new indicator
    "KAMA 10",                      # new indicator
    "Parabolic SAR",                # new indicator
    "SuperTrend",                   # new indicator
    "Williams %R",                  # new indicator
    "Aroon",                        # new indicator
    "TRIX",                         # new indicator
    "ZigZag",                       # new indicator
    "pitchfork",                    # new tool
    "position",                     # new tool (Long/Short)
    "gann",                         # new tool
    "magnetBtn",                    # magnet toggle
    "stayBtn",                      # stay-in-tool
    "drawingsList",                 # drawings sidebar
    "layoutsList",                  # layouts manager
    "chartTypeBar",                 # chart type switcher
    "Heikin Ashi",                  # chart type
    "synthesizeOHLCFromClose",      # candle synthesis fn
    "EVENT_MARKERS",                # event markers
    "renderVolumeProfile",          # VP rendering
    "toggleCompareMode",            # compare mode
    "kamaSeries",                   # KAMA fn
    "pitchfork",                    # tool data attr
]

def lambda_handler(event, context):
    r = {"url": URL}
    try:
        req = urllib.request.Request(URL, headers={
            "User-Agent": "Mozilla/5.0 (justhodl-verify-bot)",
            "Cache-Control": "no-cache",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            r["status"] = resp.status
            body = resp.read().decode("utf-8", errors="ignore")
            r["bytes"] = len(body)
            r["headers"] = {
                k.lower(): v for k, v in resp.headers.items()
                if k.lower() in ["last-modified", "etag", "age", "x-served-by"]
            }
            found, missing = [], []
            for m in V10_MARKERS:
                (found if m in body else missing).append(m)
            r["v10_markers_found"] = found
            r["v10_markers_missing"] = missing
            r["v10_score"] = f"{len(found)}/{len(V10_MARKERS)}"
            r["is_v10"] = len(missing) == 0
            titles = re.findall(r"<title>(.*?)</title>", body)
            r["title"] = titles[0] if titles else None
            # Count indicators in array
            ind_match = re.search(r"const INDICATORS = \\[(.*?)\\];", body, re.DOTALL)
            if ind_match:
                r["indicator_count"] = ind_match.group(1).count("{ key:")
            # Count drawing tools
            r["drawing_tools_count"] = len(re.findall(r'data-tool="[^"]+"', body))
    except Exception as e:
        r["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    return {"statusCode": 200, "body": json.dumps(r, default=str)}
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
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
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
