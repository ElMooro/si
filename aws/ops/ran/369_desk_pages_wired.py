#!/usr/bin/env python3
"""Step 369 — Verify the 10 desk pages have wss-client.js wired."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/369_desk_pages_wired.json"
NAME = "justhodl-tmp-desk-wired-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request

PAGES = [
    "master-rank.html", "today.html", "desk.html", "alpha-scoreboard.html",
    "portfolio.html", "why.html", "flow.html", "themes.html",
    "nobrainers.html", "macro-data.html",
    # Plus index for completeness (was already wired in earlier commit)
    "index.html",
]

# Markers we want to confirm are present in each page
PAGE_MARKERS = [
    ("/wss-client.js", "wss client script tag"),
    ("manifest.json", "PWA manifest link"),
    ("service-worker.js", "service worker register"),
]

# Markers in /wss-client.js itself (self-injecting pill version)
CLIENT_MARKERS = [
    ("function injectPill", "self-injecting pill function"),
    ("DOMContentLoaded", "injects on DOMContentLoaded"),
    ("q7vco36knh", "real WSS URL hardcoded"),
    ("subscribers = new Map", "subscribers map"),
    ("scheduleReconnect", "exponential backoff reconnect"),
]

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/369"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="ignore")
            return r.status, body
    except Exception as e:
        return None, f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    out = {"pages": {}}

    # Each desk page
    for page in PAGES:
        url = f"https://justhodl.ai/{page}"
        status, body = fetch(url)
        if status != 200:
            out["pages"][page] = {"status": status, "error": body[:200]}
            continue
        hits = {label: marker in body for marker, label in PAGE_MARKERS}
        all_present = all(hits.values())
        out["pages"][page] = {
            "status": status,
            "size": len(body),
            "markers": hits,
            "ok": all_present,
        }

    # wss-client.js itself
    status, body = fetch("https://justhodl.ai/wss-client.js")
    if status == 200:
        hits = {label: marker in body for marker, label in CLIENT_MARKERS}
        out["wss_client_js"] = {
            "status": status, "size": len(body),
            "markers": hits,
            "ok": all(hits.values()),
        }
    else:
        out["wss_client_js"] = {"status": status, "error": body[:200]}

    # Summary
    n_pages_ok = sum(1 for v in out["pages"].values() if v.get("ok"))
    out["summary"] = {
        "pages_passing": f"{n_pages_ok}/{len(PAGES)}",
        "client_ok": out["wss_client_js"].get("ok", False),
    }
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
            MemorySize=256, Timeout=180, Code={"ZipFile": zb},
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
