#!/usr/bin/env python3
"""Step 374 — Verify stress.html links propagated to live pages."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/374_stress_nav_verify.json"
NAME = "justhodl-tmp-stress-nav-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request

# Sample of pages we expect to have stress.html link
PAGES = [
    "index.html", "risk.html", "master-rank.html", "alpha-scoreboard.html",
    "desk.html", "today.html", "themes.html", "nobrainers.html", "flow.html",
    "macro-data.html", "portfolio.html", "why.html", "analogs.html",
    "cross-asset.html", "cot-extremes.html", "gdelt.html",
    "options-scanner.html", "dealer-survey.html", "narrative.html",
    "stress.html",  # itself — should also have ?preset deep-link support
]

def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/374"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return None, f"ERROR: {type(e).__name__}: {e}"

def lambda_handler(event, context):
    out = {"pages": {}}

    for page in PAGES:
        url = f"https://justhodl.ai/{page}"
        status, body = fetch(url)
        if status != 200:
            out["pages"][page] = {"status": status, "error": body[:200] if isinstance(body, str) else body}
            continue
        out["pages"][page] = {
            "status": status,
            "size": len(body),
            "has_stress_link": "stress.html" in body,
            "has_stress_card": "STRESS TEST" in body if page == "index.html" else None,
            "has_url_param_handler": "URLSearchParams" in body and "preset" in body if page == "stress.html" else None,
            "ok": "stress.html" in body,
        }

    # Also test the deep-link works
    s, body = fetch("https://justhodl.ai/stress.html?preset=gfc_2008")
    out["deep_link_test"] = {
        "status": s,
        "size": len(body) if isinstance(body, str) else None,
        "has_url_param_handler": ("URLSearchParams" in body and "presetFromUrl" in body) if isinstance(body, str) else False,
    }

    n_ok = sum(1 for v in out["pages"].values() if v.get("ok"))
    out["summary"] = {
        "passed": n_ok, "total": len(PAGES),
        "rate": f"{n_ok}/{len(PAGES)}",
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
