#!/usr/bin/env python3
"""Step 399 — Fetch live global-cycle.html via Lambda and confirm v2.0
methodology + freshness UI deployed."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/399_page_deploy_check.json"
NAME = "justhodl-tmp-page-check"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request

def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH-deploy-check/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def lambda_handler(event, context):
    out = {}
    try:
        page = fetch("https://justhodl.ai/global-cycle.html?cb=" + str(int(__import__("time").time())))
        # v2 markers
        v2 = {
            "synthetic_in_methodology": ("synthetic" in page.lower()
                                            and "equity-momentum" in page.lower()),
            "fresh_tag_css": "fresh-tag" in page,
            "countries_with_fresh_data_js": "countries_with_fresh_data" in page,
            "thirty_four_economies": "34 major economies" in page,
            "v2_engine_label": "synthetic_equity_momentum" in page,
            "schema_2_0": "Schema 2.0" in page,
            "yahoo_finance_link": "finance.yahoo.com" in page,
            "DATA FRESHNESS card": "DATA FRESHNESS" in page,
            "ret_3m_in_tile": "3m ${fmtPct" in page or "3m \\\\${fmtPct" in page,
        }
        # v1 stale markers (should be gone)
        v1_stale = {
            "old_meth_OECD_phrase":  "Composite Leading Indicator (CLI)</strong> for 35 major economies from\\n      FRED" in page,
            "old_thirtyfive":        "35 major economies" in page,
            "old_subtitle":          "OECD Composite Leading Indicator across 35 major economies" in page,
        }
        out["v2_markers"] = v2
        out["v1_stale_markers"] = v1_stale
        out["page_size"] = len(page)
        # Snippet
        idx = page.find("Synthetic Composite")
        if idx > 0:
            out["meth_snippet"] = page[idx:idx+400]
    except Exception as e:
        out["err"] = str(e)[:300]

    # Also fetch the JSON to confirm fresh_count surfaces
    try:
        d = json.loads(fetch("https://justhodl-dashboard-live.s3.amazonaws.com/data/global-business-cycle.json"))
        out["s3_summary"] = {
            "schema_version": d.get("schema_version"),
            "engine_type": d.get("engine_type"),
            "countries_with_fresh_data": d.get("countries_with_fresh_data"),
            "countries_total": d.get("countries_total"),
            "global_phase": (d.get("aggregate") or {}).get("global_phase"),
            "global_avg_cli": (d.get("aggregate") or {}).get("global_avg_cli"),
        }
    except Exception as e:
        out["s3_err"] = str(e)[:200]

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
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
