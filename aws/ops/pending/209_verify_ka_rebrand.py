#!/usr/bin/env python3
"""Step 209 — verify Phase 1 KA rebrand is live on GitHub Pages."""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-209"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], headers=event.get("headers", {}))
        with urllib.request.urlopen(req, timeout=15) as r:
            data = r.read()
            return {"ok": True, "status": r.status, "len": len(data),
                    "body": data.decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code}
    except Exception as e:
        return {"ok": False, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


with report("verify_ka_rebrand_live") as r:
    r.heading("Verify KA rebrand on live pages")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)

    PAGES = [
        "index.html", "intelligence.html", "desk.html", "desk-v2.html",
        "investor.html", "reports.html", "bot/index.html",
        "euro/index.html", "downloads.html",
    ]

    # Check each page contains 'KA Index' or 'KA ' but no 'Khalid Index' display strings
    OK = 0
    for page in PAGES:
        url = f"https://justhodl.ai/{page}"
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({"url": url, "headers": {"User-Agent": UA, "Cache-Control": "no-cache"}}),
        )
        result = json.loads(resp["Payload"].read())
        if not result.get("ok"):
            r.warn(f"  ✗ {page} HTTP {result.get('status', '?')}")
            continue
        body = result.get("body", "")

        # Display strings that should be GONE
        bad_patterns = [
            "Khalid Index", "KHALID INDEX",
            "Khalid Metrics", "KHALID METRICS",
            "Khalid Timeline",
            "Built by Khalid",
            "Khalid Afouis",
        ]
        bad_found = [p for p in bad_patterns if p in body]

        # Display strings that should now exist (at least one on relevant pages)
        good_patterns = ["KA Index", "KA INDEX", "KA Metrics", "KA Strategy"]
        good_found = [p for p in good_patterns if p in body]

        if bad_found:
            r.warn(f"  ✗ {page} still has display strings: {bad_found}")
        else:
            mark = "✅" if good_found else "🟡"
            note = f"has new: {good_found[:2]}" if good_found else "(no KA strings — page may not display KA)"
            r.log(f"  {mark} {page} — {note}")
            OK += 1

    r.section("Summary")
    r.log(f"\n  {OK}/{len(PAGES)} pages clean (no old display strings)")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
