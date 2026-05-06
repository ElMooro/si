#!/usr/bin/env python3
"""Step 203 — verify GitHub Pages serves all 6 new pages with browser UA."""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-203"
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
                    "preview": data[:500].decode("utf-8", errors="replace")}
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


PAGES = ["volatility.html", "dxy.html", "bonds.html", "macro-data.html",
         "sentiment.html", "repo.html", "system.html", "investor.html"]


with report("verify_new_pages_live") as r:
    r.heading("Verify new pages on GitHub Pages")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    results = {}
    for page in PAGES:
        url = f"https://justhodl.ai/{page}"
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({"url": url, "headers": {"User-Agent": UA}}),
        )
        result = json.loads(resp["Payload"].read())
        if result.get("ok"):
            r.log(f"  ✅ {page:25} HTTP {result['status']} len={result['len']}B")
            # Spot-check: does it have the JustHodl brand?
            preview = result.get("preview", "")
            has_brand = "JUSTHODL" in preview.upper() or "justhodl" in preview.lower()
            r.log(f"     has JustHodl brand: {has_brand}")
            results[page] = "OK"
        else:
            r.warn(f"  ✗ {page:25} status={result.get('status')} err={result.get('error','')}")
            results[page] = f"FAIL-{result.get('status','?')}"

    # Also sanity-check index.html
    r.section("Index.html sanity")
    resp = lam.invoke(
        FunctionName=PROBE_NAME, InvocationType="RequestResponse",
        Payload=json.dumps({"url": "https://justhodl.ai/", "headers": {"User-Agent": UA}}),
    )
    result = json.loads(resp["Payload"].read())
    if result.get("ok"):
        r.log(f"  index.html HTTP {result['status']} len={result['len']}B")

    r.section("Summary")
    n_ok = sum(1 for v in results.values() if v == "OK")
    r.log(f"  {n_ok}/{len(PAGES)} pages live and serving")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
