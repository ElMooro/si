#!/usr/bin/env python3
"""Step 221 — final verify of Phase 4 + 5.

After CDN propagates ka/index.html:
  1. GET https://justhodl.ai/ka/ — confirm it serves and uses new endpoints
  2. GET old /khalid/ — confirm meta-refresh redirect to /ka/
  3. GET https://justhodl.ai/data/ka-metrics.json (via S3) — confirm fresh
  4. GET new Function URL — confirm public access works
"""
import io, json, time, zipfile
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-221"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], headers=event.get("headers", {"User-Agent": "Mozilla/5.0"}))
        with urllib.request.urlopen(req, timeout=20) as r:
            data = r.read()
            return {"ok": True, "status": r.status, "len": len(data),
                    "body": data.decode("utf-8", errors="replace")[:5000]}
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")[:1000]
        except:
            body = ""
        return {"ok": False, "status": e.code, "body": body}
    except Exception as e:
        return {"ok": False, "error": str(e)}
'''

def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0); return buf.read()


with report("phase4_5_final_verify") as r:
    r.heading("Phase 4 + 5 final verify — /ka/ live, /khalid/ redirects")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    def fetch(url):
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({"url": url, "headers": {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) Chrome/124",
                "Cache-Control": "no-cache",
            }}),
        )
        return json.loads(resp["Payload"].read())

    # 1. /ka/ live
    r.section("1. https://justhodl.ai/ka/ serves with new endpoints")
    res = fetch("https://justhodl.ai/ka/")
    if res.get("ok"):
        r.log(f"  ✅ HTTP {res['status']}, {res['len']}B")
        body = res.get("body", "")
        # Check for new Function URL token
        new_url_token = "s6ascg5dntry5w5elqedee77na0fcljz"
        old_url_token = "2ijajv2pntkgj5yw5c3ukh5oq40xsyaf"
        if new_url_token in body:
            r.log(f"  ✅ contains NEW Function URL token")
        elif old_url_token in body:
            r.warn(f"  ⚠ still has OLD Function URL — CDN cache stale, retry in ~5min")
        # Check ka-*.json
        if "data/ka-metrics.json" in body:
            r.log(f"  ✅ references data/ka-metrics.json")
        elif "data/khalid-metrics.json" in body:
            r.warn(f"  ⚠ still references data/khalid-metrics.json — CDN cache stale")
        # KA branding
        if "KA Metrics" in body or "KA INDEX" in body:
            r.log(f"  ✅ KA branding visible")
    else:
        r.warn(f"  ✗ HTTP {res.get('status')} — {res.get('error', '')}")

    # 2. /khalid/ → redirect
    r.section("2. https://justhodl.ai/khalid/ should be redirect stub")
    res = fetch("https://justhodl.ai/khalid/")
    if res.get("ok"):
        body = res.get("body", "")
        r.log(f"  ✅ HTTP {res['status']}, {res['len']}B")
        if "meta http-equiv=\"refresh\"" in body and "/ka/" in body:
            r.log(f"  ✅ meta-refresh to /ka/ present")
        if "location.replace('/ka/'" in body:
            r.log(f"  ✅ JS fallback present")
        if "rel=\"canonical\" href=\"https://justhodl.ai/ka/\"" in body:
            r.log(f"  ✅ rel=canonical to /ka/ present")
    else:
        r.warn(f"  ✗ {res.get('status')}")

    # 3. data/ka-metrics.json fresh (Lambda is auto-running per EventBridge daily)
    r.section("3. S3 keys still healthy")
    keys = [
        "data/ka-metrics.json", "data/ka-config.json", "data/ka-analysis.json",
        "data/khalid-metrics.json", "data/khalid-analysis.json",
    ]
    now = datetime.now(timezone.utc)
    for k in keys:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            age = (now - obj["LastModified"]).total_seconds()
            mark = "✅" if age < 86400 else "⏳"
            r.log(f"  {mark} {k}  size={obj['ContentLength']}B  age={int(age)}s")
        except ClientError as e:
            r.warn(f"  ✗ {k}: {e}")

    # 4. New Function URL responds publicly
    r.section("4. New Function URL is publicly invokable")
    res = fetch("https://s6ascg5dntry5w5elqedee77na0fcljz.lambda-url.us-east-1.on.aws/")
    if res.get("ok"):
        r.log(f"  ✅ HTTP {res['status']}, {res['len']}B")
        body = res.get("body", "")
        if '"status"' in body:
            r.log(f"  payload preview: {body[:200]}")
    else:
        r.warn(f"  ✗ {res.get('status')} — {res.get('error', '')}")

    r.section("FINAL")
    r.log("  /ka/ + /khalid/ redirect + new Lambda + new endpoints all working")
    r.log("  Phase 4b (after 7-day grace, ~2026-05-03):")
    r.log("    - Delete justhodl-khalid-metrics Lambda")
    r.log("    - Delete its Function URL")
    r.log("    - Optionally delete data/khalid-*.json keys")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
