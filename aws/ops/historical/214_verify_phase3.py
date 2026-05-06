#!/usr/bin/env python3
"""Step 214 — verify Phase 3 frontend cutover.

Phase 3 changed 13 read sites from khalid_<x> to (ka_<x> ?? khalid_<x>).
Verify by:
  A. Each of the 7 modified pages is still served (no syntax errors broke
     deployment)
  B. The S3 data sources they read have the ka_* keys present (was already
     verified in step 212/213, but re-confirm freshness)
  C. Sanity check that pages contain the new ?? fallback pattern
"""
import io, json, time, urllib.request, zipfile
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-214"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
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


PAGES = ["index.html", "intelligence.html", "desk.html", "desk-v2.html",
         "investor.html", "reports.html", "euro/index.html"]


with report("verify_phase3_frontend_cutover") as r:
    r.heading("Verify Phase 3 — frontend reads ka_* with khalid_* fallback")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30, MemorySize=512, Architectures=["x86_64"],
    )
    time.sleep(3)

    # ── A. Pages still serve + contain new fallback pattern ────────────
    r.section("A. Each modified page still serves with ?? fallback present")
    n_ok = 0
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
        # Look for at least one ?? fallback referencing ka_
        has_fallback = ("ka_index ?? " in body) or ("ka_strategy ?? " in body) \
                       or ("ka_score ?? " in body) or ("ka_timeline ?? " in body)
        if has_fallback:
            r.log(f"  ✅ {page} ({result['len']}B) — ?? fallback present")
            n_ok += 1
        else:
            r.warn(f"  ⚠ {page} ({result['len']}B) — no ka_* ?? fallback found, CDN may still be stale")

    r.log(f"\n  {n_ok}/{len(PAGES)} pages have new fallback live")

    # ── B. S3 data sources have ka_* keys ─────────────────────────────
    r.section("B. S3 data sources have ka_* keys present")
    sources = [
        ("intelligence-report.json", ["ka_index"]),
        ("data/report.json",          ["ka_index"]),
        ("portfolio/pnl-daily.json",  ["ka_strategy"]),
        ("crypto-intel.json",         ["ka_index"]),
    ]
    n_data_ok = 0
    for key, expected in sources:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            content = obj["Body"].read()
            data = json.loads(content)

            def find_keys(o, prefix, _seen=None, _depth=0):
                if _seen is None: _seen = set()
                if _depth > 12: return _seen
                if isinstance(o, dict):
                    for k, v in o.items():
                        if isinstance(k, str) and k.startswith(prefix): _seen.add(k)
                        find_keys(v, prefix, _seen, _depth+1)
                elif isinstance(o, list):
                    for x in o: find_keys(x, prefix, _seen, _depth+1)
                return _seen

            ka_keys = find_keys(data, "ka_")
            missing = [k for k in expected if k not in ka_keys]
            if not missing:
                r.log(f"  ✅ {key}: {sorted(ka_keys)[:5]}")
                n_data_ok += 1
            else:
                r.warn(f"  ⚠ {key}: missing {missing}, found {sorted(ka_keys)}")
        except Exception as e:
            r.warn(f"  ✗ {key}: {e}")

    r.log(f"\n  {n_data_ok}/{len(sources)} S3 data sources have expected ka_* keys")

    r.section("FINAL")
    r.log(f"  Frontend pages: {n_ok}/{len(PAGES)}  S3 data: {n_data_ok}/{len(sources)}")
    if n_ok == len(PAGES) and n_data_ok == len(sources):
        r.log(f"  🎉 Phase 3 fully live — frontend reads from ka_* with khalid_* fallback,")
        r.log(f"  and dual-write data is flowing.")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
