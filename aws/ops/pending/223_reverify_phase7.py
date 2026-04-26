#!/usr/bin/env python3
"""Step 222 — verify Phase 7 didn't break anything.

Phase 7 renamed JS internals (renderKhalid → renderKa, stripKhalid →
stripKa, cardKhalid → cardKa, khalidBody → kaBody). These are
internal references — if any rename was inconsistent (e.g. element
ID renamed but getElementById still uses old name), the page will
silently fail to render that section.

Verify by:
  1. Each modified page still serves
  2. No occurrence of old JS names alongside new (e.g. "stripKhalid"
     should be 0 hits while "stripKa" should appear)
  3. Sanity: pages load + render KA branding
"""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-223"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
UA = "Mozilla/5.0 Chrome/124"

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], headers=event.get("headers", {}))
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"ok": True, "status": r.status, "len": len(r.read()) if False else None,
                    "body": (r.read()).decode("utf-8", errors="replace")}
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


# Pages modified in Phase 7 + the names that should be ABSENT (old) vs PRESENT (new)
TARGETS = [
    ("index.html",      ["renderKhalid", "khalidBody", "khalidSub"], ["renderKa", "kaBody", "kaSub"]),
    ("desk-v2.html",    ["stripKhalid", "cardKhalid", "renderKhalid"], ["stripKa", "cardKa", "renderKa"]),
    ("reports.html",    [], []),  # only comment changed
    ("bot/index.html",  [], ["/ka", "cmd-trigger"]),
]


with report("verify_phase7_internal_renames_v2") as r:
    r.heading("Phase 7 verify — JS internals renamed cleanly")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    n_clean = 0
    for page, should_be_absent, should_be_present in TARGETS:
        r.section(f"📄 {page}")
        url = f"https://justhodl.ai/{page}"
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({"url": url, "headers": {"User-Agent": UA, "Cache-Control": "no-cache"}}),
        )
        result = json.loads(resp["Payload"].read())
        if not result.get("ok"):
            r.warn(f"  ✗ HTTP {result.get('status', '?')}")
            continue

        body = result.get("body", "")
        page_clean = True

        # Check that old names are absent
        for old in should_be_absent:
            if old in body:
                r.warn(f"  ⚠ found old name '{old}' — CDN cache stale or rename incomplete")
                page_clean = False
            else:
                r.log(f"  ✅ old '{old}' absent")

        # Check that new names are present
        for new in should_be_present:
            if new in body:
                r.log(f"  ✅ new '{new}' present")
            else:
                r.warn(f"  ⚠ new '{new}' not found — CDN may not have caught up")
                page_clean = False

        if page_clean:
            n_clean += 1

    r.section("FINAL")
    r.log(f"  {n_clean}/{len(TARGETS)} pages cleanly renamed")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
