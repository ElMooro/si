#!/usr/bin/env python3
"""Step 206 — verify GitHub Pages CDN refreshed (stubs should now 404, archive should serve)."""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-206"
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
                    "preview_first_300": data[:300].decode("utf-8", errors="replace")}
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


with report("verify_cdn_refresh") as r:
    r.heading("Verify GitHub Pages CDN refresh")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=20, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    paths = [
        # Should be GONE (404 expected)
        ("Reports.html",                       404),
        ("ml.html",                            404),
        ("stocks.html",                        404),
        # Should serve from /archive/
        ("archive/pro.html",                   200),
        ("archive/exponential-search-dashboard.html", 200),
        ("archive/macroeconomic-platform.html", 200),
        ("archive/README.md",                  200),
        # Should serve normally
        ("repo.html",                          200),
        ("volatility.html",                    200),
        ("system.html",                        200),
        ("investor.html",                      200),
        ("dxy.html",                           200),
        ("bonds.html",                         200),
        ("macro-data.html",                    200),
        ("sentiment.html",                     200),
        # Index — should have all the new tiles
        ("index.html",                         200),
    ]

    n_correct = 0
    n_total = len(paths)
    for path, expected in paths:
        url = f"https://justhodl.ai/{path}"
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({"url": url, "headers": {"User-Agent": UA, "Cache-Control": "no-cache"}}),
        )
        result = json.loads(resp["Payload"].read())
        actual = result.get("status") if (result.get("ok") or "status" in result) else "ERR"
        match = (actual == expected)
        if match: n_correct += 1
        mark = "✅" if match else "❌"
        # If 200 expected and got 200, show byte size
        size_info = f" {result.get('len','?')}B" if (result.get("ok") and expected == 200) else ""
        r.log(f"  {mark} expected={expected:3} got={str(actual):3} {path:55}{size_info}")

    r.section("Summary")
    r.log(f"\n  {n_correct}/{n_total} paths match expected status")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
