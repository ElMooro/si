#!/usr/bin/env python3
"""Step 207 — verify cleanup is finally live on GitHub Pages."""
import io, json, time, zipfile
from ops_report import report
import boto3
from botocore.exceptions import ClientError

PROBE_NAME = "justhodl-tmp-probe-208"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], headers=event.get("headers", {}))
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"ok": True, "status": r.status, "len": len(r.read())}
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


with report("verify_cleanup_live_v2") as r:
    r.heading("Final cleanup verification — should ALL match now")

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
        # Stubs — should 404
        ("Reports.html",                                            404),
        ("ml.html",                                                 404),
        ("stocks.html",                                             404),
        # Archived — should serve from /archive/
        ("archive/pro.html",                                        200),
        ("archive/exponential-search-dashboard.html",               200),
        ("archive/macroeconomic-platform.html",                     200),
        ("archive/README.md",                                       200),
        # Old paths should now also 404 (they got moved)
        ("pro.html",                                                404),
        ("exponential-search-dashboard.html",                       404),
        ("macroeconomic-platform.html",                             404),
        # Working pages — should still 200
        ("repo.html",                                               200),
        ("volatility.html",                                         200),
        ("system.html",                                             200),
        ("investor.html",                                           200),
        ("dxy.html",                                                200),
        ("bonds.html",                                              200),
        ("macro-data.html",                                         200),
        ("sentiment.html",                                          200),
        ("index.html",                                              200),
    ]

    n_correct = 0
    for path, expected in paths:
        url = f"https://justhodl.ai/{path}"
        resp = lam.invoke(
            FunctionName=PROBE_NAME, InvocationType="RequestResponse",
            Payload=json.dumps({
                "url": url,
                "headers": {"User-Agent": UA, "Cache-Control": "no-cache"},
            }),
        )
        result = json.loads(resp["Payload"].read())
        actual = result.get("status")
        match = (actual == expected)
        if match: n_correct += 1
        mark = "✅" if match else "❌"
        r.log(f"  {mark} expected={expected:3} got={actual!s:3}  {path:55}")

    r.section("FINAL")
    r.log(f"\n  {n_correct}/{len(paths)} paths match expected status")
    if n_correct == len(paths):
        r.log(f"  🎉 100% — cleanup fully deployed and verified.")
    else:
        r.log(f"  GitHub Pages may still be deploying. Re-run in ~3 min.")

    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
