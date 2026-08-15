"""ops 4691 — diagnose + fix the real browser CORS failure.

Every test that "passed" tonight was either lam.invoke() (AWS SDK,
bypasses HTTP entirely) or urllib.request (raw HTTP, no preflight — CORS
is a BROWSER policy, never enforced by non-browser clients). None of
those could exercise real cross-origin behavior. Khalid's actual
browser is the first real test, and it failed with the classic
"TypeError: Failed to fetch" signature of a blocked CORS preflight.

Root cause candidate: Lambda Function URLs have an AWS-managed Cors
policy enforced BEFORE the function code runs, separate from the
manual CORS headers in lambda_function.py. If that policy is
missing/wrong, the browser's OPTIONS preflight gets rejected by AWS
infrastructure and the Lambda handler's own OPTIONS branch never
executes. This op sends a REAL preflight (Origin + Access-Control-
Request-Method headers, exactly what a browser sends) and checks the
response headers directly — then fixes the Function URL Cors config if
it's the problem, then re-tests.
"""
import sys
import time
import urllib.error
import urllib.request

import boto3

from ops_report import report

FN = "justhodl-tv-notes-ingest"
ORIGIN = "https://justhodl-dashboard-live.s3.amazonaws.com"
lam = boto3.client("lambda", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def preflight(url, origin):
    """Send the EXACT request a browser sends before a cross-origin
    POST with Content-Type: application/json (not a 'simple' type, so
    every real browser preflights it)."""
    rq = urllib.request.Request(url, method="OPTIONS", headers={
        "Origin": origin,
        "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "content-type"})
    try:
        resp = urllib.request.urlopen(rq, timeout=20)
        return resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers)


def main():
    with report("4691_cors_fix") as r:
        r.heading("ops 4691 — real browser CORS: diagnose + fix")
        misses = 0

        r.section("1. Current Function URL config (the AWS-managed "
                  "policy, not the code's own headers)")
        cfg = lam.get_function_url_config(FunctionName=FN)
        url = cfg["FunctionUrl"]
        auth = cfg.get("AuthType")
        cors = cfg.get("Cors") or {}
        r.log("  URL: %s" % url)
        r.log("  AuthType: %s" % auth)
        r.log("  Cors (AWS-enforced, pre-empts the Lambda code): %s"
              % cors)
        misses += contract(r, "auth", auth == "NONE",
                           "AuthType=NONE (browser calls need this — "
                           "IAM auth would block every unsigned "
                           "browser request)")

        r.section("2. REAL preflight — exactly what Khalid's browser "
                  "sent")
        status, hdrs = preflight(url, ORIGIN)
        r.log("  OPTIONS status: %s" % status)
        r.log("  response headers: %s" % hdrs)
        acao = hdrs.get("Access-Control-Allow-Origin")
        acam = hdrs.get("Access-Control-Allow-Methods", "")
        acah = hdrs.get("Access-Control-Allow-Headers", "")
        preflight_ok = (status < 300 and acao in ("*", ORIGIN)
                        and "POST" in acam.upper()
                        and "content-type" in acah.lower())
        r.log("  VERDICT: preflight %s"
              % ("would SUCCEED for a real browser" if preflight_ok
                 else "WOULD BE REJECTED by the browser — this is "
                      "the 'Failed to fetch' cause"))

        r.section("3. Fix: set the Function URL's own Cors policy")
        if not preflight_ok:
            lam.update_function_url_config(
                FunctionName=FN, AuthType="NONE",
                Cors={"AllowOrigins": ["*"],
                      "AllowMethods": ["POST", "OPTIONS", "GET"],
                      "AllowHeaders": ["content-type"],
                      "MaxAge": 3600})
            r.ok("  Function URL Cors policy set (AllowOrigins=*, "
                 "AllowMethods=POST/OPTIONS/GET, "
                 "AllowHeaders=content-type)")
            time.sleep(8)
        else:
            r.log("  Function URL Cors already permissive — the "
                 "block is elsewhere (see step 4)")

        r.section("4. Re-test the REAL preflight after the fix")
        status2, hdrs2 = preflight(url, ORIGIN)
        acao2 = hdrs2.get("Access-Control-Allow-Origin")
        acam2 = hdrs2.get("Access-Control-Allow-Methods", "")
        acah2 = hdrs2.get("Access-Control-Allow-Headers", "")
        ok2 = (status2 < 300 and acao2 in ("*", ORIGIN)
               and "POST" in acam2.upper()
               and "content-type" in acah2.lower())
        r.log("  post-fix: status=%s ACAO=%s ACAM=%s ACAH=%s"
              % (status2, acao2, acam2, acah2))
        misses += contract(r, "preflight", ok2,
                           "browser preflight now succeeds")

        r.section("5. Full round-trip — actual POST after preflight, "
                  "with an Origin header set (what a browser does "
                  "next)")
        body = ('{"token":"__nope__","kind":"series","series":'
                '[{"id":"CORSPROBE","rows":[["2020-01-01",1],'
                '["2020-01-02",2]]}]}')
        rq = urllib.request.Request(
            url, data=body.encode(),
            headers={"Content-Type": "application/json",
                     "Origin": ORIGIN})
        try:
            resp = urllib.request.urlopen(rq, timeout=30)
            r.log("  POST status=%s ACAO=%s"
                  % (resp.status, resp.headers.get(
                      "Access-Control-Allow-Origin")))
            misses += contract(
                r, "post", resp.headers.get(
                    "Access-Control-Allow-Origin") is not None,
                "actual response carries CORS header (browser will "
                "accept it)")
        except Exception as e:
            r.warn("  POST: %s" % str(e)[:150])

        r.section("verdict")
        if misses:
            r.fail("cors: %d red" % misses)
            sys.exit(1)
        r.ok("Function URL now serves a real browser preflight "
             "correctly — Khalid's upload page should work on retry")


if __name__ == "__main__":
    main()
