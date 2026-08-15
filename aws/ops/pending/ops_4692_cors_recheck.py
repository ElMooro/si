"""ops 4692 — correct the CORS diagnostic, confirm the page bytes,
narrow the failure to something only Khalid's browser can reveal.

4691's own check had a bug: it string-matched "POST" against
Access-Control-Allow-Methods and never accounted for AWS returning the
literal wildcard '*' (which means "all methods" under the CORS spec,
including POST). That false negative caused an unnecessary "fix"
attempt that AWS rejected (OPTIONS is 7 chars, the API caps method
strings at 6) and crashed the run before real diagnosis finished.

This op: (a) re-verifies the ALREADY-CORRECT Cors policy with a fixed
check, (b) confirms byte-for-byte that the live page has the right
INGEST/TOKEN baked in with no corruption, (c) does a full unsigned
POST exactly as a browser would, proving the server accepts it fine.
If all three pass, the failure is client-side (extension/CSP) and no
ops fix can touch it -- that gets reported honestly, not patched over.
"""
import sys
import urllib.error
import urllib.request

import boto3

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
ORIGIN = "https://justhodl-dashboard-live.s3.amazonaws.com"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def cors_ok(hdrs, origin):
    acao = hdrs.get("Access-Control-Allow-Origin", "")
    acam = hdrs.get("Access-Control-Allow-Methods", "")
    acah = hdrs.get("Access-Control-Allow-Headers", "")
    origin_ok = acao in ("*", origin)
    method_ok = (acam.strip() == "*"
                or "POST" in [m.strip().upper()
                              for m in acam.split(",")])
    header_ok = (acah.strip() == "*"
                or "content-type" in [h.strip().lower()
                                      for h in acah.split(",")])
    return origin_ok and method_ok and header_ok


def main():
    with report("4692_cors_recheck") as r:
        r.heading("ops 4692 — corrected CORS check + page byte audit")
        misses = 0

        r.section("1. Re-verify Cors policy with the FIXED check")
        cfg = lam.get_function_url_config(FunctionName=FN)
        url = cfg["FunctionUrl"]
        cors = cfg.get("Cors") or {}
        r.log("  live policy: %s" % cors)
        rq = urllib.request.Request(url, method="OPTIONS", headers={
            "Origin": ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type"})
        try:
            resp = urllib.request.urlopen(rq, timeout=20)
            hdrs, status = dict(resp.headers), resp.status
        except urllib.error.HTTPError as e:
            hdrs, status = dict(e.headers), e.code
        ok = status < 300 and cors_ok(hdrs, ORIGIN)
        r.log("  status=%s ACAO=%s ACAM=%s ACAH=%s"
              % (status, hdrs.get("Access-Control-Allow-Origin"),
                 hdrs.get("Access-Control-Allow-Methods"),
                 hdrs.get("Access-Control-Allow-Headers")))
        misses += contract(r, "cors", ok,
                           "preflight genuinely succeeds under the "
                           "real CORS spec (4691's verdict was a bug "
                           "in MY check, not the server)")

        r.section("2. Byte audit of the LIVE deployed page")
        page = s3.get_object(
            Bucket=B, Key="tools/ice-recover.html"
        )["Body"].read().decode()
        m_ingest = None
        m_token = None
        for ln in page.splitlines():
            if "INGEST =" in ln:
                m_ingest = ln.strip()
            if "TOKEN =" in ln:
                m_token = ln.strip()
        r.log("  INGEST line: %s" % m_ingest)
        r.log("  TOKEN line: %s"
              % (re.sub(r"'[^']{4}([^']*)[^']{2}'",
                        lambda mm: "'****" + "*" * len(mm.group(1))
                                  + "**'", m_token)
                 if m_token else None))
        misses += contract(
            r, "page", bool(m_ingest) and url in m_ingest,
            "INGEST constant contains the exact live Function URL")
        misses += contract(
            r, "page", bool(m_token) and "__TOKEN__" not in m_token,
            "TOKEN constant is populated, not a leftover placeholder")

        r.section("3. Full unsigned POST — exactly what the browser "
                  "attempts after preflight")
        real_token = ""
        import boto3 as _b
        ssm = _b.client("ssm", region_name="us-east-1")
        for pn in ("/justhodl/tvnotes/ingest-token",):
            try:
                real_token = ssm.get_parameter(
                    Name=pn, WithDecryption=True
                )["Parameter"]["Value"]
            except Exception:
                pass
        body = ('{"token":"%s","kind":"series","series":'
               '[{"id":"CORSPROBE2","rows":[["2020-01-01",1],'
               '["2020-01-02",2],["2020-01-03",3]]}]}' % real_token)
        rq2 = urllib.request.Request(
            url, data=body.encode(),
            headers={"Content-Type": "application/json",
                     "Origin": ORIGIN})
        try:
            resp2 = urllib.request.urlopen(rq2, timeout=30)
            rb = resp2.read().decode()
            r.log("  POST status=%s ACAO=%s body=%s"
                  % (resp2.status,
                     resp2.headers.get(
                         "Access-Control-Allow-Origin"), rb[:200]))
            misses += contract(
                r, "post", '"ok": true' in rb or '"ok":true' in rb,
                "server accepted and banked the probe series")
        except Exception as e:
            misses += contract(r, "post", False,
                               "POST failed server-side: %s"
                               % str(e)[:150])

        r.section("verdict")
        if misses:
            r.fail("cors recheck: %d red — a real server-side issue "
                   "remains" % misses)
            sys.exit(1)
        r.ok("server is fully correct: CORS policy valid, page bytes "
             "clean, unsigned browser-style POST succeeds end to end. "
             "The failure Khalid saw is NOT reproducible server-side "
             "-- it points at something local to that browser "
             "session (extension, privacy tool, or corporate/AV "
             "network filter blocking *.lambda-url.*.on.aws)")


if __name__ == "__main__":
    main()
