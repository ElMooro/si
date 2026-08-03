"""ops_4315 -- runtime-order seal from the runner (sandbox egress
can't reach the edge; runner is the verifier of record). Asserts on
the LIVE page: no `--1` decrement typo; `const HMODE` declaration
precedes every renderHeat call site (TDZ); page full-size."""
import sys, time, urllib.request
from ops_report import report
fails = []
with report("4315_runtime_seal") as r:
    r.heading("ops 4315 -- the wire, verified where it can be")
    body = ""
    for _ in range(14):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/trend-reversal.html",
                headers={"User-Agent": "ops/4315",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "initial paint" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    r.log("fetched %d bytes" % len(body))
    if len(body) < 20000:
        fails.append("page too small: %d" % len(body))
    if "--1" in body:
        fails.append("`--1` typo still on wire")
    i = body.find("const HMODE")
    calls = [k for k in range(len(body))
             if body.startswith("renderHeat(rows)", k)]
    r.log("const HMODE @ %d · renderHeat sites @ %s" % (i, calls))
    if i <= 0 or not calls or any(k < i for k in calls):
        fails.append("TDZ order wrong: decl=%d calls=%s" % (i, calls))
    else:
        r.ok("declaration precedes all %d call sites -- TDZ dead "
             "on the wire" % len(calls))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4315 PASS -- heatmap + grid render path clean")
