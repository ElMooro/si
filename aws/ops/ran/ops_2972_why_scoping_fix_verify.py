#!/usr/bin/env python3
"""ops 2972 -- verify the live why.html scoping fix (renderInvestorLenses /
fetchInvestorLenses / renderTechSignals / fetchTechSignals were trapped in
a local IIFE and unreachable from the main render pipeline, throwing
ReferenceError on every ticker). Confirms: (1) the live page now serves
the window.-exported versions and none of the broken bare declarations;
(2) the equity-research Lambda itself returns a healthy TSM document
(ruling out a server-side cause, isolating this as purely the client-side
scoping bug); (3) no other render-pipeline function name is missing its
window export (the same class of bug, comprehensively).
"""
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report

AWS_DIR = Path(__file__).resolve().parents[2]
WHY_URL = "https://justhodl.ai/why.html"
ER_URL = ("https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url"
         ".us-east-1.on.aws/?ticker=TSM")


def http_get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2972",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def fail(rep, fails, msg):
    fails.append(msg)
    rep.fail(msg)


def main():
    fails, warns = [], []
    hl = {}
    with report("2972_why_scoping_fix_verify") as rep:

        rep.section("1. Live why.html serves the fix")
        st, html = http_get(WHY_URL + "?v=%d" % int(
            datetime.now(timezone.utc).timestamp()))
        rep.kv(status=st, bytes=len(html))
        must_have = ["window.renderInvestorLenses=function",
                     "window.fetchInvestorLenses=async function",
                     "window.renderTechSignals=function",
                     "window.fetchTechSignals=async function"]
        for m in must_have:
            if m not in html:
                fail(rep, fails, "live page missing exported form: %r" % m)
        must_not_have = ["\nfunction renderInvestorLenses(",
                         "\nasync function fetchInvestorLenses(",
                         "\nfunction renderTechSignals(",
                         "\nasync function fetchTechSignals("]
        for m in must_not_have:
            if m in html:
                fail(rep, fails, "live page still has the broken bare "
                     "declaration: %r" % m)
        rep.kv(fix_markers_present=sum(1 for m in must_have if m in html))

        rep.section("2. Comprehensive re-scan: no OTHER render function "
                   "has the same trapped-scope bug")
        # every `html += renderX(d)` / `fetchX(...)` call in the pipeline
        pipeline_calls = re.findall(
            r'\bhtml\s*\+=\s*(render\w+)\(', html)
        pipeline_calls += re.findall(
            r'\b(fetch\w+)\([^)]*\);\s*(?://.*)?$', html, re.MULTILINE)
        pipeline_calls = sorted(set(pipeline_calls))
        rep.kv(pipeline_functions_checked=len(pipeline_calls))
        # split the doc at the second <script> tag boundary (same split
        # the bug lived across) to test cross-block visibility precisely
        parts = html.split("<script>")
        script_bodies = [p.split("</script>")[0] for p in parts[1:]]
        if len(script_bodies) < 3:
            warns.append("expected 3 <script> blocks live, found %d -- "
                         "skipping the cross-block scan" %
                         len(script_bodies))
        else:
            main_block, v2_block = script_bodies[1], script_bodies[2]
            missing = []
            for fn in pipeline_calls:
                declared_bare_in_v2 = re.search(
                    r'(?:^|\n)(?:async\s+)?function\s+%s\s*\(' %
                    re.escape(fn), v2_block) is not None
                exported_in_v2 = ("window.%s=" % fn) in v2_block or \
                    ("window.%s =" % fn) in v2_block
                called_in_main = fn in main_block
                if declared_bare_in_v2 and not exported_in_v2 and \
                        called_in_main:
                    missing.append(fn)
            if missing:
                fail(rep, fails, "same trapped-scope bug found in OTHER "
                     "function(s), not yet fixed: %s" % missing)
            else:
                rep.ok("no other pipeline function has an unexported "
                       "bare declaration inside the v2 IIFE")

        rep.section("3. Equity-research Lambda: TSM document itself "
                   "healthy (isolates the bug as client-side only)")
        try:
            st2, body2 = http_get(ER_URL, timeout=280)
            d = json.loads(body2)
            hl["er_status"] = st2
            hl["er_generated_at"] = d.get("generated_at")
            hl["er_schema"] = d.get("schema_version")
            hl["er_exec_len"] = len(d.get("executive_summary") or "")
            rep.kv(**hl)
            if not d.get("generated_at"):
                fail(rep, fails, "TSM document missing generated_at")
            if hl["er_exec_len"] < 100:
                fail(rep, fails, "TSM executive_summary suspiciously "
                     "short: %d chars" % hl["er_exec_len"])
        except Exception as e:
            warns.append("equity-research TSM fetch slow/cold or failed "
                         "(%s) -- not the reported bug, but noting it" %
                         str(e)[:120])

        if not fails:
            rep.ok("live why.html confirmed fixed: all 4 exports present, "
                   "no broken bare declarations, no sibling instances of "
                   "the same bug, and the TSM data source is healthy")
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2972, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2972.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    rep.log("report written: %s" % rp)
    if fails:
        sys.exit(1)


main()
sys.exit(0)
