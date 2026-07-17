"""ops 3373 — hot-money 3372 harness re-gate (probe bugs, not product bugs).

3372 substance PASSED (19 drilled countries; core-4 Asia + 8 Europe populated;
15/15 momentum). Two probe defects re-gated here:
  • G1: req() appended a ?t= cache-buster to the PRESIGNED Lambda
    Code.Location URL — query mutation breaks the S3 signature → 403 →
    marker check false-negative. Fetch presigned URLs verbatim.
  • G6: page badge is written as the JS-escaped literal \\ud83c\\udfaf inside
    the template string — served bytes contain the ESCAPE SEQUENCE, not
    UTF-8 emoji bytes. Match the escaped form.
Doctrine: presigned URLs are immutable; grep pages for the bytes as
authored, not as rendered.
"""

import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3

from ops_report import report

FN = "justhodl-hot-money"
PAGE = "https://justhodl.ai/hot-money.html"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3373"}


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:300]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:250]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    lam = boto3.client("lambda", "us-east-1")
    info = lam.get_function(FunctionName=FN)
    loc = info["Code"]["Location"]
    with urllib.request.urlopen(urllib.request.Request(loc, headers=UA), timeout=60) as r:
        zbytes = r.read()
    src = zipfile.ZipFile(io.BytesIO(zbytes)).read("lambda_function.py").decode("utf-8", "replace")
    gate("G1_deployed_zip_v140",
         'VERSION = "1.4.0"' in src and "def _drill_one" in src and "FOCUS = [" in src,
         f"zip {len(zbytes)} bytes, markers present")

    ok6, st = False, -1
    deadline = time.time() + 180
    while time.time() < deadline:
        rq = urllib.request.Request(PAGE + f"?t={int(time.time())}", headers=UA)
        try:
            with urllib.request.urlopen(rq, timeout=25) as r:
                st, body = r.status, r.read()
        except Exception as e:  # noqa: BLE001
            st, body = -1, str(e).encode()
        if st == 200 and rb"\ud83c\udfaf focus" in body and b"standing focus" in body:
            ok6 = True
            break
        time.sleep(12)
    gate("G6_page_focus_markers", ok6, f"http {st} (escaped-literal match)")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3373.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3373_hot_money_regate") as _rep:
    _rep.heading("ops 3373 — hot-money harness re-gate")
    main(_rep)
