"""ops 3470 — Fundamental Graphs engine EMBEDDED into why.html (ADDITIVE).

Replaces the ops-3467 tail link with a full null-safe analysis module that
injects section #jhFundGraphs directly under #jhVitalsTop for the viewed
ticker: 9-chip forensic vitals strip (ROIC / FCF yield / EV/EBITDA /
Rev YoY / Net-BB / ND/EBITDA / Altman / Piotroski / Beneish, green-amber-
red thresholds), 6 lens-switchable 10-year mini-charts (% Chg / YoY, 5Y/
10Y), legend with latest + pp delta, and a state-carrying deep link into
the full comparator. Zero existing why.html content touched.

Known-issue queued (non-blocking): engine search "micro" name-intent still
misses MSFT on FMP /stable name search; ticker-intent queries rank
correctly (apple->AAPL #1, tesla->TSLA #1).

Gates:
  E1  why.html live: fgwhy-3470 + jhFundGraphs markers AND jhVitalsTop +
      jhDollarFlows intact (additive proof); old fg-xlink-js gone
  E2  engine regression: ?symbol=CHTR quarter ok, marker, >=190 keys
"""
import gzip
import io
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
lam = boto3.client("lambda", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3470",
                                               "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=45) as r:
        raw = r.read()
        if (r.headers.get("Content-Encoding") or "").lower() == "gzip":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return r.status, raw


with report("3470_why_fundgraph_module") as rep:
    out = {"ops": 3470, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3470 — why.html x Fundamental Graphs embedded module")

    ok1, det = False, {}
    for _ in range(21):
        try:
            st, b = fetch(f"https://justhodl.ai/why.html?cb={int(time.time())}")
            ok1 = (b"fgwhy-3470" in b and b"jhFundGraphs" in b
                   and b"jhVitalsTop" in b and b"jhDollarFlows" in b
                   and b"fg-xlink-js" not in b)
            det = {"status": st, "module": b"fgwhy-3470" in b,
                   "vitals_intact": b"jhVitalsTop" in b,
                   "dollarflows_intact": b"jhDollarFlows" in b,
                   "old_block_gone": b"fg-xlink-js" not in b}
        except Exception as e:  # noqa: BLE001
            det = {"err": str(e)[:120]}
        if ok1:
            break
        time.sleep(20)
    gate("E1_why_module_live_additive", ok1, det)

    try:
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        st, raw = fetch(f"{url}/?symbol=CHTR&period=quarter")
        d = json.loads(raw)
        gate("E2_engine_regression",
             st == 200 and d.get("ok") and d.get("marker") == "FUNDGRAPH_V1_OPS3462"
             and len(d.get("points", {})) >= 190,
             {"keys": len(d.get("points", {})), "version": d.get("version"),
              "cached": d.get("cached")})
    except Exception as e:  # noqa: BLE001
        gate("E2_engine_regression", False, str(e)[:220])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3470.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
