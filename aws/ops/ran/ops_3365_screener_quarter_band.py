#!/usr/bin/env python3
"""ops 3365 — screener 13F-quarter probe hardening (Khalid-approved 2026-07-17).

Change (9 lines, probe only): AAPL probe now requires investorsHolding>3000
(fully-loaded quarter) instead of any non-empty response. Everything else in
the PROTECTED screener is untouched: no URL, schedule, env, or logic changes.

Gates:
  G1  pre-deploy diff sanity: deployed zip lacks band, local has it
  G2  deploy justhodl-stock-screener (env/timeout/memory passthrough,
      create_function_url=False, no EventBridge changes, no invoke)
  G3  deployed-zip markers settled
  G4  in-runner proof: band selects Q1 2026 (>3000 AAPL holders)
"""
import io, json, sys, time, urllib.error, urllib.request, zipfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

ROOT = Path(__file__).resolve().parents[2]
FN = "justhodl-stock-screener"
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BAND = '(test[0].get("investorsHolding") or 0) > 3000'
LAM = boto3.client("lambda", region_name="us-east-1")


def _zip_src():
    code = LAM.get_function(FunctionName=FN)["Code"]["Location"]
    z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(code, timeout=60).read()))
    return z.read("lambda_function.py").decode()


with report("3365_screener_quarter_band") as r:
    r.section("G1 — pre-deploy sanity")
    local = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
    if BAND not in local:
        r.fail("G1 local source missing band — aborting before touching protected screener")
        sys.exit(1)
    pre = _zip_src()
    r.log(f"[G1] deployed has band already: {BAND in pre} (expect False)")
    r.ok("G1 ✓ local carries the band; proceeding")

    r.section("G2 — deploy screener (config passthrough)")
    cfg = LAM.get_function_configuration(FunctionName=FN)
    env = cfg.get("Environment", {}).get("Variables", {})
    deploy_lambda(report=r, function_name=FN,
                  source_dir=ROOT / "lambdas" / FN / "source",
                  env_vars=env, timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                  description="Screener — 13F probe completeness band >3000 holders (ops 3365, Khalid-approved)",
                  create_function_url=False, smoke=False)

    r.section("G3 — deployed markers settled")
    ok = False
    for _ in range(12):
        c = LAM.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            src = _zip_src()
            if BAND in src and "incomplete" in src:
                ok = True
                break
        time.sleep(10)
    if not ok:
        r.fail("G3 markers missing in deployed zip")
        sys.exit(1)
    r.ok("G3 ✓ band deployed & settled")

    r.section("G4 — band selection proof")
    from datetime import datetime, timezone, date
    now = datetime.now(timezone.utc)
    qends = []
    for cy in (now.year, now.year - 1):
        for cq in (4, 3, 2, 1):
            m = cq * 3
            qends.append((cy, cq, date(cy, m, 30 if m in (6, 9) else 31)))
    pick = None
    for cy, cq, _qd in sorted([t for t in qends if (now.date() - t[2]).days >= 60],
                              key=lambda t: -t[2].toordinal())[:4]:
        u = ("https://financialmodelingprep.com/stable/institutional-ownership/"
             f"symbol-positions-summary?apikey={KEY}&symbol=AAPL&year={cy}&quarter={cq}")
        try:
            js = json.loads(urllib.request.urlopen(u, timeout=30).read().decode())
            if isinstance(js, list) and js and (js[0].get("investorsHolding") or 0) > 3000:
                pick = (cy, cq, js[0].get("investorsHolding"))
                break
        except Exception as e:
            r.log(f"  probe Q{cq} {cy}: {e!r}")
    if not pick:
        r.fail("G4 no quarter passes the band")
        sys.exit(1)
    r.log(f"[G4] band → Q{pick[1]} {pick[0]} ({pick[2]:,} AAPL holders)")
    r.ok("G4 ✓ selects fully-filed quarter")

    Path(ROOT / "ops" / "reports" / "3365.json").write_text(json.dumps({
        "band_in_predeploy_zip": BAND in pre,
        "selected": f"Q{pick[1]} {pick[0]}", "holders": pick[2],
        "verdict": "PASS"}, indent=1))
    r.ok("VERDICT: PASS")
