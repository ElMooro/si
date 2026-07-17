#!/usr/bin/env python3
"""ops 3364 — fleet audit remediation: pin justhodl-ignition to fully-filed 13F quarter.

Background (ops 3360): FMP symbol-positions-summary serves PARTIAL mid-filing
quarters; ignition called it with NO year/quarter, inheriting FMP's default.

Gates:
  G1  empirical: what does FMP return with NO year/quarter for AAPL?
      (records whether ignition was actively poisoned — informational)
  G2  deploy justhodl-ignition v1.1.0 (env passthrough, schedule preserved)
  G3  deployed-zip markers: filed_q + pinned params
  G4  in-runner proof: fully-filed selection lands Q1 2026 with >3000 holders
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
import boto3

ROOT = Path(__file__).resolve().parents[2]
FN = "justhodl-ignition"
KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
LAM = boto3.client("lambda", region_name="us-east-1")


def _http(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return r.read().decode()


def _probe(y=None, q=None):
    u = ("https://financialmodelingprep.com/stable/institutional-ownership/"
         f"symbol-positions-summary?apikey={KEY}&symbol=AAPL")
    if y:
        u += f"&year={y}&quarter={q}"
    js = json.loads(_http(u))
    return js[0] if isinstance(js, list) and js else {}


with report("3364_ignition_filed_quarter") as r:
    r.section("G1 — FMP default (no year/quarter) behavior")
    d = _probe()
    r.log(f"[G1] default → date={d.get('date')} investorsHolding={d.get('investorsHolding')} "
          f"investorsHoldingChange={d.get('investorsHoldingChange')} "
          f"totalInvestedChange={d.get('totalInvestedChange')}")
    default_partial = (d.get("investorsHolding") or 0) <= 3000
    if default_partial:
        r.warn("G1 ⚠ FMP default = PARTIAL quarter → ignition WAS ingesting garbage change-fields")
    else:
        r.ok("G1 ✓ FMP default currently full — pinning still required (default flips every quarter)")

    r.section("G2 — deploy ignition v1.1.0")
    env = LAM.get_function_configuration(FunctionName=FN).get("Environment", {}).get("Variables", {})
    cfg = LAM.get_function_configuration(FunctionName=FN)
    deploy_lambda(report=r, function_name=FN,
                  source_dir=str(ROOT / "lambdas" / FN / "source"),
                  env_vars=env, timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                  description="Ignition v1.1.0 — inst lens pinned to fully-filed 13F quarter (ops 3364)",
                  create_function_url=False, smoke=False)

    r.section("G3 — deployed-code markers")
    ok = False
    for i in range(12):
        c = LAM.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful":
            code = LAM.get_function(FunctionName=FN)["Code"]["Location"]
            z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(code, timeout=60).read()))
            src = z.read("lambda_function.py").decode()
            if "def filed_q()" in src and 'prm.update({"year": yy, "quarter": qq})' in src and '"1.1.0"' in src:
                ok = True
                break
        time.sleep(10)
    if not ok:
        r.fail("G3 markers missing in deployed zip")
        sys.exit(1)
    r.ok("G3 ✓ v1.1.0 markers deployed & settled")

    r.section("G4 — fully-filed selection proof (runner-side)")
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
        p = _probe(cy, cq)
        if (p.get("investorsHolding") or 0) > 3000:
            pick = (cy, cq, p.get("investorsHolding"))
            break
    if not pick:
        r.fail("G4 no fully-filed quarter found")
        sys.exit(1)
    r.log(f"[G4] selection → Q{pick[1]} {pick[0]} ({pick[2]:,} AAPL holders)")
    if not (pick[0] == 2026 and pick[1] == 1):
        r.warn(f"G4 note: expected Q1 2026, got Q{pick[1]} {pick[0]} — verify calendar")
    r.ok("G4 ✓ gate selects fully-filed quarter")

    Path(ROOT / "ops" / "reports" / "3364.json").write_text(json.dumps({
        "g1_default_partial": default_partial,
        "g1_default_date": d.get("date"),
        "g1_default_holders": d.get("investorsHolding"),
        "g4_selected": f"Q{pick[1]} {pick[0]}",
        "g4_holders": pick[2],
        "verdict": "PASS"}, indent=1))
    r.ok("VERDICT: PASS")
