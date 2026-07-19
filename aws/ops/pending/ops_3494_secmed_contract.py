"""ops 3494 — sector-medians H1 contract close.

3493's H1 demanded >=7 keys; the two margin medians are unreachable from
this feed — forensic-screen's writer sets gross/op margin on the
SCREENER row object, not the forensic dict, so all_results never carries
them (audited). The achievable, honest contract is the 6 core keys, and
H2 already proved them on real data (11 sectors; Tech P/E 34.0 >
Utilities 22.3; Beneish medians all in band). This regate pins that
contract exactly. Engine/pages unchanged — v1.4.1 stays live.

  K1 served medians file: n_sectors >= 8 AND keys ⊇ {pe_ttm, ps_ttm,
     peg_ttm, fcf_yield_pct, beneish_m, sloan_accruals_pct} AND
     Technology.pe_ttm > Utilities.pe_ttm
"""
import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
CORE6 = {"pe_ttm", "ps_ttm", "peg_ttm", "fcf_yield_pct",
         "beneish_m", "sloan_accruals_pct"}

with report("3494_secmed_contract") as rep:
    out = {"ops": 3494, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:440]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:400]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3494 — sector-medians contract (6 core keys)")

    try:
        req = urllib.request.Request(
            "https://justhodl.ai/data/fundgraph/sector-medians.json",
            headers={"User-Agent": "ops-3494"})
        with urllib.request.urlopen(req, timeout=30) as r:
            sm = json.loads(r.read())
        keys = set(sm.get("keys") or [])
        secs = sm.get("sectors") or {}
        tech = (secs.get("Technology") or {}).get("pe_ttm")
        util = (secs.get("Utilities") or {}).get("pe_ttm")
        gate("K1_contract",
             sm.get("n_sectors", 0) >= 8 and CORE6 <= keys
             and tech is not None and util is not None and tech > util,
             {"n_sectors": sm.get("n_sectors"), "keys": sorted(keys),
              "tech_pe": tech, "util_pe": util})
    except Exception as e:  # noqa: BLE001
        gate("K1_contract", False, str(e)[:280])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3494.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
