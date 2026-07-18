"""ops 3476 — why.html gets the FULL metric picker (shared catalog).

Single source of truth: /fg-catalog.js (FG_CAT 200 keys + FG_TABS +
FG_INST) extracted verbatim from the flagship; both fundamental-graphs.html
(v1.5) and the why.html module (fgwhy-3476) now consume it — zero drift.
Module v2: tabbed "Add metric" picker (Favorites / Institutional / IS / BS
/ CF / Growth / Statistics / Per-share / Forecasts + search), up to 20
metrics per chart, shared favorites (jh_fg_favm) + color tags (jh_fg_metc)
with the flagship, per-analyst custom set persisted (jh_fgwhy_custom),
legend chip removal, deep link carries the custom set.

Gates:
  K1  /fg-catalog.js live: marker + rule_of_40 + >=10KB
  K2  flagship v1.5 live: ops3476 + fg-catalog.js ref + inline catalog GONE
      ("'Total revenue','IS'" absent) + prior features intact (fgExportCSV)
  K3  why.html live: fgwhy-3476 + jhfgPicker markers, fgwhy-3470 gone,
      vitals/dollar-flows/TDZ-fix intact
"""
import json, sys, time, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3476"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read()


with report("3476_why_full_picker") as rep:
    out = {"ops": 3476, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:380]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line); rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3476 — shared catalog + full picker on why.html")

    checks = {"K1_catalog_live": False, "K2_flagship_v15": False,
              "K3_why_module_v2": False}
    det = {}
    for _ in range(21):
        try:
            if not checks["K1_catalog_live"]:
                st, b = fetch(f"https://justhodl.ai/fg-catalog.js?cb={int(time.time())}")
                checks["K1_catalog_live"] = (st == 200 and b"FG_CATALOG_OPS3476" in b
                                             and b"rule_of_40" in b and len(b) > 10000)
                det["catalog"] = {"status": st, "bytes": len(b)}
            if not checks["K2_flagship_v15"]:
                st, b = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}")
                checks["K2_flagship_v15"] = (b"ops3476" in b and b"fg-catalog.js" in b
                                             and b"'Total revenue','IS'" not in b
                                             and b"fgExportCSV" in b)
                det["flagship"] = {"externalized": b"'Total revenue','IS'" not in b}
            if not checks["K3_why_module_v2"]:
                st, b = fetch(f"https://justhodl.ai/why.html?cb={int(time.time())}")
                checks["K3_why_module_v2"] = (b"fgwhy-3476" in b and b"jhfgPicker" in b
                                              and b"fgwhy-3470" not in b
                                              and b"jhVitalsTop" in b
                                              and b"jhDollarFlows" in b
                                              and b"ops3475" in b)
                det["why"] = {"module": b"fgwhy-3476" in b,
                              "picker": b"jhfgPicker" in b,
                              "tdz_fix_intact": b"ops3475" in b}
        except Exception as e:
            det["err"] = str(e)[:120]
        if all(checks.values()):
            break
        time.sleep(20)
    for k, v in checks.items():
        gate(k, v, det)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3476.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
