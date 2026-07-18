"""ops 3475 — why.html TDZ fix: 'Cannot access verdict before initialization'.

Root cause (proved by local render-truth harness — OLD text throws this
exact ReferenceError, NEW renders 9.9KB): the ops-3300 verdict-guard in
renderDilutionPillar() declared `let verdict` seven lines BELOW the
pre-existing `const col=V[verdict]`. Every ticker whose equity-research doc
carries the Pillar-6 dilution module (fresh docs — GOOGL) threw at render
and fell into the page's "generating server-side" catch-all. Fix: `col`
moved AFTER the guard — also the correct semantics (badge color must match
the RECOMPUTED verdict, the guard's whole purpose).

Gates: T1 live page has ops3475 marker AND the old broken adjacency is gone
       T2 all prior markers intact (jhVitalsTop/jhDollarFlows/jhFundGraphs/
          fgwhy-3470) — additive/no-collateral proof
"""
import json, sys, time, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]
BROKEN_ADJ = b"UNKNOWN:'#8899aa'};\n  const col=V[verdict]"

with report("3475_why_tdz_fix") as rep:
    out = {"ops": 3475, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:380]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line); rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3475 — why.html renderDilutionPillar TDZ fix (verdict)")

    ok1 = ok2 = False; det = {}
    for _ in range(21):
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/why.html?cb={int(time.time())}",
                headers={"User-Agent": "ops-3475"})
            with urllib.request.urlopen(req, timeout=30) as r:
                b = r.read()
            ok1 = (b"ops3475" in b) and (BROKEN_ADJ not in b)
            ok2 = all(m in b for m in (b"jhVitalsTop", b"jhDollarFlows",
                                       b"jhFundGraphs", b"fgwhy-3470"))
            det = {"marker": b"ops3475" in b,
                   "old_adjacency_gone": BROKEN_ADJ not in b,
                   "prior_markers": ok2}
        except Exception as e:
            det = {"err": str(e)[:120]}
        if ok1 and ok2:
            break
        time.sleep(20)
    gate("T1_fix_live", ok1, det)
    gate("T2_no_collateral", ok2, det)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3475.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
