"""ops 3615 — Khalid's mockup: keep all data, rebuild the visuals. Strategic
layer restyled into the full dashboard (5 verdict cards, strategic/tactical
banner, ring-bubble scatter, percentile-driven Tactical Regime panel, $10k
bars, full Score table with colored verdicts, 3 Suggested Allocations) in
reskin-safe tokens. Legacy cards + decisive-call untouched below."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3615_visual_dash") as rep:
    rep.heading("ops 3615 — Capital Compass visual dashboard served-proof")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:600]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    ok1 = False; det = ""; dl = time.time() + 480
    MK = ("Best Defensive Asset", "Current Tactical Regime", "Suggested Allocations",
          "Your $10,000 in", "Capital Compass Score", "Strategic 10-Year View",
          "Maximum Expected Wealth", "Capital Preservation",
          "by_opportunity_percentile", "compass-decisive-call")
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/compass.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {k: (k in html) for k in MK}
            det = str({k: v for k, v in mk.items() if not v}) or "all-present"
            if all(mk.values()):
                ok1 = True; det = f"all {len(MK)} markers served; len={len(html)}"; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(20)
    gate("G1_dashboard_served", ok1, det)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3615.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
