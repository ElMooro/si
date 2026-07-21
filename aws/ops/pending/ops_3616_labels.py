"""ops 3616 — Khalid: put the asset names ON the chart, not just circles.
Scatter now ALWAYS labels every bubble: ticker inside the ring when it fits,
else greedy collision-avoided placement (above/below/right/left), class-
colored. Gate: served page has lblSlot engine, inBub gone."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3616_labels") as rep:
    rep.heading("ops 3616 — always-on chart labels served-proof")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:400]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:360]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    ok1 = False; det = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/compass.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            mk = {"lblSlot": "lblSlot" in html, "no_inBub": "inBub" not in html,
                  "dash": "Current Tactical Regime" in html,
                  "legacy": "by_opportunity_percentile" in html}
            det = str(mk)
            if all(mk.values()):
                ok1 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(18)
    gate("G1_labels", ok1, det)
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3616.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
