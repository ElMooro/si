"""ops 3608 — palette saga FINAL: #e6e8ee (s≈0.19, h≈225) sits in reskin's cool
band with l>0.82 → ramp()-converted. Only s<0.06 true greys pass untouched.
Canary accent → #e8e8e8. Single gate on all three served roles."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3608_true_neutral") as rep:
    rep.heading("ops 3608 — true-neutral canary accent, served-proof")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:480]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:440]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    ROLES = ['stroke="#e8e8e8" stroke-width="1.1" stroke-dasharray',
             "pale dashed = ASIA canary",
             ":'#e8e8e8')+'\\\">'+m.asia_state"]
    ok1 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            hit = [rl in html for rl in ROLES]
            det = str(hit) + f" e8e8e8_count={html.count('e8e8e8')}"
            if all(hit):
                ok1 = True; break
        except Exception as e:
            det = str(e)[:140]
        time.sleep(20)
    gate("G1_served_roles", ok1, det)
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3608.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
