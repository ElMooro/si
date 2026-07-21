"""ops 3610 — the 3609 miss was the scan WINDOW: the chip's '#e8e8e8' sits in
the ternary ~120 bytes AFTER 'm.asia_state' (the \\' in 3609's ctx were repr
artifacts). Gate = hex within [i, i+320] downstream + roles 0/1 + count>=5."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3610_chip_window") as rep:
    rep.heading("ops 3610 — chip gate, downstream window (saga close)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:520]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:480]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    ok1 = False; det = ""; dl = time.time() + 360
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            i = html.find("m.asia_state")
            ctx = html[max(0, i):i + 320] if i >= 0 else ""
            chip_ok = i >= 0 and "e8e8e8" in ctx
            r0 = 'stroke="#e8e8e8" stroke-width="1.1" stroke-dasharray' in html
            r1 = "pale dashed = ASIA canary" in html
            det = f"chip_downstream={chip_ok} r0={r0} r1={r1} hex={html.count('e8e8e8')} ctx_tail={ctx[80:200]!r}"
            if chip_ok and r0 and r1 and html.count("e8e8e8") >= 5:
                ok1 = True; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(18)
    gate("G1_roles_final", ok1, det)
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3610.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
