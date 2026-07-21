"""ops 3609 — close the palette saga: gate the ASIA chip from OBSERVED served
bytes (no more escaping guesses). PASS = '#e8e8e8' appears within the 140 bytes
preceding 'm.asia_state' in the served page, plus roles 0/1 re-verified and
hex count >=5. Records the served context verbatim for the archive."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3609_chip_observe") as rep:
    rep.heading("ops 3609 — observed-bytes chip gate (palette saga close)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:520]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:480]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    ok1 = False; det = ""; ctx = ""; dl = time.time() + 420
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            i = html.find("m.asia_state")
            ctx = html[max(0, i - 140):i + 24] if i >= 0 else ""
            chip_ok = i >= 0 and "e8e8e8" in ctx
            r0 = 'stroke="#e8e8e8" stroke-width="1.1" stroke-dasharray' in html
            r1 = "pale dashed = ASIA canary" in html
            n_hex = html.count("e8e8e8")
            det = f"chip_in_ctx={chip_ok} r0={r0} r1={r1} hex={n_hex} ctx={ctx!r}"
            if chip_ok and r0 and r1 and n_hex >= 5:
                ok1 = True; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(20)
    gate("G1_observed_roles", ok1, det)
    out["served_chip_context"] = ctx
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: G1"
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3609.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
