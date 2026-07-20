"""ops 3606 — final palette fix: reskin v2 ALSO remaps pinks/reds (h>=300 or
<15, s>0.40) to semantic — magenta was doomed too. Asia canary accent moved to
desaturated neutral #e6e8ee (survives every hue rule). Gates on the three ROLE
anchors (overlay stroke+dasharray, legend 'pale dashed', ASIA chip ternary)."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3606_neutral_accent") as rep:
    rep.heading("ops 3606 — reskin-stable neutral canary accent, served-proof")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:460]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    ROLES = ['stroke="#e6e8ee" stroke-width="1.1" stroke-dasharray',
             "pale dashed = ASIA canary",
             ":'#e6e8ee')+'" + '"' + ">'+m.asia_state"]
    ok1 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0 (ops)",
                             "Accept-Encoding": "identity"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
            hit = {i: (rl in html) for i, rl in enumerate(ROLES)}
            order = html.find('id="jh-fifx"') < html.find('id="jh-spx-ma"')
            det = f"roles={hit} order_top={order} len={len(html)}"
            if all(hit.values()) and order:
                ok1 = True; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(20)
    gate("G1_served_roles", ok1, det)
    out["root_cause_final"] = (
        "reskin_site.py v2 hue engine on _site: [165-300 sat]→amber · "
        "[90-165 s>.35 greens]→semantic · [>=300 or <15 s>.40 pinks/reds]→semantic. "
        "Stable accents: hue 15-90 OR desaturated. Canary overlay now neutral #e6e8ee "
        "dashed (institutional reference-series grey). #22d3ee/#ff6ec7 were both converted, never lost.")
    print(out["root_cause_final"]); rep.log(out["root_cause_final"])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3606.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
