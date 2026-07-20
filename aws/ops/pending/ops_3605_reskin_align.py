"""ops 3605 — cyan mystery CLOSED: pages.yml reskin_site.py v2 is a hue engine
(saturated hue 165-300 → amber at build) — the overlay's #22d3ee was being
palette-converted, not stripped. Source now uses reskin-SAFE magenta #ff6ec7
(hue ~323, outside the cool band). Gate: served page carries the overlay
stroke, legend span and ASIA chip in #ff6ec7 at the exact anchor roles."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3605_reskin_align") as rep:
    rep.heading("ops 3605 — reskin-safe canary accent (#ff6ec7) served-proof")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:460]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    ok1 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/signal-board.html?cb=" + str(int(time.time())),
                    headers={"User-Agent": "Mozilla/5.0 (ops)",
                             "Accept-Encoding": "identity"}), timeout=30) as r:
                html = r.read().decode("utf-8", "replace")
                hdr = {k: r.headers.get(k) for k in ("cf-cache-status", "content-encoding")}
            n_hex = html.count("ff6ec7")
            a1 = 'stroke="#ff6ec7"' in html            # ribbon Asia overlay
            a2 = "color:#ff6ec7" in html               # legend + ASIA chip
            order = html.find('id="jh-fifx"') < html.find('id="jh-spx-ma"')
            det = (f"hex_count={n_hex} overlay_stroke={a1} color_refs={a2} "
                   f"order_top={order} len={len(html)} hdr={hdr}")
            if n_hex >= 3 and a1 and a2 and order:
                ok1 = True; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(20)
    gate("G1_served_palette", ok1, det)
    out["root_cause"] = ("pages.yml step 'Reskin legacy palette -> Amber' runs "
                        "scripts/reskin_site.py v2 hue engine on _site: saturated "
                        "hue 165-300 → amber. #22d3ee (190°) was converted, not lost. "
                        "Rule: page accents must sit OUTSIDE 165-300 or match the amber system.")
    print(out["root_cause"]); rep.log(out["root_cause"])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3605.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
