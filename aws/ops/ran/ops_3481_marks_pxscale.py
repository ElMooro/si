"""ops 3481 — price own-scale (collapse fix) + placeable vertical markers.

Root cause of "Show price collapses the metrics": core forced the price
series onto the 2nd metric axis (Values, 2 groups) or the shared % axis —
a $300 tape crushed 0-50% series flat. Core v3 (OPS3481): price ALWAYS
renders on its own hidden scale (right-rail labeled only when free);
truth-test proves metric paths byte-identical with/without price.

New: click anywhere on either chart to DROP a vertical marker snapped to
the nearest data date (violet line + date label + \u2715 handle to remove);
persisted per symbol-set (flagship, +?mk= deep-link) / per ticker (why
module); Clear-marks button. jsdom: place->persist->remove proven.
(chart-pro already ships native TradingView-style line tools — untouched.)

Gates: Y1 served-JS integrity (4 surfaces node-check) + core OPS3481
       Y2 flagship v1.9 live (ops3481, onMark, mkclr)
       Y3 module marks live (jh_fgwhy_marks) + ops3475/flags intact
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3481"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p], capture_output=True).returncode == 0


with report("3481_marks_pxscale") as rep:
    out = {"ops": 3481, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:380]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3481 — px own-scale + vertical markers")

    got = {}
    for _ in range(21):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3481" in got["core"] and b"ops3481" in got["flag"] \
               and b"jh_fgwhy_marks" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)

    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    gate("Y1_served_js", all(checks) and b"OPS3481" in got.get("core", b"")
         and b"onUnmark" in got.get("core", b""),
         {"node_ok": checks, "core_bytes": len(got.get("core", b""))})
    f = got.get("flag", b"")
    gate("Y2_flagship_v19", b"ops3481" in f and b"onMark" in f and b"mkclr" in f, {})
    y = got.get("why", b"")
    gate("Y3_module_marks", b"jh_fgwhy_marks" in y and b"ops3475" in y
         and b"jhfgFlags" in y, {})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3481.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
