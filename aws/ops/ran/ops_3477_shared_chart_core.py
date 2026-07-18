"""ops 3477 — shared chart core + why.html flagship-grade chart + live picker.

/fg-chart.js (FG_CHART_OPS3477) extracted from flagship v1.3 draw():
dual-axis unit groups, log scale, today divider, dashed estimates, gap-safe
paths, value pills, crosshair. Flagship v1.6 delegates render/fmt/grp to it
(inline core removed). why.html module v3 (fgwhy-3477) renders through the
SAME core at 470px with Values/%Chg/YoY, 1-10Y, Log, FQ/TTM, price overlay —
and the picker now updates the chart LIVE behind the modal (jsdom harness:
live-add ✓, negatives chart in Values ✓, TTM 100B→400B ✓, persistence ✓;
this was Khalid's "checked but never added" — %-mode silently dropped
negative-base series and nothing updated until close).

Gates:
  P1 served-asset integrity: fg-chart.js + fg-catalog.js + flagship inline
     + module inline all pass `node --check` AS SERVED
  P2 flagship v1.6 live: ops3477 + fg-chart ref + inline core gone
     (badges.sort absent) + fgExportCSV intact
  P3 why.html: fgwhy-3477 in, 3476 gone, picker + TDZ-fix + prior sections
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3477"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(src_bytes, label):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(src_bytes)
        p = f.name
    r = subprocess.run(["node", "--check", p], capture_output=True, text=True)
    return r.returncode == 0, (r.stderr or "")[:160], label


with report("3477_shared_chart_core") as rep:
    out = {"ops": 3477, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line); rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3477 — /fg-chart.js core + module v3 live picker")

    got = {}
    for _ in range(21):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if (b"FG_CHART_OPS3477" in got["core"] and b"ops3477" in got["flag"]
                    and b"fgwhy-3477" in got["why"]):
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)

    checks = []
    checks.append(node_ok(got.get("core", b""), "fg-chart.js"))
    checks.append(node_ok(got.get("cat", b""), "fg-catalog.js"))
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x=", "flagship-inline"))
    m2 = re.search(rb'<script id="fgwhy-3477">([\s\S]*?)</script>', got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x=", "module-inline"))
    bad = [(l, e) for ok, e, l in checks if not ok]
    gate("P1_served_js_integrity",
         not bad and b"FG_CHART_OPS3477" in got.get("core", b"")
         and len(got.get("core", b"")) > 7000,
         {"core_bytes": len(got.get("core", b"")), "bad": bad})

    f = got.get("flag", b"")
    gate("P2_flagship_v16",
         b"ops3477" in f and b"/fg-chart.js" in f
         and b"badges.sort" not in f and b"fgExportCSV" in f
         and b"FGChart.render" in f,
         {"delegated": b"FGChart.render" in f,
          "core_removed": b"badges.sort" not in f})

    y = got.get("why", b"")
    gate("P3_why_module_v3",
         b"fgwhy-3477" in y and b"fgwhy-3476" not in y and b"jhfgPicker" in y
         and b"ops3475" in y and b"jhVitalsTop" in y and b"jhDollarFlows" in y,
         {"v3": b"fgwhy-3477" in y, "old_gone": b"fgwhy-3476" not in y,
          "tdz_intact": b"ops3475" in y})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3477.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
