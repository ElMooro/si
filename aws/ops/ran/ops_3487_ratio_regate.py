"""ops 3487 — T1 shim fix regate (window-stub, no replace). Original: Tier-2b pairs spread/ratio builder (flagship v2.1).

FGChart.ratio (core, OPS3487): pure at-or-before date-aligned division,
<=140d gap guard, zero guard — micro-proven locally (exact values,
gap-drop, div-0 drop). Flagship: Ratio button -> builder popover
(symA*metric / symB*metric over the 202-metric catalog), up to 6 ratios as
dashed x-unit series riding Values/%Chg/YoY, dashed legend chips with
latest + own-history p/z, ?rt= deep-links, saved-graphs carry ratios,
load resets stale ratios/marks.

Gates:
  T1 CI re-runs the ratio micro-test (node, exact-value asserts)
  T2 served core has FGChart.ratio + OPS3487; 4-surface node-check
  T3 flagship v2.1 live (ops3487, rtbtn, ratioRaw, data-ri) ; prior
     features intact (whales_q, fgFlags, mkclr, ernbtn)
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3487"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_run(src):
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(src); p = f.name
    return subprocess.run(["node", p], capture_output=True, text=True)


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p], capture_output=True).returncode == 0


with report("3487_ratio_regate") as rep:
    out = {"ops": 3487, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3487 — pairs ratio builder")

    core_local = (REPO / "fg-chart.js").read_text()
    test = core_local + """
;var FGChart=window.FGChart;"""
    test = test + """
;(function(){
  var A=[['2024-01-15',10],['2024-04-15',20],['2024-07-15',30],['2024-10-15',40]];
  var B=[['2024-01-15',2],['2024-04-15',4],['2024-07-15',5],['2024-10-15',0]];
  var R=FGChart.ratio(A,B);
  var t1=JSON.stringify(R)===JSON.stringify([['2024-01-15',5],['2024-04-15',5],['2024-07-15',6]]);
  var t2=FGChart.ratio(A,[['2023-01-01',2]]).length===0;
  console.log(JSON.stringify({t1:t1,t2:t2,R:R}));
  process.exit(t1&&t2?0:1);
})();"""
    # strip DOM-only render body references? ratio is standalone; window absent in node:
    test = "var window={};\n" + test
    r = node_run(test)
    gate("T1_ratio_micro", r.returncode == 0,
         (r.stdout or r.stderr)[:220])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3487" in got["core"] and b"ops3487" in got["flag"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    gate("T2_served_core", all(checks) and b"OPS3487" in got.get("core", b"")
         and b"ratio:ratio" in got.get("core", b""), {"node_ok": checks})
    f = got.get("flag", b"")
    d3 = {"ops3487": b"ops3487" in f, "rtbtn": b"rtbtn" in f,
          "ratioRaw": b"ratioRaw" in f, "chip_x": b"data-ri" in f,
          "whales_intact": b"whales_q" in f, "flags_intact": b"fgFlags" in f,
          "marks_intact": b"mkclr" in f, "ern_intact": b"ernbtn" in f}
    gate("T3_flagship_v21", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3487.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
