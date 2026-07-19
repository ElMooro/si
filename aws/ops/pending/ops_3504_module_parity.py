"""ops 3504 — why.html module parity with the flagship's recent layer.

Added (all module-side, additive): (1) 👁 hide per metric chip — hidden
metrics leave the chart and become dashed ghost chips that restore on
click (persisted, localStorage jh_fgwhy_hidden; ✕-remove already
existed). (2) Table view — last 12 periods x plotted series, unit-
formatted. (3) ⬇CSV — long-format date,series,value from the full
series (pure builder exposed for the harness). (4) ⬇PNG — SVG->canvas
snapshot on dark bg. Earnings/Log/Events/TA/Macro-dropdown/RVOL/volume
bars were already present (census-verified). Harness v11 = 29 behaviors
(hide->ghost->restore round-trip asserted on legend text, table >=5
rows with series headers, CSV 508 rows starting date,series,value).

  M1 surfaces: module carries jhfgTbl2/jhfgCsv2/jhfgPng2/data-eye/
     data-unhide/jh_fgwhy_hidden/buildCSV + priors (jhfgTa, jhfgMxSel,
     volume_w, jhfgVerd, ops3475) + node x2; flagship untouched-green
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3504"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3504_module_parity") as rep:
    out = {"ops": 3504, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:440]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3504 — module parity (hide/table/csv/png)")
    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            if b"jhfgTbl2" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)
    y = got.get("why", b""); f = got.get("flag", b"")
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', y)
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", f)
    d1 = {"node": node_ok(m2.group(1) if m2 else b"x=")
          and node_ok(m1.group(1) if m1 else b"x="),
          "module_new": all(k in y for k in
                            [b"jhfgTbl2", b"jhfgCsv2", b"jhfgPng2",
                             b"data-eye", b"data-unhide",
                             b"jh_fgwhy_hidden", b"buildCSV"]),
          "module_priors": all(k in y for k in
                               [b"jhfgTa", b"jhfgMxSel", b"volume_w",
                                b"jhfgVerd", b"ops3475", b"data-mxrm"]),
          "flagship_green": all(k in f for k in
                                [b"mxsel", b"volume_w", b"data-eye"])}
    gate("M1_surfaces", all(d1.values()), d1)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3504.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
