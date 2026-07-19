"""ops 3492 — Tier-3a: why.html module parity (ratio + macro + DuPont).

Single-source promotion: macro registry + wk2d + bridge URL moved into
/fg-catalog.js (FG_SHARED_OPS3492: FG_WLAPI / FG_MACROS / FG_wk2d);
flagship v2.5 consumes window.* (inline copies deleted, asserted). The
why.html module gains: ＋Ratio (single-ticker metric÷metric via
FGChart.ratio, dashed x-series, persisted jh_fgwhy_ratios, legend ✕),
Macro (up to 2 own-scale overlays via loadMacro2 with id-fallback +
derived 2s10s + wk2d conversion, persisted jh_fgwhy_macro, boot
prefetch), and the DuPont ROE lens (roe = net margin × asset turnover ×
equity multiplier) on BOTH surfaces. jsdom harness v4: ratio-add renders
'÷' legend + persists; macro toggle with ISO-WEEK stub renders the
own-scale dashed line + chip + persists.

Gates:
  P1 served catalog: FG_SHARED_OPS3492 + FG_MACROS + FG_wk2d + TVC:US10Y
  P2 flagship v2.5: ops3492 + window.FG_MACROS consumed + inline id
     ladder GONE ("ids:['TVC:US10Y'" absent) + DuPont + priors intact
  P3 module: jhfgRt + jhfgMx + loadMacro2 + DuPont + ops3492 + priors
  P4 4-surface node-check as served
"""
import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3492"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3492_module_parity") as rep:
    out = {"ops": 3492, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:440]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:400]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3492 — module parity: ratio + macro + DuPont")

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if (b"FG_SHARED_OPS3492" in got["cat"]
                    and b"ops3492" in got["flag"]
                    and b"jhfgMx" in got["why"]):
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)

    c = got.get("cat", b"")
    gate("P1_shared_config",
         b"FG_SHARED_OPS3492" in c and b"FG_MACROS" in c
         and b"FG_wk2d" in c and b"TVC:US10Y" in c
         and b"derived:['US10Y','US02Y']" in c,
         {"bytes": len(c)})

    f = got.get("flag", b"")
    d2 = {"ops3492": b"ops3492" in f,
          "consumes_shared": b"window.FG_MACROS" in f,
          "inline_gone": b"ids:['TVC:US10Y'" not in f,
          "dupont": b"DuPont" in f,
          "mx_intact": b"mxbtn" in f, "rt_intact": b"rtbtn" in f,
          "evt_intact": b"evtbtn" in f, "whales_intact": b"whales_q" in f}
    gate("P2_flagship_v25", all(d2.values()), d2)

    y = got.get("why", b"")
    d3 = {"ratio_btn": b"jhfgRt" in y, "macro_btn": b"jhfgMx" in y,
          "loader": b"loadMacro2" in y, "dupont": b"DuPont" in y,
          "ops3492": b"ops3492" in y,
          "ratios_store": b"jh_fgwhy_ratios" in y,
          "macro_store": b"jh_fgwhy_macro" in y,
          "tdz_intact": b"ops3475" in y, "evt_intact": b"jhfgEvt" in y,
          "flags_intact": b"jhfgFlags" in y,
          "marks_intact": b"jh_fgwhy_marks" in y}
    gate("P3_module_parity", all(d3.values()), d3)

    checks = [node_ok(got.get("core", b"x=")), node_ok(c or b"x=")]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", f)
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', y)
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    gate("P4_served_js", all(checks), {"node_ok": checks})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3492.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
