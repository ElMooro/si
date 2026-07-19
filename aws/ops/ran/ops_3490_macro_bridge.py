"""ops 3490 — Tier-2d (final): macro overlays via the wl-series bridge.

Core v7 (OPS3490): per-series OWN scales — any overlay flagged own:true
gets its own hidden Y domain (price included), so multiple macro lines
never squash each other or the metric axes; micro-proven locally
(joint render == solo renders, byte-identical paths). Flagship v2.3:
Macro button -> up to 3 curated overlays (US10Y, US2Y, 2s10s, HY OAS,
CPI YoY, Fed Funds, Unemployment, DXY) fetched from the existing
justhodl-wl-series-api Function URL with ID-FALLBACK per concept (first
200 wins, self-heals vocabulary drift), dashed slate own-scale lines,
dotted legend chips with latest value, ?mx= deep links.

Gates:
  X1 live probe battery: for each of the 8 concepts try its id ladder
     against the bridge; PASS if >=5 concepts resolve with >=50 weekly
     points each; the resolved id map is printed for the archive
  X2 served-JS 4-surface node-check + core OPS3490 marker
  X3 flagship v2.3 live (ops3490, mxbtn, MACROS, data-mx) with every
     prior feature marker intact
"""
import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
WLAPI = "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws"
CONCEPTS = [
    ("US10Y", ["TVC:US10Y", "FRED:DGS10", "DGS10"]),
    ("US02Y", ["TVC:US02Y", "FRED:DGS2", "DGS2"]),
    ("T10Y2Y", ["FRED:T10Y2Y", "T10Y2Y"]),
    ("HYOAS", ["FRED:BAMLH0A0HYM2", "BAMLH0A0HYM2"]),
    ("CPIYOY", ["FRED:CPIAUCSL~pct4", "CPIAUCSL~pct4", "FRED:CPIAUCSL"]),
    ("FEDFUNDS", ["FRED:FEDFUNDS", "FEDFUNDS", "FRED:DFF"]),
    ("UNRATE", ["FRED:UNRATE", "UNRATE"]),
    ("DXY", ["TVC:DXY", "CAPITALCOM:DXY", "DXY", "FRED:DTWEXBGS"]),
]


def fetch(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3490"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3490_macro_bridge") as rep:
    out = {"ops": 3490, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:420]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3490 — macro overlays via wl-series bridge")

    resolved, missed = {}, []
    for key, ids in CONCEPTS:
        hit = None
        for sid in ids:
            try:
                st, b = fetch(f"{WLAPI}/?sym={urllib.parse.quote(sid)}")
                if st == 200:
                    d = json.loads(b)
                    if len(d.get("points") or []) >= 50:
                        hit = {"id": sid, "n": d["n"],
                               "last": d["points"][-1]}
                        break
            except Exception:  # noqa: BLE001
                continue
        if hit:
            resolved[key] = hit
        else:
            missed.append(key)
    rep.log("resolved id map: " + json.dumps(resolved)[:800])
    print("RESOLVED:", json.dumps(resolved)[:800])
    gate("X1_bridge_probe", len(resolved) >= 5,
         {"resolved": {k: v["id"] for k, v in resolved.items()},
          "missed": missed,
          "sample_last": {k: v["last"] for k, v in
                          list(resolved.items())[:3]}})

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")[1]
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")[1]
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")[1]
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")[1]
            if b"OPS3490" in got["core"] and b"ops3490" in got["flag"]:
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>",
                   got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>',
                   got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    gate("X2_served_js", all(checks) and b"OPS3490" in got.get("core", b"")
         and b"Yown" in got.get("core", b""), {"node_ok": checks})
    f = got.get("flag", b"")
    d3 = {"ops3490": b"ops3490" in f, "mxbtn": b"mxbtn" in f,
          "registry": b"MACROS" in f, "chip_x": b"data-mx" in f,
          "evt_intact": b"evtbtn" in f, "rt_intact": b"rtbtn" in f,
          "whales_intact": b"whales_q" in f, "flags_intact": b"fgFlags" in f}
    gate("X3_flagship_v23", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3490.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
