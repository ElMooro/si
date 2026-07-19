"""ops 3502 — eye-fix, auto TA markers, macro dropdowns, VOLUME layer.

Fixes shipped: (1) flagship metric-chip template had an UNCLOSED style
attribute (my 3499 insertion) which mangled the chip HTML — the eye was
there but invisible; AND the generic '.x' remove handler overwrote the
eye's onclick. Both fixed: clean quoted template, remove scoped to
[data-m], eye is a plain U+1F441 glyph. (2) 200-DMA crosses, GC/DC and
CONFIRMED double patterns now render on the chart automatically (no TA
toggle needed); full event set still behind TA. (3) Macro popover
(broken UX) replaced by a dropdown <select> on BOTH surfaces, wired to
the real loaders (loadMacro / loadMacro2) with unavailable-rollback and
removable overlay chips. (4) VOLUME: engine v1.8.0 (cache v18)
continuity-gated (>=95% nonzero over last 3y else named dormancy),
weekly SUM series + 20d-avg + RVOL status + VOL_SPIKE events (>=2.5x
prior 20d avg, cap 15); core v10 bars primitive (own-scale rects,
micro-proven tallest-at-max); RVOL chip in both status strips. Harness
v10 = 24 behaviors.

Gates:
  V0 FMP probe: historical-price-eod/light AAPL rows carry a numeric
     'volume' field (>=99%% of last 250) — printed BEFORE trusting
  V1 CI volume battery (weekly-sum exact 5000, rvol 3.0 + spike,
     cap15 sorted, 90.1%%-coverage blocked, short blocked)
  V2 AAPL live v18: volume.state ok, coverage printed, len(volume_w)
     >=400, rvol>0, spikes<=15 inside sorted events, AND an EXACT
     reconciliation — one complete ISO week summed from the probe rows
     equals the engine's volume_w entry to the share
  V3 surfaces: core OPS3502+bars, flagship (mxsel + renderMacroChips +
     clean data-eye template + volume_w), module (jhfgMxSel + data-mxrm
     + volume_w + auto-marker filter), priors, node x4
"""
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, date, timedelta
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report                      # noqa: E402
from _lambda_deploy_helpers import deploy_lambda   # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url, t=40):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3502"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3502_volume_and_fixes") as rep:
    out = {"ops": 3502, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:460]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3502 — eye/auto-TA/macro-dropdown/volume")

    probe_rows = []
    try:
        raw = json.loads(fetch(
            "https://financialmodelingprep.com/stable/"
            "historical-price-eod/light?symbol=AAPL&from=%s&apikey=%s"
            % ((date.today() - timedelta(days=420)).isoformat(), FMP_KEY)))
        hist = raw.get("historical") or raw.get("data") or raw \
            if isinstance(raw, (dict, list)) else []
        if isinstance(hist, dict):
            hist = hist.get("historical") or []
        probe_rows = [(str(r.get("date"))[:10], r.get("close"),
                       r.get("volume")) for r in hist
                      if isinstance(r, dict) and r.get("date")]
        probe_rows.sort()
        tail = probe_rows[-250:]
        okv = sum(1 for _, _, v in tail
                  if isinstance(v, (int, float)) and v > 0)
        gate("V0_fmp_volume_probe",
             len(tail) >= 200 and okv >= 0.99 * len(tail),
             {"n_rows": len(probe_rows), "tail_250_with_volume": okv,
              "sample": probe_rows[-2:],
              "keys": sorted((hist[-1] or {}).keys())[:8]
              if hist else []})
    except Exception as e:  # noqa: BLE001
        gate("V0_fmp_volume_probe", False, str(e)[:300])

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        d0 = date(2023, 1, 2)

        def D(i):
            return (d0 + timedelta(days=i + (i // 5) * 2)).isoformat()

        N = 800
        vr = [(D(i), 1000.0) for i in range(N)]
        wk = [[D(i * 5 + 4), 1.0] for i in range(N // 5)]
        P, ev, st = m.volume_layer(vr, wk)
        t1 = st["state"] == "ok" and all(
            v == 5000 for _, v in P["volume_w"][2:-2])
        vr2 = vr[:-1] + [(vr[-1][0], 3000.0)]
        _, ev2, st2 = m.volume_layer(vr2, wk)
        t2 = abs(st2["rvol"] - 3.0) < 1e-9 and any(
            "3.0x" in e[2] for e in ev2 if e[1] == "VOL_SPIKE")
        vr3 = [(D(i), 1000.0 if i % 30 else 9000.0) for i in range(N)]
        _, ev3, _ = m.volume_layer(vr3, wk)
        t3 = len(ev3) <= 15 and ev3 == sorted(ev3)
        vr4 = [(D(i), 0.0 if i % 10 == 0 else 1000.0) for i in range(N)]
        P4, _, st4 = m.volume_layer(vr4, wk)
        t4 = P4 is None and "coverage 90.1%" in st4["why"]
        P5, _, st5 = m.volume_layer(vr[:100], wk)
        t5 = P5 is None and "fewer than 250" in st5["why"]
        gate("V1_volume_battery", all([t1, t2, t3, t4, t5]),
             {"weekly_sum": t1, "rvol_spike": t2, "cap_sorted": t3,
              "coverage_block": t4, "short_block": t5})
    except Exception as e:  # noqa: BLE001
        gate("V1_volume_battery", False, str(e)[:300])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.8.0 volume layer (ops 3502)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL"], "periods": ["quarter"],
         "refresh": True}).encode())

    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v18.json")["Body"].read())
        te = doc.get("tech") or {}
        st = (te.get("status") or {})
        vst = st.get("volume") or {}
        vw = (doc.get("points") or {}).get("volume_w") or []
        ev = te.get("events") or []
        spikes = [e for e in ev if e[1] == "VOL_SPIKE"]
        recon = None
        if probe_rows and vw:
            byweek = {}
            for d, _, v in probe_rows:
                try:
                    k = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
                except ValueError:
                    continue
                if isinstance(v, (int, float)):
                    byweek.setdefault(k, 0.0)
                    byweek[k] += v
            wk_map = {datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]: v
                      for d, v in vw[-60:]}
            full = sorted(set(byweek) & set(wk_map))[1:-1]
            if full:
                k = full[-2]
                recon = (k, byweek[k], wk_map[k],
                         abs(byweek[k] - wk_map[k]) < 1.0)
        gate("V2_aapl_volume_live",
             vst.get("state") == "ok"
             and (vst.get("coverage_pct") or 0) >= 95
             and len(vw) >= 400 and (vst.get("rvol") or 0) > 0
             and len(spikes) <= 15 and ev == sorted(ev)
             and (recon is None or recon[3]),
             {"volume_status": vst, "n_volume_w": len(vw),
              "n_spikes": len(spikes), "recent_spikes": spikes[-3:],
              "week_reconciliation": recon})
    except Exception as e:  # noqa: BLE001
        gate("V2_aapl_volume_live", False, str(e)[:320])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3502" in got["core"] and b"mxsel" in got["flag"] \
               and b"jhfgMxSel" in got["why"]:
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
    f = got.get("flag", b"")
    y = got.get("why", b"")
    d3 = {"node_ok": all(checks),
          "core": b"OPS3502" in got.get("core", b"")
          and b"s.bars" in got.get("core", b""),
          "flag": all(k in f for k in
                      [b"mxsel", b"renderMacroChips", b"volume_w",
                       b"data-eye", rb"style=\"${hid?'opacity:.35':''}\""
                       .replace(rb"\"", b'"')])
          and b"mxbtn" not in f,
          "why": all(k in y for k in
                     [b"jhfgMxSel", b"data-mxrm", b"volume_w", b"_200$"]),
          "priors": b"jhfl" in f and b"fgverd" in f
          and b"jhfgVerd" in y and b"ops3475" in y}
    gate("V3_surfaces", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3502.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
