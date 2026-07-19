"""ops 3500 — Technical-analysis layer on Fundamental Graphs.

Engine v1.7.0 (cache v17) TA_ENGINE_OPS3500 computed on TRUE DAILY bars
(fetch_price already pulls 10y daily; weekly was only the chart
downsample): 20/50/100/200-DMA + every price-vs-MA cross (2y window),
golden/death 50/200 (full 10y), Bollinger 20d +/-2sigma, Wilder RSI-14,
double top/bottom detector (10-bar fractal pivots, 3% peaks, 20-250 bar
gap, 5% valley, confirmed-on-break). Weekly-sampled overlay series land
in P (px_ma*/px_bb_*/rsi_14); doc.tech = {events, status}. Core v9:
pxAux series share the PRICE scale exactly (aux==price => identical
paths, micro-proven) + tech rail circles. Both surfaces: TA toggle
(auto-enables price), MA/BB overlays, cross/GC/DC/pattern markers,
status strip (vs-200 %, bull stack, last cross, RSI, BB position,
patterns). jsdom harness v9 = 21 behaviors.

Gates:
  T1 CI math battery: SMA exact on ramp; Bollinger vs manual mu+/-2sd;
     RSI Wilder exact (100 then 65.0 on the -7 step); X_UP_200 and
     GC dates equal SELF-RECOMPUTED crossings; bull-stack on trend;
     double top AND bottom confirmed on crafted M/W; zero patterns on
     monotone trend
  T2 AAPL live v17: bb_up > ma20 > bb_dn; |last/ma200-1| < 40%;
     above_200 == (last_close > ma200); RSI in (0,100); events sorted
     with >=1 entry; px_ma200 weekly series >= 100 pts; status printed
  T3 surfaces: core OPS3500+pxAux, flagship v3.0 (tabtn/fgTech/px_ma200),
     module (jhfgTa/jhfgTaStrip), priors, node x4
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


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3500"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3500_ta_layer") as rep:
    out = {"ops": 3500, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:480]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:440]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3500 — technicals: MAs, crosses, Bollinger, RSI, patterns")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)

        def D(i):
            return "2024-%02d-%02d" % (1 + i // 28, 1 + i % 28)

        t_sma = m._sma([float(c) for c in range(1, 61)], 5)[59] == 58.0
        cs2 = [10.0] * 40
        cs2[30] = 20.0
        up, dn, _ = m._bb20(cs2)
        mu = sum(cs2[11:31]) / 20
        sd = (sum((x - mu) ** 2 for x in cs2[11:31]) / 20) ** 0.5
        t_bb = abs(up[30] - (mu + 2 * sd)) < 1e-9 \
            and abs(dn[30] - (mu - 2 * sd)) < 1e-9
        cs4 = [float(100 + i) for i in range(16)] + [108.0]
        r2 = m._rsi14(cs4)
        exp = 100 - 100 / (1 + ((13.0) / 14) / (7 / 14))
        t_rsi = r2[14] == 100.0 and abs(r2[16] - exp) < 1e-9
        closes = [100.0] * 200 + [90.0] * 80 + [120.0] * 120
        ta = m.compute_ta([(D(i), c) for i, c in enumerate(closes)])
        ma200 = m._sma(closes, 200)
        ma50 = m._sma(closes, 50)
        exp_x = next(D(i) for i in range(len(closes))
                     if ma200[i] is not None and closes[i] > ma200[i]
                     and any(ma200[j] is not None and closes[j] < ma200[j]
                             for j in range(i)))
        xs = [e for e in ta["events"] if e[1] == "X_UP_200"]
        idxs = [i for i in range(len(closes))
                if ma50[i] is not None and ma200[i] is not None
                and ma50[i] != ma200[i]]
        sg = [1 if ma50[i] > ma200[i] else -1 for i in idxs]
        exp_gc = next(D(idxs[b]) for b in range(1, len(sg))
                      if sg[b - 1] == -1 and sg[b] == 1)
        gc = [e for e in ta["events"] if e[1] == "GC_50_200"]
        t_x = bool(xs) and xs[0][0] == exp_x
        t_gc = bool(gc) and gc[0][0] == exp_gc
        ta3 = m.compute_ta([(D(i), 100.0 + i * 0.3) for i in range(400)])
        t_stk = ta3["status"]["bull_stack"] and not ta3["status"]["patterns"]
        seq = [70 + i * 0.95 for i in range(31)] + [100.0] \
            + [100 - 1.0 * (i + 1) for i in range(10)] \
            + [90.2, 89.8, 90.4, 89.9, 90.1, 90.3, 89.7, 90.0, 90.2, 89.9] \
            + [91, 93, 95, 97, 98.5, 99.4, 100.1, 100.3] \
            + [99, 97, 94, 91, 88, 85, 82, 79, 78, 77, 76, 75]
        ta2 = m.compute_ta([(D(i), float(c)) for i, c in enumerate(seq)])
        dt = [p for p in ta2["status"]["patterns"] if p["type"] == "DBL_TOP"]
        taB = m.compute_ta([(D(i), float(200 - c))
                            for i, c in enumerate(seq)])
        db = [p for p in taB["status"]["patterns"]
              if p["type"] == "DBL_BOTTOM"]
        t_pat = bool(dt) and dt[0]["confirmed"] \
            and bool(db) and db[0]["confirmed"]
        gate("T1_math_battery",
             all([t_sma, t_bb, t_rsi, t_x, t_gc, t_stk, t_pat]),
             {"sma": t_sma, "bb": t_bb, "rsi": t_rsi,
              "x200": (t_x, exp_x), "gc": (t_gc, exp_gc),
              "stack_and_nofalse": t_stk, "patterns": t_pat})
    except Exception as e:  # noqa: BLE001
        gate("T1_math_battery", False, str(e)[:320])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.7.0 TA layer (ops 3500)",
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
            Key="data/fundgraph/cache/AAPL_quarter_v17.json")["Body"].read())
        st = (doc.get("tech") or {}).get("status") or {}
        ev = (doc.get("tech") or {}).get("events") or []
        p200 = (doc.get("points") or {}).get("px_ma200") or []
        cons = (st.get("above_200")
                == (st.get("last_close", 0) > st.get("ma200", 9e9)))
        band_ok = (st.get("ma20") is not None
                   and (doc["points"].get("px_bb_up") or [[0, 0]])[-1][1]
                   > st["ma20"]
                   > (doc["points"].get("px_bb_dn") or [[0, 9e9]])[-1][1])
        gate("T2_aapl_live",
             band_ok and cons
             and abs(st.get("last_close", 0) / (st.get("ma200") or 1) - 1)
             < 0.4
             and 0 < (st.get("rsi14") or -1) < 100
             and len(ev) >= 1 and ev == sorted(ev)
             and len(p200) >= 100,
             {"status": {k: st.get(k) for k in
                         ("last_close", "ma200", "pct_vs_200", "above_200",
                          "bull_stack", "rsi14", "bb_pos", "last_cross")},
              "n_events": len(ev), "n_px_ma200": len(p200),
              "patterns": st.get("patterns")})
    except Exception as e:  # noqa: BLE001
        gate("T2_aapl_live", False, str(e)[:320])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3500" in got["core"] and b"ops3500" in got["flag"] \
               and b"jhfgTa" in got["why"]:
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
          "core": b"OPS3500" in got.get("core", b"")
          and b"pxAux" in got.get("core", b""),
          "flag": all(k in f for k in
                      [b"ops3500", b"tabtn", b"fgTech", b"px_ma200"]),
          "why": b"jhfgTa" in y and b"jhfgTaStrip" in y,
          "priors": b"data-eye" in f and b"jhfl" in f
          and b"jhfgVerd" in y and b"ops3475" in y}
    gate("T3_surfaces", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3500.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
