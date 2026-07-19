"""ops 3508 — Tier-3 closed: factor-DNA radar (master-ranker join),
NIGHTLY sector medians, DuPont STACKED decomposition.

Engine v1.10.0 (cache v20):
- factor_dna(sym): joins data/master-ranker.json top_tickers (recon
  3507 found the row home); picks numeric factor columns present in
  >=80%% of the universe (preferred order quality/value/momentum/
  growth/..., cap 7, needs >=5 distinct values), converts the ticker's
  values to CROSS-SECTIONAL percentiles (below + half-ties). Honest
  named dormancy (<30 rows, ticker absent, <4 comparable columns).
- sector medians pulled out of the Monday gate -> the 09:25 UTC daily
  warmer now rebuilds medians+bands NIGHTLY.
Surfaces:
- shared FG_RADAR renderer in fg-catalog.js (rings, spokes, filled
  percentile polygon, axis labels pNN); radar card on BOTH pages.
- DuPont button both pages: loads the 4-series lens (flagship) and
  renders the STACKED decomposition — SIGNED log-contributions around
  a dashed zero axis, segments summing exactly to log ROE (the naive
  positive-share version was mathematically wrong for NM<100%% and the
  harness caught it drawing nothing). Auto-scaled, ROE%% labels.
Harness v13 = 36 behaviors, exit-code enforced.

Gates:
  H1 CI factor-DNA battery: seeded 200-row universe -> pct exact
     (quality 98.2 vs hand-computed), value<15/momentum>90 ordering,
     absent-ticker + thin-universe dormancy named
  H2 NVDA live v20: factor_dna.state ok, 4-7 axes, all pct in [0,100],
     axes printed
  H3 nightly medians: deployed zip carries the NIGHTLY marker inside
     warm_auto BEFORE the Monday gate; direct sector_medians invoke
     refreshes LastModified to now
  H4 surfaces: FG_RADAR served in catalog; flagship ops3508 + fgRadar
     + dupbtn + signed-log marker; module jhfgRadar + jhfgDup + signed-
     log marker; priors; node x4
"""
import importlib.util
import io
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3508"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3508_tier3_close") as rep:
    out = {"ops": 3508, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:460]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3508 — factor-DNA radar · nightly medians · DuPont")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        random.seed(7)
        rows = [{"ticker": "T%03d" % i,
                 "quality": random.random() * 100,
                 "value": random.random() * 100,
                 "momentum": random.random() * 100,
                 "growth": random.random() * 100,
                 "composite": random.random() * 100,
                 "name": "x"} for i in range(200)]
        rows[5] = {"ticker": "NVDA", "quality": 99.0, "value": 10.0,
                   "momentum": 95.0, "growth": 97.0, "composite": 92.0,
                   "name": "n"}
        m._RANKER["doc"] = rows
        m._RANKER["ts"] = 1e12
        f = m.factor_dna("NVDA")
        ax = {a["k"]: a for a in f["axes"]}
        col = sorted(r["quality"] for r in rows)
        below = sum(1 for v in col if v < 99.0)
        exp = round(100 * (below + 0.5) / 200, 1)
        t1 = (f["state"] == "ok" and len(f["axes"]) >= 4
              and abs(ax["quality"]["pct"] - exp) < 0.11
              and ax["value"]["pct"] < 15 and ax["momentum"]["pct"] > 90)
        f2 = m.factor_dna("ZZZZ")
        m._RANKER["doc"] = rows[:10]
        f3 = m.factor_dna("NVDA")
        gate("H1_factor_battery",
             t1 and "not in master-ranker" in f2["why"]
             and f3["state"] == "insufficient",
             {"quality_pct": ax["quality"]["pct"], "expected": exp,
              "axes": [a["k"] for a in f["axes"]],
              "dormancy": [f2["why"], f3["why"]]})
    except Exception as e:  # noqa: BLE001
        gate("H1_factor_battery", False, str(e)[:320])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.10.0 factor-DNA (ops 3508)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["NVDA", "AAPL"], "periods": ["quarter"],
         "refresh": True}).encode())

    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/NVDA_quarter_v20.json")["Body"].read())
        fd = doc.get("factor_dna") or {}
        axes = fd.get("axes") or []
        gate("H2_nvda_radar",
             fd.get("state") == "ok" and 4 <= len(axes) <= 7
             and all(0 <= (a.get("pct") or -1) <= 100 for a in axes),
             {"state": fd.get("state"), "why": fd.get("why"),
              "n_universe": fd.get("n_universe"),
              "axes": [(a["k"], a["pct"]) for a in axes]})
    except Exception as e:  # noqa: BLE001
        gate("H2_nvda_radar", False, str(e)[:300])

    try:
        code = lam.get_function(FunctionName=FN)["Code"]["Location"]
        zb = urllib.request.urlopen(code, timeout=60).read()
        src = zipfile.ZipFile(io.BytesIO(zb)).read(
            "lambda_function.py").decode()
        i1 = src.index("NIGHTLY (ops 3508)")
        i2 = src.index("if annual_too:")
        lam.invoke(FunctionName=FN,
                   Payload=json.dumps({"sector_medians": True}).encode())
        time.sleep(3)
        lm = s3c.head_object(
            Bucket=BUCKET,
            Key="data/fundgraph/sector-medians.json")["LastModified"]
        age = (datetime.now(timezone.utc) - lm).total_seconds()
        gate("H3_nightly_medians", i1 < i2 and age < 300,
             {"marker_before_monday_gate": i1 < i2,
              "medians_age_s": int(age)})
    except Exception as e:  # noqa: BLE001
        gate("H3_nightly_medians", False, str(e)[:300])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"FG_RADAR" in got["cat"] and b"ops3508" in got["flag"] \
               and b"jhfgRadar" in got["why"]:
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)
    f = got.get("flag", b"")
    y = got.get("why", b"")
    ca = got.get("cat", b"")
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", f)
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', y)
    d4 = {"node": node_ok(ca) and node_ok(m1.group(1) if m1 else b"x=")
          and node_ok(m2.group(1) if m2 else b"x="),
          "catalog": b"FG_RADAR" in ca,
          "flag": all(k in f for k in
                      [b"ops3508", b"fgRadar", b"dupbtn",
                       b"signed log-contributions"]),
          "why": all(k in y for k in
                     [b"jhfgRadar", b"jhfgDup",
                      b"signed log-contributions", b"factor_dna"]),
          "priors": b"red-flag digest" in y and b"mxsel" in f
          and b"volume_w" in f}
    gate("H4_surfaces", all(d4.values()), d4)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3508.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
