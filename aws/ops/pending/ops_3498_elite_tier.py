"""ops 3498 — ELITE tier: astonishing metrics get a ⭐ above green.

Engine v1.6.0 (cache v16): ELITE_NORM absolute table (ROIC>=30, perfect
Piotroski 9, Altman>=8, Beneish<=-3, net cash >=1x EBITDA, SBC<=0.5%,
...) upgrades a green in place — never a new emission, never without a
value. Second basis: SECTOR TOP DECILE via new p10/p90 bands computed
in build_sector_medians v2 from the same 503-name cross-section (schema
additive: flat medians untouched, "bands" added). Elite-first ordering,
n_elite in summary. Surfaces: gold ⭐ chips/cards + ELITE badge, counts
gain ⭐N. jsdom harness v7 = 16 behaviors (elite renders FIRST, gold,
badged). CI battery: elite-fortress(>=11), ZERO-false-elites on merely-
good greens, sector-decile exact both directions.

Gates:
  E1 CI elite battery (4 suites)
  E2 medians v2: invoke sector_medians -> bands present for >=6 keys,
     Technology fcf_yield p10 < p50 < p90 strict
  E3 AAPL live v16: n_elite >= 1, every elite is side G with 'ELITE' in
     why and non-null val; Altman Z elite expected (10.94 >= 8); top
     elites printed
  E4 surfaces: ops3498 + \\u2b50 markers both pages + priors + node x4
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3498"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


def flat(v, n=14):
    return [["2024-%02d-01" % (i % 12 + 1), v] for i in range(n)]


with report("3498_elite_tier") as rep:
    out = {"ops": 3498, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:420]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3498 — elite tier (astonishing metrics)")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        LB = 4
        EF = {"roic_pct": flat(35), "gross_margin_pct": flat(80),
              "fcf_margin_pct": flat(34), "piotroski_f": flat(9),
              "altman_z": flat(11), "interest_coverage_ttm": flat(80),
              "beneish_m": flat(-3.2), "netdebt_to_ebitda_ttm": flat(-2),
              "net_shareholder_yield_pct": flat(9),
              "sbc_to_revenue_pct": flat(0.3),
              "sloan_accruals_pct": flat(0.6), "income_quality": flat(1.7)}
        r1 = m.derive_verdicts(EF, LB, "Technology", {}, {})
        t1 = (r1["summary"]["n_elite"] >= 11
              and r1["greens"][0].get("elite")
              and all("ELITE" in x["why"] for x in r1["greens"]
                      if x.get("elite")))
        PG = {"roic_pct": flat(16), "gross_margin_pct": flat(58),
              "fcf_margin_pct": flat(22), "piotroski_f": flat(7),
              "altman_z": flat(3.5), "beneish_m": flat(-2.6),
              "net_shareholder_yield_pct": flat(4.5),
              "current_ratio": flat(2.2)}
        r2 = m.derive_verdicts(PG, LB, "Technology", {}, {})
        t2 = r2["summary"]["n_green"] >= 8 and r2["summary"]["n_elite"] == 0
        r3 = m.derive_verdicts(
            {"fcf_yield_pct": flat(8.5)}, LB, "Technology",
            {"fcf_yield_pct": 4.0},
            {"fcf_yield_pct": {"p10": 1.0, "p90": 7.5, "n": 60}})
        t3 = any(x.get("elite") and "top decile" in x["why"]
                 for x in r3["greens"])
        r4 = m.derive_verdicts(
            {"fcf_yield_pct": flat(6.5)}, LB, "Technology",
            {"fcf_yield_pct": 4.0},
            {"fcf_yield_pct": {"p10": 1.0, "p90": 7.5, "n": 60}})
        t4 = all(not x.get("elite") for x in r4["greens"])
        r5 = m.derive_verdicts(
            {"beneish_m": flat(-2.8)}, LB, "Technology",
            {"beneish_m": -2.3},
            {"beneish_m": {"p10": -2.75, "p90": -1.6, "n": 60}})
        t5 = any(x.get("elite") and "p10 -2.75" in x["why"]
                 for x in r5["greens"])
        gate("E1_elite_battery", all([t1, t2, t3, t4, t5]),
             {"fortress": t1, "no_false": t2, "decile_hi": t3,
              "sub_p90_stays_green": t4, "decile_lo": t5,
              "n_elite_fortress": r1["summary"]["n_elite"]})
    except Exception as e:  # noqa: BLE001
        gate("E1_elite_battery", False, str(e)[:300])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.6.0 elite tier (ops 3498)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(2)

    try:
        lam.invoke(FunctionName=FN,
                   Payload=json.dumps({"sector_medians": True}).encode())
        sm = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/sector-medians.json")["Body"].read())
        bands = sm.get("bands") or {}
        bkeys = {k for m2 in bands.values() for k in m2}
        tb = ((bands.get("Technology") or {}).get("fcf_yield_pct") or {})
        p50 = ((sm.get("sectors") or {}).get("Technology")
               or {}).get("fcf_yield_pct")
        strict = (tb.get("p10") is not None and p50 is not None
                  and tb.get("p90") is not None
                  and tb["p10"] < p50 < tb["p90"])
        gate("E2_bands_v2", len(bkeys) >= 6 and strict,
             {"band_keys": sorted(bkeys),
              "tech_fcf_yield": {"p10": tb.get("p10"), "p50": p50,
                                 "p90": tb.get("p90"), "n": tb.get("n")}})
    except Exception as e:  # noqa: BLE001
        gate("E2_bands_v2", False, str(e)[:280])

    try:
        lam.invoke(FunctionName=FN, Payload=json.dumps(
            {"warm": ["AAPL"], "periods": ["quarter"],
             "refresh": True}).encode())
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v16.json")["Body"].read())
        V = doc.get("verdicts") or {}
        els = [x for x in (V.get("greens") or []) if x.get("elite")]
        gate("E3_aapl_elites",
             V.get("summary", {}).get("n_elite", 0) >= 1
             and all(x["side"] == "G" and "ELITE" in x["why"]
                     and x.get("val") is not None for x in els)
             and any(x["k"] == "altman_z" for x in els),
             {"n_elite": V["summary"].get("n_elite"),
              "elites": [x["why"] for x in els][:4],
              "n_green": V["summary"].get("n_green"),
              "n_red": V["summary"].get("n_red")})
    except Exception as e:  # noqa: BLE001
        gate("E3_aapl_elites", False, str(e)[:280])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"ops3498" in got["flag"] and b"\\u2b50" in got["why"]:
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
    d4 = {"node_ok": all(checks),
          "flag": b"ops3498" in f and b"\\u2b50" in f and b"n_elite" in f,
          "why": b"\\u2b50" in y and b"ELITE" in y,
          "priors": b"fgverd" in f and b"jhfgVerd" in y
          and b"ops3475" in y and b"secbtn" in f}
    gate("E4_surfaces", all(d4.values()), d4)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3498.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
