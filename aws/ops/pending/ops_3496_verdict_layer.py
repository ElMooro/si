"""ops 3496 — verdict layer (green + red flags, norm + sector + trend).

Engine v1.5.0 (cache v15) VERDICTS_ENGINE_OPS3495: 26-rule pure layer
over every doc — absolute-norm rules (ROIC>=15 green, Beneish>=-1.78
red, ...), 3y trend rules (margin expansion/roll, DSO stretch), and
sector-relative rules against the ops-3493 medians (cheap/rich vs p50,
LOWER_BETTER-aware). Financial-sector suppression mirrors forensic
FIN_SECTORS verbatim (incl. Real Estate); a verdict is NEVER emitted
without a numeric value. Both surfaces render ONLY doc.verdicts (zero
page-side rules -> parity by construction): flagship green/red card
panel + metacard ✅/🚩 counts; why.html chip rows with SEV3 badges and
click-to-chart. jsdom harness v6 = 15 behaviors.

Gates:
  U1 CI six-suite battery: fortress(>=14G,0R) / disaster(>=15R,0G,
     sev3 set exact) / FIN-suppression / missing-data(0 verdicts) /
     sector-exact-ref / negative-multiple guard
  U2 AAPL live v15: verdicts present, zero null-val verdicts, EVERY
     sector-basis ref equals the served sector-medians value exactly
  U3 JPM live: fin_suppressed non-empty AND no suppressed-rule keys
     appear in greens/reds (doctrine on real data)
  U4 served surfaces: flagship (ops3495, fgverd, counts) + module
     (jhfgVerd, data-vk) + priors + 4-surface node-check
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

SUPPRESSED_KEYS = {"altman_z", "beneish_m", "sloan_accruals_pct", "roic_pct",
                   "income_quality", "interest_coverage_ttm",
                   "netdebt_to_ebitda_ttm", "current_ratio", "fcf_margin_pct",
                   "gross_margin_pct", "operating_margin_pct", "dso_days"}


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3496"})
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


def stepped(a, b, n=14):
    return [["2024-%02d-01" % (i % 12 + 1), a if i < n - 1 else b]
            for i in range(n)]


with report("3496_verdict_layer") as rep:
    out = {"ops": 3496, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:460]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:420]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3496 — green/red verdict layer")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        LB = 4
        FORT = {"roic_pct": flat(24), "gross_margin_pct": stepped(58.5, 62),
                "operating_margin_pct": stepped(28, 32),
                "fcf_margin_pct": flat(26), "fcf_yield_pct": flat(7),
                "income_quality": flat(1.4), "piotroski_f": flat(8),
                "altman_z": flat(6), "interest_coverage_ttm": flat(30),
                "revenue_cagr_3y_pct": flat(15), "eps_cagr_3y_pct": flat(18),
                "fcf_cagr_3y_pct": flat(16),
                "net_shareholder_yield_pct": flat(5),
                "beneish_m": flat(-2.9), "sloan_accruals_pct": flat(1.5),
                "netdebt_to_ebitda_ttm": flat(-0.5),
                "sbc_to_revenue_pct": flat(1.2), "current_ratio": flat(2.8),
                "dso_days": stepped(50, 42)}
        DIS = {"roic_pct": flat(3), "gross_margin_pct": stepped(30, 18),
               "operating_margin_pct": stepped(9, 3),
               "fcf_margin_pct": flat(-3), "fcf_yield_pct": flat(0.2),
               "income_quality": flat(0.5), "piotroski_f": flat(2),
               "altman_z": flat(1.2), "interest_coverage_ttm": flat(1.4),
               "revenue_cagr_3y_pct": flat(-2), "eps_cagr_3y_pct": flat(-12),
               "fcf_cagr_3y_pct": flat(-15),
               "net_shareholder_yield_pct": flat(-4),
               "beneish_m": flat(-1.2), "sloan_accruals_pct": flat(13),
               "netdebt_to_ebitda_ttm": flat(6),
               "sbc_to_revenue_pct": flat(14), "current_ratio": flat(0.8),
               "dso_days": stepped(50, 64)}
        r1 = m.derive_verdicts(FORT, LB, "Technology", {})
        t_f = (r1["summary"]["n_red"] == 0 and r1["summary"]["n_green"] >= 14
               and {"roic_pct", "altman_z", "beneish_m"}
               <= {x["k"] for x in r1["greens"]})
        r2 = m.derive_verdicts(DIS, LB, "Technology", {})
        sev3 = {x["k"] for x in r2["reds"] if x["sev"] == 3}
        t_d = (r2["summary"]["n_green"] == 0
               and r2["summary"]["n_red"] >= 15
               and {"beneish_m", "altman_z", "interest_coverage_ttm",
                    "fcf_margin_pct"} <= sev3)
        r3 = m.derive_verdicts(DIS, LB, "Real Estate", {"pe_ttm": 30})
        allk3 = {x["k"] for x in r3["greens"] + r3["reds"]}
        t_fin = (not (allk3 & SUPPRESSED_KEYS)
                 and len(r3["summary"]["fin_suppressed"]) >= 12)
        r4 = m.derive_verdicts({}, LB, "Technology", {"pe_ttm": 30})
        t_md = (r4["summary"]["n_green"] == 0
                and r4["summary"]["n_red"] == 0)
        r5 = m.derive_verdicts({"pe_ttm": flat(15), "peg_ttm": flat(-1)},
                               LB, "Technology",
                               {"pe_ttm": 30.0, "peg_ttm": 2.0})
        g5 = {x["k"]: x for x in r5["greens"]}
        t_sec = (g5.get("pe_ttm", {}).get("ref") == 30.0
                 and "peg_ttm" not in
                 {x["k"] for x in r5["greens"] + r5["reds"]})
        r6 = m.derive_verdicts({"pe_ttm": flat(60)}, LB, "Technology",
                               {"pe_ttm": 30.0})
        t_rich = any(x["k"] == "pe_ttm" and x["side"] == "R"
                     and "+100%" in x["why"] for x in r6["reds"])
        gate("U1_six_suites",
             all([t_f, t_d, t_fin, t_md, t_sec, t_rich]),
             {"fortress": t_f, "disaster": t_d, "fin": t_fin,
              "missing": t_md, "sector": t_sec, "rich": t_rich,
              "sev3": sorted(sev3)})
    except Exception as e:  # noqa: BLE001
        gate("U1_six_suites", False, str(e)[:300])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.5.0 verdict layer (ops 3496)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL", "CHTR", "JPM"], "periods": ["quarter"],
         "refresh": True}).encode())

    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v15.json")["Body"].read())
        V = doc.get("verdicts") or {}
        allv = (V.get("greens") or []) + (V.get("reds") or [])
        sm = json.loads(fetch(
            "https://justhodl.ai/data/fundgraph/sector-medians.json"))
        med = (sm.get("sectors") or {}).get(
            (doc.get("profile") or {}).get("sector") or "", {})
        ref_ok = all(abs((x.get("ref") or 0) - med.get(x["k"], -9e9)) < 1e-6
                     for x in allv if x.get("basis") == "sector")
        gate("U2_aapl_live",
             len(allv) > 0 and all(x.get("val") is not None for x in allv)
             and V["summary"]["n_green"] > 0 and ref_ok,
             {"n_green": V["summary"]["n_green"],
              "n_red": V["summary"]["n_red"],
              "sector_refs_exact": ref_ok,
              "top_green": (V.get("greens") or [{}])[0].get("why"),
              "top_red": (V.get("reds") or [{}])[0].get("why")
              if V.get("reds") else None})
    except Exception as e:  # noqa: BLE001
        gate("U2_aapl_live", False, str(e)[:280])

    try:
        docj = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/JPM_quarter_v15.json")["Body"].read())
        Vj = docj.get("verdicts") or {}
        allkj = {x["k"] for x in
                 (Vj.get("greens") or []) + (Vj.get("reds") or [])}
        gate("U3_jpm_suppression",
             len(Vj.get("summary", {}).get("fin_suppressed") or []) >= 12
             and not (allkj & SUPPRESSED_KEYS),
             {"sector": (docj.get("profile") or {}).get("sector"),
              "suppressed_n": len(Vj["summary"]["fin_suppressed"]),
              "leaked": sorted(allkj & SUPPRESSED_KEYS),
              "n_green": Vj["summary"].get("n_green"),
              "n_red": Vj["summary"].get("n_red")})
    except Exception as e:  # noqa: BLE001
        gate("U3_jpm_suppression", False, str(e)[:280])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"ops3495" in got["flag"] and b"jhfgVerd" in got["why"]:
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
          "flag": b"ops3495" in f and b"fgverd" in f and b"n_green" in f,
          "why": b"jhfgVerd" in y and b"data-vk" in y,
          "priors": b"secbtn" in f and b"mxbtn" in f and b"rtbtn" in f
          and b"ops3475" in y and b"jhfgSec" in y}
    gate("U4_surfaces", all(d4.values()), d4)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3496.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
