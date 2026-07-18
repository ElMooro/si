"""ops 3465 — Fundamental Graphs metric-library expansion.

Engine v1.1.0 (200+ series: Growth/CAGR family, HF quality + capital-return
set, full credit block, six distress scores + Tobin's Q + KZ, per-employee)
and page v1.2 (Favorites tab, curated Institutional tab with HF badges,
per-metric ★ favorites and color tags). Cache keys bumped to _v11.

Gates:
  M1  deploy v1.1.0 + warm refresh 6/6 (CHTR/AAPL/MSFT × Q+A), keys>=190
  M2  AAPL hard set — 26 new institutional keys present with history
  M3  soft coverage log (employees / peg / kz / fulmer / rule_of_40)
  M4  page v1.2 live: ops3465 marker + Institutional tab + FAV store
"""
import json
import sys
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
s3 = boto3.client("s3", region_name="us-east-1")

HARD = ["revenue_yoy_pct", "eps_yoy_pct", "fcf_yoy_pct", "revenue_cagr_3y_pct",
        "revenue_cagr_5y_pct", "gp_to_assets_pct", "ev_gp_ttm",
        "earnings_yield_ebit_pct", "roc_greenblatt_pct", "fcf_conversion_pct",
        "cash_conversion_pct", "capex_to_da", "net_buyback_ttm",
        "net_buyback_yield_pct", "net_shareholder_yield_pct", "total_yield_pct",
        "tobins_q", "equity_multiplier", "debt_to_capital",
        "gross_debt_to_ebitda", "ebitda_interest_coverage", "fcf_to_debt_pct",
        "altman_z_prime", "springate", "zmijewski_x",
        "goodwill_to_assets_pct", "sustainable_growth_pct", "ncav_ps"]
SOFT = ["employees", "revenue_per_employee", "peg_ttm", "kz_index",
        "fulmer_h", "rule_of_40", "dps_yoy_pct", "retention_pct"]

with report("3465_fundgraph_metric_expansion") as rep:
    out = {"ops": 3465, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3465 — 200-metric library + Favorites/Institutional picker")

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=120, memory=512,
        description="Fundamental Graphs API v1.1.0 — 200+ series (ops 3465)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)

    warm = lam.invoke(
        FunctionName=FN,
        Payload=json.dumps({"warm": ["CHTR", "AAPL", "MSFT"],
                            "periods": ["quarter", "annual"],
                            "refresh": True}).encode(),
    )
    wp = json.loads(warm["Payload"].read() or b"{}")
    wd = wp.get("warmed") or {}
    m1 = (wp.get("version") == "1.1.0"
          and all(v.get("ok") for v in wd.values()) and len(wd) == 6
          and all(v.get("keys", 0) >= 190 for k, v in wd.items()
                  if k.endswith("_quarter")))
    gate("M1_deploy_warm_v110", m1, {"version": wp.get("version"), "warmed": wd})

    try:
        doc = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v11.json")["Body"].read())
        pts = doc.get("points", {})
        missing = [k for k in HARD if not pts.get(k)]
        thin = [k for k in HARD if pts.get(k) and len(pts[k]) < 8]
        gate("M2_hard_institutional_set", not missing and not thin,
             {"catalog_n": doc.get("catalog_n"), "missing": missing,
              "thin": thin,
              "rule_of_40_last": (pts.get("rule_of_40") or [[None, None]])[-1],
              "roic_last": (pts.get("roic_pct") or [[None, None]])[-1]})
        soft_cov = {k: len(pts.get(k) or []) for k in SOFT}
        gate("M3_soft_coverage_log", True, soft_cov)
    except Exception as e:  # noqa: BLE001
        gate("M2_hard_institutional_set", False, str(e)[:220])
        gate("M3_soft_coverage_log", True, "skipped")

    page_ok, det = False, {}
    for _ in range(21):
        try:
            req = urllib.request.Request(
                f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}",
                headers={"User-Agent": "ops-3465"})
            with urllib.request.urlopen(req, timeout=30) as r:
                b = r.read()
            page_ok = (b"ops3465" in b and b"Institutional" in b
                       and b"jh_fg_favm" in b and b"jh_fg_metc" in b)
            det = {"status": 200, "markers": page_ok}
        except Exception as e:  # noqa: BLE001
            det = {"err": str(e)[:120]}
        if page_ok:
            break
        time.sleep(20)
    gate("M4_page_v12_live", page_ok, det)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3465.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
