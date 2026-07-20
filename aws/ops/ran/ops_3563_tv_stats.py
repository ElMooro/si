"""ops 3563 — TV STATISTICS + FORECASTS parity + CATEGORIES (Khalid
screenshots round 2). Graphs v1.11.0 adds every derivable Statistics
metric: P/S, P/B, P/CF, P/tangible-book, BVPS + tangible BVPS, FCF/
share, Graham number, ROA/ROCE/ROTA/ROTE, quick ratio, inventory &
asset turnover, debt/assets (+LT), LT-debt/equity, debt/revenue,
effective interest rate, equity/assets, goodwill/assets, inventory/
revenue, cash/debt, COGS/revenue, days inventory + days payable,
payout ratio, gross buyback yield, tangible common equity, six
per-employee metrics (FCF/EBITDA/op-income/debt/assets/R&D — joining
the existing revenue+NI per employee), Zmijewski / Springate /
Fulmer distress scores, and FORWARDS from analyst estimates (P/E,
P/S, EV/EBITDA, EV/EBIT, EV/Revenue on NTM sums). Metric Explorer on
all 3 pages now groups the dropdown by CATEGORY (Growth, Valuation,
Forward, Profitability, Solvency, Efficiency, Forensics, Technicals,
Ownership, Risk/R-R, Raw IS/BS/CF). fg-catalog +47.

  G1 PHASE-2 of 3561: matrix carries the raw items (>=22 of 30 new
     raw cols; metrics count printed) — the 496 chain's harvest
  G2 deploy graphs v1.11 (zip fulmer_h) + AAPL warm smoke: >=30 of
     the new stat keys present; AAPL P/B, ROA, days-inventory,
     Zmijewski, pe_fwd printed (real values)
  G3 kick refresh chain #2 (stats need doc rebuild)
  G4 pages x3 served with optgroup markers + node
"""
import io, json, re, subprocess, sys, tempfile, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")
RAW30 = ["otherOpex", "costAndExpenses", "sellingMarketing",
         "nonOpIncomeTotal", "otherCurrentAssets",
         "otherNonCurrentAssets", "totalNonCurrentAssets",
         "deferredTaxAssets", "taxPayables", "otherCurrentLiabilities",
         "otherNonCurrentLiabilities", "totalNonCurrentLiabilities",
         "deferredRevenueNC", "deferredTaxLiabNC", "commonStockPar",
         "aoci", "otherEquity", "preferredStock", "totalInvestments",
         "capLeaseObligations", "deferredIncomeTaxCF", "dReceivables",
         "dInventory", "dPayables", "otherWC", "otherNonCash",
         "otherCFI", "otherCFF", "netChangeInCash", "forexCash"]
STATK = ["ps_ttm", "price_to_book", "price_to_cfo_ttm",
         "price_to_tangible_book", "book_value_per_share",
         "tangible_bvps", "fcf_per_share", "graham_number", "roa_pct",
         "roce_pct", "rota_pct", "rote_pct", "quick_ratio",
         "inventory_turnover", "asset_turnover", "debt_to_assets_pct",
         "lt_debt_to_assets_pct", "lt_debt_to_equity",
         "debt_to_revenue", "effective_interest_rate_pct",
         "equity_to_assets_pct", "goodwill_to_assets_pct",
         "inventory_to_revenue_pct", "cash_to_debt",
         "cogs_to_revenue_pct", "days_inventory", "days_payable",
         "payout_ratio_pct", "buyback_yield_gross_pct",
         "tangible_common_equity_pct", "fcf_per_employee",
         "ebitda_per_employee", "op_income_per_employee",
         "debt_per_employee", "assets_per_employee",
         "rnd_per_employee", "zmijewski_x", "springate_s", "fulmer_h",
         "pe_fwd", "ps_fwd", "ev_ebitda_fwd", "ev_ebit_fwd",
         "ev_revenue_fwd"]


def fetch(url, t=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3563"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


with report("3563_tv_stats") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:700]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    try:
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        C = MX["cols"]
        nn = lambda k: sum(1 for v in C.get(k) or []
                           if isinstance(v, (int, float)))
        have = {k: nn(k) for k in RAW30}
        nh = sum(1 for v in have.values() if v >= 50)
        gate("G1_phase2_raw", nh >= 22,
             {"raw_cols_present": nh, "of": 30,
              "metrics_total": len(MX.get("metrics") or []),
              "sparse_examples": {k: have[k] for k in
                                  ("preferredStock",
                                   "capLeaseObligations",
                                   "netChangeInCash")}})
    except Exception as e:
        gate("G1_phase2_raw", False, str(e)[:300])

    fn = "justhodl-fundamental-graphs"
    for _ in range(30):
        c0 = lam.get_function_configuration(FunctionName=fn)
        if c0.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(4)
    cfg = lam.get_function_configuration(FunctionName=fn)
    for att in range(4):
        try:
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=REPO/"aws"/"lambdas"/fn/"source",
                          env_vars=(cfg.get("Environment") or {})
                          .get("Variables") or {},
                          timeout=cfg["Timeout"],
                          memory=cfg["MemorySize"],
                          description="graphs v1.11 tv-stats (3563)",
                          create_function_url=False, smoke=False)
            break
        except Exception as e:  # noqa: BLE001
            if "Conflict" in str(e):
                time.sleep(25)
            else:
                raise
    for _ in range(30):
        c0 = lam.get_function_configuration(FunctionName=fn)
        if c0.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(
        loc, timeout=60).read())).read("lambda_function.py")
    zok = b"fulmer_h" in src and b"pe_fwd" in src
    try:
        lam.invoke(FunctionName=fn,
                   Payload=json.dumps({"warm": ["AAPL"],
                                       "periods": ["quarter"],
                                       "refresh": True}).encode())
        time.sleep(2)
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v21.json")
            ["Body"].read())
        P = doc.get("points") or {}
        nh = sum(1 for k in STATK if P.get(k))
        lv = lambda k: (P.get(k) or [[None, None]])[-1][1]
        gate("G2_aapl_stats", zok and nh >= 30,
             {"zip": zok, "present": nh, "of": len(STATK),
              "missing": [k for k in STATK if not P.get(k)][:10],
              "aapl": {"price_to_book": lv("price_to_book"),
                       "roa_pct": lv("roa_pct"),
                       "days_inventory": lv("days_inventory"),
                       "zmijewski_x": lv("zmijewski_x"),
                       "graham_number": lv("graham_number"),
                       "pe_fwd": lv("pe_fwd"),
                       "ev_ebitda_fwd": lv("ev_ebitda_fwd")}})
    except Exception as e:
        gate("G2_aapl_stats", False, str(e)[:340])

    try:
        lam.invoke(FunctionName="justhodl-fundamental-census",
                   InvocationType="Event",
                   Payload=json.dumps({"phase": "warm", "cursor": 0,
                                       "refresh": True}).encode())
        gate("G3_chain2", True, "stats sweep running")
    except Exception as e:
        gate("G3_chain2", False, str(e)[:200])

    ok_p = True
    for pg, sid, mark in (("fundamental-census.html", "OPS3529",
                           b"optgroup"),
                          ("etf-census.html", "OPSPAGE", b"CATR"),
                          ("fixed-income-census.html", "OPSPAGE",
                           b"CATR")):
        pa = b""
        for _ in range(14):
            try:
                pa = fetch(f"https://justhodl.ai/{pg}?cb=%d"
                           % int(time.time()))
                if mark in pa:
                    break
            except Exception:
                pass
            time.sleep(20)
        mm = re.search(rb'<script id="' + sid.encode()
                       + rb'">\n([\s\S]*?)</script>', pa)
        node = False
        if mm:
            with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                             delete=False) as f2:
                f2.write(mm.group(1).replace(b"__BT_URL__",
                                             b"https://x"))
                pth = f2.name
            node = subprocess.run(["node", "--check", pth],
                                  capture_output=True).returncode == 0
        ok_p &= (mark in pa) and node
    gate("G4_pages", ok_p, "categories served + node x3")

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3563.json").write_text(
        json.dumps({"ops": 3563, "fails": fails}))
sys.exit(0)
