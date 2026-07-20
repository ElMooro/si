"""ops 3566 — TV-parity CLOSE. Sweep #2 should be complete: fresh
aggregate, then the full-fleet gate (raw 30 + stats 44 at >=460
coverage), spot proofs from the MATRIX, and the coverage table."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 0}))
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

with report("3566_parity_close") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:760]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    lam.invoke(FunctionName="justhodl-fundamental-census",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]
    N = len(MX["tickers"])
    idx = {t: i for i, t in enumerate(MX["tickers"])}
    nn = lambda k: sum(1 for v in C.get(k) or []
                       if isinstance(v, (int, float)))
    raw_cov = {k: nn(k) for k in RAW30}
    stat_cov = {k: nn(k) for k in STATK}
    raw_ok = sum(1 for v in raw_cov.values() if v >= 400)
    stat_ok = sum(1 for v in stat_cov.values() if v >= 400)
    partial = min(raw_cov.values())
    gate("J1_fleet", raw_ok >= 24 and stat_ok >= 36,
         {"raw_cols>=400names": raw_ok, "stats_cols>=400names":
          stat_ok, "min_raw_coverage": partial,
          "metrics_total": len(MX.get("metrics") or []),
          "generated_at": MX.get("generated_at"),
          "low_coverage": sorted(
              [(k, v) for k, v in {**raw_cov, **stat_cov}.items()
               if v < 400], key=lambda x: x[1])[:10]})
    g = lambda t, k: (C.get(k) or [None]*N)[idx.get(t, 0)]
    spots = {"AAPL_price_to_book": g("AAPL", "price_to_book"),
             "MSFT_roa_pct": g("MSFT", "roa_pct"),
             "NVDA_pe_fwd": g("NVDA", "pe_fwd"),
             "WMT_days_inventory": g("WMT", "days_inventory"),
             "AAPL_netChangeInCash": g("AAPL", "netChangeInCash"),
             "JNJ_payout_ratio_pct": g("JNJ", "payout_ratio_pct"),
             "XOM_fulmer_h": g("XOM", "fulmer_h"),
             "COST_graham_number": g("COST", "graham_number")}
    gate("J2_spots", sum(1 for v in spots.values()
                         if isinstance(v, (int, float))) >= 7, spots)
    bb = sorted([(MX["tickers"][i], v) for i, v in
                 enumerate(C.get("pe_fwd") or [])
                 if isinstance(v, (int, float)) and v > 0],
                key=lambda x: x[1])
    gate("J3_fwd_board", len(bb) >= 350,
         {"n_pe_fwd": len(bb), "cheapest_fwd": bb[:8],
          "richest_fwd": bb[-5:]})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3566.json").write_text(
        json.dumps({"ops": 3566, "fails": fails}))
sys.exit(0)
