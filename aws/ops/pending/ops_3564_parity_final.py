"""ops 3564 — TV-PARITY FINAL GATE + honest checklist. After chain
#2 (stats sweep), the matrix must carry the raw items AND the
Statistics suite; prints the coverage table Khalid can audit:
DIRECT (in matrix) / AGGREGATE-MAPPED / NOT-IN-SOURCE (FMP doesn't
publish; never synthesized).

  H1 matrix: >=22/30 raw cols AND >=34/44 stat cols (n>=50 names
     each); metrics_total printed; category census of the dropdown
  H2 TV checklist verdict table (counts + the honest ✖ list)
  H3 spot values: AAPL P/B + MSFT ROA + NVDA pe_fwd from the MATRIX
     (not the doc) — proof the sweep landed fleet-wide
"""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
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
NOT_IN_SOURCE = ["PP&E by class (buildings/machinery/land/leases/…)",
                 "Accumulated depreciation by class",
                 "Inventory WIP / finished / raw splits",
                 "Receivables gross / bad-debt split",
                 "Income tax current-vs-deferred domestic/foreign",
                 "Interest capitalized", "Notes payable",
                 "Accrued payroll", "Dividends payable",
                 "Separate impairment/restructuring/legal lines",
                 "Free float", "Preferred dividends paid (separate)"]

with report("3564_parity_final") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:760]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]
    N = len(MX["tickers"])
    idx = {t: i for i, t in enumerate(MX["tickers"])}
    nn = lambda k: sum(1 for v in C.get(k) or []
                       if isinstance(v, (int, float)))
    raw_ok = sum(1 for k in RAW30 if nn(k) >= 50)
    stat_ok = sum(1 for k in STATK if nn(k) >= 50)
    gate("H1_matrix", raw_ok >= 22 and stat_ok >= 34,
         {"raw_present": raw_ok, "stat_present": stat_ok,
          "metrics_total": len(MX.get("metrics") or []),
          "generated_at": MX.get("generated_at"),
          "raw_missing": [k for k in RAW30 if nn(k) < 50][:8],
          "stat_missing": [k for k in STATK if nn(k) < 50][:8]})

    gate("H2_checklist", True,
         {"DIRECT_raw": raw_ok, "DIRECT_stats": stat_ok,
          "AGGREGATE_MAPPED": ["EBIT→operating income",
                               "impairment/restructuring→other "
                               "expenses & non-op total",
                               "treasury stock→other equity",
                               "buyback yield≈net_buyback_yield "
                               "(+gross added)"],
          "NOT_IN_SOURCE_never_synthesized": NOT_IN_SOURCE})

    g = lambda t, k: (C.get(k) or [None]*N)[idx[t]] if t in idx \
        else None
    spots = {"AAPL_price_to_book": g("AAPL", "price_to_book"),
             "MSFT_roa_pct": g("MSFT", "roa_pct"),
             "NVDA_pe_fwd": g("NVDA", "pe_fwd"),
             "AAPL_netChangeInCash": g("AAPL", "netChangeInCash"),
             "JNJ_payout_ratio_pct": g("JNJ", "payout_ratio_pct")}
    gate("H3_spots", all(isinstance(v, (int, float))
                         for v in spots.values()), spots)

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3564.json").write_text(
        json.dumps({"ops": 3564, "fails": fails}))
sys.exit(0)
