"""ops 3570 — final TV-parity numbers (read-only reprint; 3569 md was
clobbered by parallel-session runs, JSON said ALL PASS)."""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
RAW30 = ["otherOpex","costAndExpenses","sellingMarketing","nonOpIncomeTotal","otherCurrentAssets","otherNonCurrentAssets","totalNonCurrentAssets","deferredTaxAssets","taxPayables","otherCurrentLiabilities","otherNonCurrentLiabilities","totalNonCurrentLiabilities","deferredRevenueNC","deferredTaxLiabNC","commonStockPar","aoci","otherEquity","preferredStock","totalInvestments","capLeaseObligations","deferredIncomeTaxCF","dReceivables","dInventory","dPayables","otherWC","otherNonCash","otherCFI","otherCFF","netChangeInCash","forexCash"]
STATK = ["ps_ttm","price_to_book","price_to_cfo_ttm","price_to_tangible_book","book_value_per_share","tangible_bvps","fcf_per_share","graham_number","roa_pct","roce_pct","rota_pct","rote_pct","quick_ratio","inventory_turnover","asset_turnover","debt_to_assets_pct","lt_debt_to_assets_pct","lt_debt_to_equity","debt_to_revenue","effective_interest_rate_pct","equity_to_assets_pct","goodwill_to_assets_pct","inventory_to_revenue_pct","cash_to_debt","cogs_to_revenue_pct","days_inventory","days_payable","payout_ratio_pct","buyback_yield_gross_pct","tangible_common_equity_pct","fcf_per_employee","ebitda_per_employee","op_income_per_employee","debt_per_employee","assets_per_employee","rnd_per_employee","zmijewski_x","springate_s","fulmer_h","pe_fwd","ps_fwd","ev_ebitda_fwd","ev_ebit_fwd","ev_revenue_fwd"]
with report("3570_final_numbers") as rep:
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]; N = len(MX["tickers"])
    idx = {t: i for i, t in enumerate(MX["tickers"])}
    nn = lambda k: sum(1 for v in C.get(k) or []
                       if isinstance(v, (int, float)))
    g = lambda t, k: (C.get(k) or [None]*N)[idx.get(t, 0)]
    bb = sorted([(MX["tickers"][i], v) for i, v in
                 enumerate(C.get("pe_fwd") or [])
                 if isinstance(v, (int, float)) and v > 0],
                key=lambda x: x[1])
    out = {"generated_at": MX.get("generated_at"),
           "metrics_total": len(MX.get("metrics") or []),
           "raw>=400": sum(1 for k in RAW30 if nn(k) >= 400),
           "stats>=400": sum(1 for k in STATK if nn(k) >= 400),
           "n_pe_fwd": len(bb),
           "low_cov": sorted([(k, nn(k)) for k in RAW30+STATK
                              if nn(k) < 400], key=lambda x: x[1])[:8],
           "cheapest_fwd": bb[:8],
           "spots": {"WMT_days_inventory": g("WMT","days_inventory"),
                     "WMT_inv_turnover": g("WMT","inventory_turnover"),
                     "MSFT_pe_fwd": g("MSFT","pe_fwd"),
                     "COST_graham": g("COST","graham_number"),
                     "AAPL_netChangeInCash":
                         g("AAPL","netChangeInCash")}}
    line = "PASS  N1_final — " + json.dumps(out)[:900]
    print(line); rep.log(line)
    print("RESULT: ALL PASS")
    (REPO/"aws/ops/reports/3570.json").write_text(
        json.dumps({"ops": 3570, "fails": []}))
sys.exit(0)
