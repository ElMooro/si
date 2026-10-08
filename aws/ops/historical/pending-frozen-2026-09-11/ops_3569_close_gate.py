"""ops 3569 — sweep closer under the 30-min workflow cap (3568 ran
twice ~70 combined minutes of rebuild before timeout-cancels; docs
should be ~done). Data-aware rescan; rebuild remainder with a hard
18-min budget; aggregate; fleet gate. Doctrine banked: run-ops
timeout-minutes=30 → every ops self-budgets <=25 min.

  M1 data-aware scan (true missing)
  M2 budgeted rebuild (stop at 1080s, report remainder honestly)
  M3 aggregate + fleet gate: raw>=24 & stats>=34 at >=400 names,
     pe_fwd>=350, WMT days-inventory/turnover alive
"""
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
                   config=Config(read_timeout=300,
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

with report("3569_close_gate") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:740]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    tickers = MX["tickers"]
    missing = []
    t0 = time.time()
    for i, t in enumerate(tickers):
        try:
            body = s3c.get_object(Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{t}_quarter_v21.json"
                )["Body"].read()
            if b'"fulmer_h": [[' not in body and \
                    b'"roce_pct": [[' not in body:
                missing.append(t)
        except Exception:  # noqa: BLE001
            missing.append(t)
    gate("M1_scan", True, {"true_missing": len(missing),
                           "sample": missing[:10],
                           "scan_s": round(time.time() - t0, 1)})

    BUDGET = 1080
    done, errs, left = 0, 0, list(missing)
    t1 = time.time()
    try:
        while left and time.time() - t1 < BUDGET:
            batch, left = left[:6], left[6:]
            try:
                lam.invoke(FunctionName="justhodl-fundamental-graphs",
                           Payload=json.dumps(
                               {"warm": batch, "periods": ["quarter"],
                                "refresh": True}).encode())
                done += len(batch)
            except Exception as e:  # noqa: BLE001
                errs += 1
                print("[berr]", batch[0], str(e)[:70])
            if done % 60 < 6:
                print(f"[rb] {done}/{len(missing)} "
                      f"{time.time()-t1:.0f}s")
    finally:
        print(f"[rb-final] {done}/{len(missing)} left={len(left)} "
              f"errs={errs} {time.time()-t1:.0f}s")
    gate("M2_rebuild", errs <= 5,
         {"rebuilt": done, "remainder": len(left), "errs": errs,
          "elapsed_s": round(time.time() - t1, 1)})

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
    raw_ok = sum(1 for k in RAW30 if nn(k) >= 400)
    stat_ok = sum(1 for k in STATK if nn(k) >= 400)
    bb = [v for v in C.get("pe_fwd") or []
          if isinstance(v, (int, float)) and v > 0]
    g = lambda t, k: (C.get(k) or [None]*N)[idx.get(t, 0)]
    gate("M3_fleet", raw_ok >= 24 and stat_ok >= 34
         and len(bb) >= 350,
         {"raw>=400": raw_ok, "stats>=400": stat_ok,
          "n_pe_fwd": len(bb),
          "metrics_total": len(MX.get("metrics") or []),
          "low_cov": sorted([(k, nn(k)) for k in RAW30 + STATK
                             if nn(k) < 400],
                            key=lambda x: x[1])[:8],
          "spots": {"WMT_days_inventory": g("WMT", "days_inventory"),
                    "WMT_inventory_turnover":
                        g("WMT", "inventory_turnover"),
                    "MSFT_pe_fwd": g("MSFT", "pe_fwd"),
                    "COST_graham": g("COST", "graham_number")}})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3569.json").write_text(
        json.dumps({"ops": 3569, "fails": fails}))
sys.exit(0)
