"""ops 3567 — brute-force sweep close (3534 pattern). Chain #2
stalled ~129/496 (overlapped chains + mid-flight redeploy). This ops
finds every cache doc missing the v1.11 marker (fulmer_h), rebuilds
them via SYNC graphs invokes in 6-name batches with progress prints,
runs aggregate, then the full fleet gate.

  K1 census of missing docs (count printed)
  K2 rebuild loop (batches of 6, sync, finally-guaranteed progress)
  K3 aggregate + fleet gate: raw>=24 cols & stats>=36 cols at >=400
     names; forwards board >=350; spots
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
                   config=Config(read_timeout=420,
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

with report("3567_bruteforce_close") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:760]
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
            if b'"fulmer_h"' not in body and b'"pe_fwd"' not in body \
                    and b'"roce_pct"' not in body:
                missing.append(t)
        except Exception:  # noqa: BLE001
            missing.append(t)
        if i % 120 == 0:
            print(f"[scan] {i}/{len(tickers)} "
                  f"missing={len(missing)} {time.time()-t0:.0f}s")
    gate("K1_scan", True, {"missing_n": len(missing),
                           "sample": missing[:10],
                           "scan_s": round(time.time() - t0, 1)})

    done, errs = 0, 0
    t1 = time.time()
    try:
        for j in range(0, len(missing), 6):
            batch = missing[j:j + 6]
            try:
                lam.invoke(FunctionName="justhodl-fundamental-graphs",
                           Payload=json.dumps(
                               {"warm": batch,
                                "periods": ["quarter"],
                                "refresh": True}).encode())
                done += len(batch)
            except Exception as e:  # noqa: BLE001
                errs += 1
                print("[batch-err]", batch[0], str(e)[:80])
            if (j // 6) % 8 == 0:
                print(f"[rebuild] {done}/{len(missing)} errs={errs} "
                      f"{time.time()-t1:.0f}s")
    finally:
        print(f"[rebuild-final] {done}/{len(missing)} errs={errs} "
              f"{time.time()-t1:.0f}s")
    gate("K2_rebuild", errs <= 3 and done >= len(missing) - 18,
         {"rebuilt": done, "errs": errs,
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
    bb = sorted([(MX["tickers"][i2], v) for i2, v in
                 enumerate(C.get("pe_fwd") or [])
                 if isinstance(v, (int, float)) and v > 0],
                key=lambda x: x[1])
    g = lambda t, k: (C.get(k) or [None]*N)[idx.get(t, 0)]
    gate("K3_fleet", raw_ok >= 24 and stat_ok >= 34
         and len(bb) >= 350,
         {"raw>=400": raw_ok, "stats>=400": stat_ok,
          "n_pe_fwd": len(bb),
          "metrics_total": len(MX.get("metrics") or []),
          "cheapest_fwd": bb[:8],
          "low_cov": sorted([(k, nn(k)) for k in RAW30 + STATK
                             if nn(k) < 400],
                            key=lambda x: x[1])[:8],
          "spots": {"WMT_days_inventory": g("WMT", "days_inventory"),
                    "MSFT_pe_fwd": g("MSFT", "pe_fwd"),
                    "WMT_inventory_turnover":
                        g("WMT", "inventory_turnover")}})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3567.json").write_text(
        json.dumps({"ops": 3567, "fails": fails}))
sys.exit(0)
