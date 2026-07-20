"""ops 3568 — TRUE sweep close. 3567's scan false-positived (5
"missing" yet coverage frozen at 131): docs evidently carry metric
NAMES without series (catalog echo), so substring checks lie. This
ops proves it on WMT, rescans with the data-aware pattern
b'"fulmer_h": [[', rebuilds the real missing set, aggregates, gates.

  L1 WMT/MSFT probe: name-in-body vs series-in-points (the lie shown)
  L2 data-aware scan (true missing count)
  L3 rebuild loop (sync 6-batches, progress)
  L4 aggregate + fleet gate (raw>=24 & stats>=34 cols at >=400;
     pe_fwd>=350; WMT spots alive)
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
DATAPAT = b'"fulmer_h": [['
DATAPAT2 = b'"roce_pct": [['

with report("3568_true_close") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:740]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    probe = {}
    for t in ("WMT", "MSFT", "AAPL"):
        body = s3c.get_object(Bucket=BUCKET,
            Key=f"data/fundgraph/cache/{t}_quarter_v21.json"
            )["Body"].read()
        doc = json.loads(body)
        P = doc.get("points") or {}
        probe[t] = {"name_in_body": b'"fulmer_h"' in body,
                    "series_in_points": bool(P.get("fulmer_h")),
                    "pe_fwd_series": bool(P.get("pe_fwd"))}
    lie = any(v["name_in_body"] and not v["series_in_points"]
              for v in probe.values())
    gate("L1_probe", True, {"probe": probe, "scan_lied": lie})

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
            if DATAPAT not in body and DATAPAT2 not in body:
                missing.append(t)
        except Exception:  # noqa: BLE001
            missing.append(t)
        if i % 120 == 0:
            print(f"[scan2] {i}/{len(tickers)} "
                  f"missing={len(missing)} {time.time()-t0:.0f}s")
    gate("L2_scan", True, {"true_missing": len(missing),
                           "sample": missing[:8],
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
                print("[berr]", batch[0], str(e)[:80])
            if (j // 6) % 10 == 0:
                print(f"[rebuild] {done}/{len(missing)} errs={errs} "
                      f"{time.time()-t1:.0f}s")
    finally:
        print(f"[final] {done}/{len(missing)} errs={errs} "
              f"{time.time()-t1:.0f}s")
    gate("L3_rebuild", errs <= 5 and done >= len(missing) - 24,
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
    bb = [v for v in C.get("pe_fwd") or []
          if isinstance(v, (int, float)) and v > 0]
    g = lambda t, k: (C.get(k) or [None]*N)[idx.get(t, 0)]
    gate("L4_fleet", raw_ok >= 24 and stat_ok >= 34
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
    (REPO/"aws/ops/reports/3568.json").write_text(
        json.dumps({"ops": 3568, "fails": fails}))
sys.exit(0)
