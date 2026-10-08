"""ops 3561 — TV METRIC PARITY, phase 1 (Khalid: every TradingView
statement metric in the census). Fundamental-graphs +30 raw FMP
pass-through line items (other opex, S&M, non-op income total, other/
total non-current assets & liabilities, deferred tax assets/
liabilities, tax payable, deferred revenue NC, common-stock par,
AOCI, other equity, preferred stock, total investments, cap-lease
obligations, CF deltas AR/Inv/AP/other-WC, other non-cash, other
CFI/CFF, net change in cash, FX effect). Census col floor 0.25 /
cap 300 so sparse-but-real raw items ride. fg-catalog + page labels
+30. TV items FMP does not publish (PP&E/inventory/accum-dep class
breakdowns, domestic-vs-foreign tax splits, interest capitalized,
notes payable, accrued payroll, dividends payable, separate
impairment/restructuring/legal lines) will be listed honestly in the
phase-2 checklist — never synthesized.

  F1 deploy graphs + census (zip markers forexCash / "1.10.0")
  F2 AAPL smoke: sync warm {refresh} then cache doc points contains
     >=24 of the 30 new keys (per-key presence printed)
  F3 kick FULL refresh chain ({"phase":"warm","cursor":0,
     "refresh":true}) — 496 docs rebuild ≈25 min, aggregate auto;
     phase-2 ops gates the matrix + prints the TV checklist
"""
import io, json, sys, time, urllib.request, zipfile
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
NEWK = ["otherOpex", "costAndExpenses", "sellingMarketing",
        "nonOpIncomeTotal", "otherCurrentAssets",
        "otherNonCurrentAssets", "totalNonCurrentAssets",
        "deferredTaxAssets", "taxPayables", "otherCurrentLiabilities",
        "otherNonCurrentLiabilities", "totalNonCurrentLiabilities",
        "deferredRevenueNC", "deferredTaxLiabNC", "commonStockPar",
        "aoci", "otherEquity", "preferredStock", "totalInvestments",
        "capLeaseObligations", "deferredIncomeTaxCF", "dReceivables",
        "dInventory", "dPayables", "otherWC", "otherNonCash",
        "otherCFI", "otherCFF", "netChangeInCash", "forexCash"]

with report("3561_tv_parity") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:700]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    for fn, mark, tmo, mem in (
            ("justhodl-fundamental-graphs", b"forexCash", 300, 1024),
            ("justhodl-fundamental-census", b'"1.10.0"', 900, 1536)):
        cfg = lam.get_function_configuration(FunctionName=fn)
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=REPO/"aws"/"lambdas"/fn/"source",
                      env_vars=(cfg.get("Environment") or {})
                      .get("Variables") or {},
                      timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                      description=f"{fn} tv-parity (ops 3561)",
                      create_function_url=False, smoke=False)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
        src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(
            loc, timeout=60).read())).read("lambda_function.py")
        if mark not in src:
            gate(f"F1_zip_{fn}", False, "marker missing")

    try:
        lam.invoke(FunctionName="justhodl-fundamental-graphs",
                   Payload=json.dumps({"warm": ["AAPL"],
                                       "periods": ["quarter"],
                                       "refresh": True}).encode())
        time.sleep(2)
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/fundgraph/cache/AAPL_quarter_v21.json")
            ["Body"].read())
        P = doc.get("points") or {}
        have = {k: (k in P and len(P[k]) > 0) for k in NEWK}
        nh = sum(1 for v in have.values() if v)
        lv = {k: (P[k][-1][1] if have[k] else None)
              for k in ("costAndExpenses", "totalNonCurrentAssets",
                        "netChangeInCash", "nonOpIncomeTotal")}
        gate("F2_aapl", nh >= 24,
             {"present": nh, "of": len(NEWK),
              "missing": [k for k, v in have.items() if not v],
              "aapl_latest": lv})
    except Exception as e:
        gate("F2_aapl", False, str(e)[:340])

    try:
        lam.invoke(FunctionName="justhodl-fundamental-census",
                   InvocationType="Event",
                   Payload=json.dumps({"phase": "warm", "cursor": 0,
                                       "refresh": True}).encode())
        gate("F3_chain_kicked", True,
             "496-doc refresh chain running; aggregate auto at end")
    except Exception as e:
        gate("F3_chain_kicked", False, str(e)[:240])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3561.json").write_text(
        json.dumps({"ops": 3561, "fails": fails}))
sys.exit(0)
