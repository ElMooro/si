"""ops 3517 — full-history close: price stitcher (4 windows under the
/light ~5k-row cap; 3516 probe proved the cap) + stmt_rows transparency
in the doc. Rewarm AAPL/PG on v21 and gate depth for real.

  Q1 AAPL: revenue >=140 pts oldest <1995; px oldest <1985 with
     >=2500 weekly pts; stmt_rows printed
  Q2 PG: revenue >=150 oldest <1990; px oldest <1985
"""
import json, sys, time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"; BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")
with report("3517_history_close") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:480]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    rep.heading("ops 3517 — price stitcher + depth regate")
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                            "S3_BUCKET": BUCKET, "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="v1.11.1 price stitch + stmt_rows (ops 3517)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL", "PG"], "periods": ["quarter"], "refresh": True}).encode())
    for sym, gn, rmin, rbefore in (("AAPL","Q1_aapl",140,"1995"),
                                   ("PG","Q2_pg",150,"1990")):
        try:
            body = s3c.get_object(Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v21.json")["Body"].read()
            doc = json.loads(body)
            rev = doc["points"].get("revenue") or []
            px = doc.get("price") or []
            gate(gn, len(rev) >= rmin and rev[0][0] < rbefore
                 and len(px) >= 2500 and px[0][0] < "1985",
                 {"n_revenue": len(rev), "oldest_revenue": rev[0][0] if rev else None,
                  "n_px_weeks": len(px), "oldest_px": px[0][0] if px else None,
                  "stmt_rows": doc.get("stmt_rows"),
                  "doc_kb": len(body)//1024, "ver": doc.get("version")})
        except Exception as e:
            gate(gn, False, str(e)[:300])
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3517.json").write_text(json.dumps({"ops":3517,"fails":fails}))
sys.exit(0)
