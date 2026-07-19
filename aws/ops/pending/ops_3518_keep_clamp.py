"""ops 3518 — the last full-history clamp: keep_from used MAX_Q=44/
MAX_A=14 (found at line 888 after stmt_rows transparency proved 163
rows reached the build). v1.11.2 lifts to 220/65. Rewarm + final depth
gates + a range-filter sanity: MAX view rows == n_revenue.
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
with report("3518_keep_clamp") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:460]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    rep.heading("ops 3518 — MAX_Q 44->220 (full statements)")
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                            "S3_BUCKET": BUCKET, "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="v1.11.2 keep-clamp 220/65 (ops 3518)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["AAPL", "PG", "NVDA"], "periods": ["quarter"],
         "refresh": True}).encode())
    for sym, gn, rmin, rbefore in (("AAPL","R1_aapl",150,"1990"),
                                   ("PG","R2_pg",150,"1990"),
                                   ("NVDA","R3_nvda",100,"2003")):
        try:
            body = s3c.get_object(Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v21.json")["Body"].read()
            doc = json.loads(body)
            rev = doc["points"].get("revenue") or []
            px = doc.get("price") or []
            V = doc.get("verdicts") or {}
            gate(gn, len(rev) >= rmin and rev[0][0] < rbefore
                 and len(px) >= 1500,
                 {"n_revenue": len(rev), "oldest_revenue": rev[0][0],
                  "oldest_px": px[0][0], "n_px_weeks": len(px),
                  "doc_kb": len(body)//1024,
                  "n_green": (V.get("summary") or {}).get("n_green"),
                  "stmt_rows": doc.get("stmt_rows")})
        except Exception as e:
            gate(gn, False, str(e)[:300])
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3518.json").write_text(json.dumps({"ops":3518,"fails":fails}))
sys.exit(0)
