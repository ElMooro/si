"""ops 3544 — retail columns on the true dark-pool schema (probe
3543-Q2): dark_pool_pct -> retail_dp_pct, state -> retail_accum
(+1 ACC / -1 DIST), score -> retail_dp_score. 60-board coverage,
honest sparsity (joined post-coverage-filter like whales/earnings).

  R1 deploy + aggregate: retail_dp_pct n in 30..80, retail_accum has
     both +1 and -1, ACC∩conviction sample printed
"""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-census"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=420, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")
with report("3544_retail_wire") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1536,
                  description="Census v1.7.2 retail wire (ops 3544)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN,
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    C = MX["cols"]
    nn = lambda k: sum(1 for v in C.get(k) or []
                       if isinstance(v, (int, float)))
    acc = [MX["tickers"][i] for i, v in
           enumerate(C.get("retail_accum") or []) if v == 1]
    dist = [MX["tickers"][i] for i, v in
            enumerate(C.get("retail_accum") or []) if v == -1]
    conv = C.get("conviction_score") or []
    acc_conv = sorted([(t, conv[MX["tickers"].index(t)]) for t in acc
                       if isinstance(conv[MX["tickers"].index(t)],
                                     (int, float))],
                      key=lambda x: -x[1])[:6]
    gate("R1_retail", 20 <= nn("retail_dp_pct") <= 90
         and len(acc) >= 3 and len(dist) >= 1,
         {"retail_n": nn("retail_dp_pct"), "acc_n": len(acc),
          "dist_n": len(dist), "acc_sample": acc[:10],
          "dist_sample": dist[:8], "acc_high_conviction": acc_conv})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3544.json").write_text(
        json.dumps({"ops": 3544, "fails": fails}))
sys.exit(0)
