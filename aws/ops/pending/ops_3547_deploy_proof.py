"""ops 3547 — deploy the REAL v1.7.2 with zip-marker proof. Chain of
misses: 3544 deployed pre-patch code under a v1.7.2 label; 3545 was a
0-byte artifact; 3546 regated without deploying. Doctrine: after every
deploy, download the live zip and grep the change marker — the zip is
truth, descriptions lie.

  S1 deploy census; download deployed zip; assert b"retail_dp_pct"
     present and b"retail_dp_svr_pct" absent
  S2 aggregate: retail_dp_pct n 10..90, both accum signs,
     ACC ∩ conviction printed
"""
import io, json, sys, time, urllib.request, zipfile
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

with report("3547_deploy_proof") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={}, timeout=900, memory=1536,
                  description="Census v1.7.2 (zip-proven, ops 3547)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    try:
        loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
        zb = urllib.request.urlopen(loc, timeout=60).read()
        src = zipfile.ZipFile(io.BytesIO(zb)).read("lambda_function.py")
        gate("S1_zip_marker", b"retail_dp_pct" in src
             and b"retail_dp_svr_pct" not in src
             and b'"1.7.2"' in src,
             {"zip_kb": len(zb)//1024,
              "has_new": b"retail_dp_pct" in src,
              "old_gone": b"retail_dp_svr_pct" not in src})
    except Exception as e:
        gate("S1_zip_marker", False, str(e)[:260])

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
    gate("S2_retail", 10 <= nn("retail_dp_pct") <= 90
         and len(acc) >= 2 and len(dist) >= 1,
         {"retail_n": nn("retail_dp_pct"),
          "acc_n": len(acc), "dist_n": len(dist),
          "acc_sample": acc[:10], "dist_sample": dist[:8],
          "acc_high_conviction": acc_conv})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3547.json").write_text(
        json.dumps({"ops": 3547, "fails": fails}))
sys.exit(0)
