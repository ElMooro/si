"""ops 3511 — concern_score polarity flip (auto-discovered column,
high=bad; ops 3510's self-documenting gate surfaced NVDA at p99.2
un-flipped). v1.10.2 + rewarm; NVDA low-concern must land <20."""
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
with report("3511_concern_flip") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:420]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO/"aws"/"lambdas"/FN/"source",
                  env_vars={"FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
                            "S3_BUCKET": BUCKET, "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="v1.10.2 concern flip (ops 3511)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["NVDA", "AAPL"], "periods": ["quarter"], "refresh": True}).encode())
    doc = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundgraph/cache/NVDA_quarter_v20.json")["Body"].read())
    ax = {a["k"]: a for a in doc["factor_dna"]["axes"]}
    gate("K1_flip", ax["concern_score"]["pct"] < 20
         and ax["concern_score"]["label"] == "low concern"
         and ax["altman_z"]["pct"] > 95,
         {"concern": ax["concern_score"], "altman": ax["altman_z"]["pct"]})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3511.json").write_text(json.dumps({"ops":3511,"fails":fails}))
sys.exit(0)
