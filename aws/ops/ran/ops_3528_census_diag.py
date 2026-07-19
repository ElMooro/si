"""ops 3528 — census warm diagnosis + at-scale regate. Prints the RAW
fundgraph warm invoke response (StatusCode/FunctionError/payload peek),
counts v21 cache docs before/after, polls the running async chain,
re-aggregates, and regates B2/B3 at real scale.
"""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=900, connect_timeout=10,
                                 retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")

def cache_count():
    n, token = 0, None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": "data/fundgraph/cache/",
              "MaxKeys": 1000}
        if token: kw["ContinuationToken"] = token
        r = s3c.list_objects_v2(**kw)
        n += sum(1 for o in r.get("Contents") or []
                 if o["Key"].endswith("_quarter_v21.json"))
        if not r.get("IsTruncated"): return n
        token = r["NextContinuationToken"]

with report("3528_census_diag") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    c0 = cache_count()
    r = lam.invoke(FunctionName="justhodl-fundamental-graphs",
                   Payload=json.dumps({"warm": ["MSFT", "JPM", "XOM"],
                                       "periods": ["quarter"]}).encode())
    peek = r["Payload"].read()[:400].decode(errors="replace")
    gate("C1_warm_response", True,
         {"status": r.get("StatusCode"), "err": r.get("FunctionError"),
          "peek": peek})
    time.sleep(4)
    c1 = cache_count()
    gate("C2_warm_writes", c1 >= c0 + 2,
         {"before": c0, "after": c1})

    grow = [c1]
    for _ in range(10):
        time.sleep(45)
        grow.append(cache_count())
    gate("C3_chain_progress", grow[-1] > grow[0],
         {"trajectory": grow})

    lam.invoke(FunctionName="justhodl-fundamental-census",
               Payload=json.dumps({"phase": "aggregate"}).encode())
    time.sleep(3)
    D = json.loads(s3c.get_object(Bucket=BUCKET,
                   Key="data/fundamental-census.json")["Body"].read())
    cov = D["coverage"]; mb = D["metric_boards"]
    sh = mb["share_count_yoy_pct"]
    gate("C4_scale", cov["scored"] >= 100
         and all(b["n"] >= 80 for b in mb.values()),
         {"scored": cov["scored"],
          "top5": [(x["t"], x["score"], x["n_elite"])
                   for x in D["top_quality"][:5]],
          "careful5": [(x["t"], x["flags"], x["red3"][:2], x["flag_w"])
                       for x in D["careful"][:5]],
          "buybacks": sh["best"][:3], "issuers": sh["worst"][:3],
          "avg": D["summary"]["avg_score"],
          "flagged": D["summary"]["n_flagged"]})
    gate("C5_direction", sh["best"][0]["v"] < sh["worst"][0]["v"]
         and (sh["worst"][0]["v"] > 0 or cov["scored"] < 150),
         {"best": sh["best"][0], "worst": sh["worst"][0]})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3528.json").write_text(json.dumps({"ops":3528,"fails":fails}))
sys.exit(0)
