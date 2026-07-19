"""ops 3521 — upload config/feed-sla.json to S3 (registry reads S3;
pages.yml never syncs config/ — same path retired.json took) + regate."""
import json, sys, time
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
s3c = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
with report("3521_sla_upload") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:420]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    body = (REPO/"config/feed-sla.json").read_bytes()
    s3c.put_object(Bucket="justhodl-dashboard-live",
                   Key="config/feed-sla.json", Body=body,
                   ContentType="application/json", CacheControl="no-cache")
    lam.invoke(FunctionName="justhodl-feed-registry", Payload=b"{}")
    time.sleep(2)
    reg = json.loads(s3c.get_object(Bucket="justhodl-dashboard-live",
                     Key="data/feed-registry.json")["Body"].read())
    rows = reg.get("feeds") or reg.get("rows") or []
    ex = [r for r in rows if r.get("sla_source") == "explicit"]
    gate("V1_explicit", len(ex) >= 3,
         {"n_explicit": len(ex),
          "sample": [(r["key"], r["sla_h"], round(r.get("age_h",0),1),
                      r.get("stale")) for r in ex[:5]]})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3521.json").write_text(json.dumps({"ops":3521,"fails":fails}))
sys.exit(0)
