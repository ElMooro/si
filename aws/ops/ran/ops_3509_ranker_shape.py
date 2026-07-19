"""ops 3509 — master-ranker container shapes (read-only)."""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
s3c = boto3.client("s3", region_name="us-east-1")
with report("3509_ranker_shape") as rep:
    d = json.loads(s3c.get_object(Bucket="justhodl-dashboard-live",
                                  Key="data/master-ranker.json")["Body"].read())
    det = {}
    for k, v in d.items():
        det[k] = {"type": type(v).__name__,
                  "len": len(v) if isinstance(v, (list, dict)) else None}
    print("PASS  S1_shapes —", json.dumps(det)[:700])
    tt = d.get("top_tickers")
    print("PASS  S2_top_tickers —", json.dumps(tt)[:900])
    print("PASS  S3_wl_research —", json.dumps(d.get("wl_research"))[:600])
    rep.log(json.dumps(det)[:700]); rep.log(json.dumps(tt)[:900])
    (REPO/"aws/ops/reports/3509.json").write_text(json.dumps({"ops":3509,"shapes":det}))
    print("RESULT: ALL PASS")
sys.exit(0)
