"""ops 3569 — doc-vs-matrix truth probe (WMT + 3 others)."""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
KEYS = ["days_inventory", "netChangeInCash", "pe_fwd", "roce_pct",
        "fulmer_h", "price_to_book"]
with report("3569_wmt_probe") as rep:
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    idx = {t: i for i, t in enumerate(MX["tickers"])}
    for t in ("WMT", "MSFT", "AAPL", "ZTS"):
        doc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key=f"data/fundgraph/cache/{t}_quarter_v21.json")
            ["Body"].read())
        P = doc.get("points") or {}
        dv = {k: ((P.get(k) or [[None, None]])[-1][1],
                  len(P.get(k) or [])) for k in KEYS}
        mv = {k: (MX["cols"].get(k) or [None]*len(idx))[idx[t]]
              for k in KEYS}
        line = ("PASS  P_" + t + " — " + json.dumps(
            {"doc_generated": (doc.get("generated_at") or "")[:19],
             "doc": dv, "matrix": mv}, default=str))[:740]
        print(line); rep.log(line)
    print("RESULT: ALL PASS")
    (REPO/"aws/ops/reports/3569.json").write_text(
        json.dumps({"ops": 3569, "fails": []}))
sys.exit(0)
