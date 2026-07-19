"""ops 3552 — comeback∩census overlap truth (read-only). If the
comeback universe (52w-low names fleet-wide) simply doesn't intersect
the S&P census, with_census=0 is honest and the overlay stays as a
dormant-but-armed join."""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
with report("3552_comeback_overlap") as rep:
    d = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/comeback-screener.json")["Body"].read())
    rows = []
    for v in (d.get("boards") or {}).values():
        if isinstance(v, list):
            rows += [x for x in v if isinstance(x, dict)]
    ticks = [str(x.get("ticker") or "").upper() for x in rows]
    MX = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/fundamental-census-matrix.json")["Body"].read())
    uni = set(MX["tickers"])
    inter = [t for t in ticks if t in uni]
    line = ("PASS  X1_overlap — " + json.dumps(
        {"n_comeback": len(ticks), "sample": ticks[:10],
         "sp500_overlap_n": len(inter), "overlap": inter[:8],
         "verdict": ("universe-disjoint (honest zero)" if not inter
                     else "OVERLAP EXISTS — enrichment bug")}))[:600]
    print(line); rep.log(line)
    ok = (len(inter) == 0)
    print("RESULT:", "ALL PASS" if ok else "FAILS: ['X1_overlap']")
    (REPO/"aws/ops/reports/3552.json").write_text(
        json.dumps({"ops": 3552, "fails": [] if ok else ["X1_overlap"]}))
sys.exit(0)
