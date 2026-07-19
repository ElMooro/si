"""ops 3533 — forensic-row + census-doc probe (read-only)."""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
s3c = boto3.client("s3", region_name="us-east-1")
with report("3533_probe_uni") as rep:
    f = json.loads(s3c.get_object(Bucket="justhodl-dashboard-live",
        Key="data/forensic-screen.json")["Body"].read())
    rows = f.get("all_results") or []
    r0 = rows[0] if rows else {}
    print("PASS  G1_forensic —", json.dumps({
        "top_keys": sorted(f.keys())[:12], "n_all_results": len(rows),
        "row0_keys": sorted(r0.keys())[:14],
        "row0_ticker": r0.get("ticker"), "row0_symbol": r0.get("symbol"),
        "generated_at": f.get("generated_at")})[:600])
    D = json.loads(s3c.get_object(Bucket="justhodl-dashboard-live",
        Key="data/fundamental-census.json")["Body"].read())
    print("PASS  G2_census —", json.dumps({
        "generated_at": D.get("generated_at"),
        "coverage": D.get("coverage"),
        "elapsed_s": D.get("elapsed_s")})[:500])
    rep.log("probe"); print("RESULT: ALL PASS")
    (REPO/"aws/ops/reports/3533.json").write_text("{}")
sys.exit(0)
