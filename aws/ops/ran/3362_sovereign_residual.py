import json, boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
with report("3362_sovereign_residual") as r:
    ss = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
    errs = ss.get("errors") or []
    r.section("residual errors")
    r.log(f"count: {len(errs)}")
    for e in errs:
        r.log(f"  ✗ {e}")
    # show which SovCISS/CISS/equity actually populated
    r.log(f"sources OK: {ss.get('sources')}")
    ciss = ss.get("systemic_stress_ciss") or {}
    sov = ss.get("sovereign_stress_sovciss") or {}
    r.log(f"CISS populated: {list(ciss.keys())}")
    r.log(f"SovCISS populated: {list(sov.keys())}")
    sp = ss.get("sovereign_spreads") or {}
    r.log(f"sovereign_spreads: {list(sp.keys()) if isinstance(sp,dict) else sp}")
