"""ops 3399 — confirm the fused eurodollar-hub signal is actually VALUED (not just registered)
in both risk-regime and crisis-composite outputs."""
import json, boto3
from ops_report import report
s3=boto3.client("s3",region_name="us-east-1")
with report("3399_verify_fusion") as r:
    r.section("risk-regime — eurodollar_hub block value")
    rr=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/risk-regime.json")["Body"].read())
    # find the block wherever it lives
    eh=rr.get("eurodollar_hub")
    if not eh:
        # search nested
        for k,v in rr.items():
            if isinstance(v,dict) and "eurodollar_hub" in v:
                eh=v["eurodollar_hub"]; break
    r.log(f"  top-level keys: {list(rr.keys())[:20]}")
    r.log(f"  eurodollar_hub detail: {json.dumps(eh)[:200] if eh else 'NOT FOUND as top key'}")
    # the score field name
    for sk in ("composite_score","score","roro_score","regime_score"):
        if sk in rr: r.log(f"  {sk}={rr[sk]} regime={rr.get('regime')}")

    r.section("crisis-composite — eurodollar-hub component value")
    cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crisis-composite.json")["Body"].read())
    r.log(f"  top-level keys: {list(cc.keys())[:20]}")
    comps=cc.get("components") or []
    for c in comps:
        if "hub" in json.dumps(c).lower():
            r.log(f"  HUB component: {json.dumps(c)[:220]}")
    for sk in ("master_score","score","master","crisis_score"):
        if sk in cc: r.log(f"  {sk}={cc[sk]} defcon={cc.get('defcon') or cc.get('level')}")
