"""ops 3365 — dump the full sovereign-stress.json shape so the dedicated page renders
everything the engine computes (CISS by region, SovCISS by country, spreads, equity/bond,
unemployment, country scores, cross-reference). Read-only."""
import json, boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
with report("3365_sovereign_shape") as r:
    ss = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sovereign-stress.json")["Body"].read())
    r.section("top-level keys")
    r.log(f"{list(ss.keys())}")
    for k in ("europe_stress","systemic_stress_ciss","sovereign_stress_sovciss",
              "most_stressed_sovereign","equity_market_stress","sovereign_spreads",
              "bond_market_read","unemployment","industrial_production",
              "country_stress_scores","cross_reference","headline"):
        v = ss.get(k)
        if isinstance(v, dict):
            r.log(f"\n== {k} == keys: {list(v.keys())[:12]}")
            # show one sample nested entry
            for kk, vv in list(v.items())[:3]:
                r.log(f"   {kk}: {json.dumps(vv)[:160]}")
        else:
            r.log(f"\n== {k} == {json.dumps(v)[:200]}")
