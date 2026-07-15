"""ops 3355 — lock exact fields for the brain-directed risk signals to add to JSI:
global-tide risk.global_risk_0_100, and the HYG/LQD credit-risk ratio (operator named it
explicitly). Determine HYG/LQD's raw shape so we can transform it to a 0-100 stress axis
(falling HYG/LQD = credit risk-off = stress). Read-only.
"""
import json
import boto3
from ops_report import report

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode())
    except Exception as e:
        return {"__err__": type(e).__name__}


with report("3355_brain_risk_fields") as r:
    r.section("global-tide risk score")
    gt = gj("data/global-tide.json")
    risk = gt.get("risk") or {}
    r.log(f"  risk.global_risk_0_100 = {risk.get('global_risk_0_100')}  tier={risk.get('tier')}")
    r.log(f"  components: {risk.get('components')}")

    r.section("risk-ratios HYG/LQD")
    rr = gj("data/risk-ratios.json")
    if rr.get("__err__"):
        r.log(f"  risk-ratios: {rr['__err__']}")
    else:
        r.log(f"  top keys: {list(rr.keys())}")
        hl = rr.get("hyg_lqd")
        r.log(f"  hyg_lqd = {json.dumps(hl)[:400] if hl else 'MISSING'}")
        # show any nested numeric that reads like a ratio/level/percentile/z
        if isinstance(hl, dict):
            for k, v in hl.items():
                if isinstance(v, (int, float)):
                    r.log(f"    hyg_lqd.{k} = {v}")
        for extra in ("angl_hyg", "hyg", "acwi"):
            e = rr.get(extra)
            if isinstance(e, dict):
                nums = {k: v for k, v in e.items() if isinstance(v, (int, float))}
                r.log(f"  {extra}: {nums}")
