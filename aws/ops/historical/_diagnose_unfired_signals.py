"""Diagnose why divergence_extreme, cot_extreme, eurodollar_stress didn't log."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def fs3(key):
    try:
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
    except Exception as e:
        return {"_error": str(e)}


def main():
    with report("diagnose_unfired_signals") as r:
        r.heading("Divergences")
        d = fs3("divergence/current.json")
        if "_error" in d:
            r.log(f"  ✗ {d['_error']}")
        else:
            r.log(f"  keys: {list(d.keys())[:10]}")
            divs = d.get("divergences") or d.get("active_divergences") or []
            r.log(f"  total: {len(divs)}")
            for i, x in enumerate(divs[:6]):
                z = x.get("residual_z") or x.get("z_score")
                r.log(f"    [{i}] z={z}  asset_y={x.get('asset_y') or x.get('y')}  asset_x={x.get('asset_x') or x.get('x')}")
                r.log(f"        keys={list(x.keys())[:10]}")

        r.heading("COT extremes")
        d = fs3("data/cot-extremes.json")
        if "_error" in d:
            d = fs3("cot/extremes.json")
        if "_error" in d:
            r.log(f"  ✗ both paths missing")
        else:
            r.log(f"  keys: {list(d.keys())[:10]}")
            ex = d.get("extremes") or d.get("contracts") or []
            r.log(f"  total: {len(ex)}")
            for i, x in enumerate(ex[:6]):
                pct = x.get("percentile_rank") or x.get("pct_rank")
                contract = x.get("contract") or x.get("name")
                r.log(f"    [{i}] contract={contract}  pct={pct}")

        r.heading("Eurodollar stress")
        d = fs3("data/eurodollar-stress.json")
        if "_error" in d:
            r.log(f"  ✗ {d['_error']}")
        else:
            r.log(f"  keys: {list(d.keys())[:10]}")
            for k in ("composite_stress_score", "composite_score", "composite", "regime"):
                if k in d:
                    r.log(f"  {k}: {d[k]}")


if __name__ == "__main__":
    main()
