"""Read the actual forward-edge numbers already measured in signal-backtest.json."""
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
sb = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-backtest.json")["Body"].read())

print("maturity:", sb.get("maturity"), "| n_obs:", sb.get("n_observations"),
      "| snapshots_used:", sb.get("snapshots_used"))

def show(label, blk):
    print(f"\n=== {label} ===")
    if isinstance(blk, dict):
        for k, v in blk.items():
            if isinstance(v, dict):
                # typical fields: n, avg_return/mean, median, hit_rate/win_rate, horizon
                keep = {kk: v.get(kk) for kk in ("n","count","avg","mean","avg_return","median","median_return","hit_rate","win_rate","fwd","horizon_days") if kk in v}
                print(f"  {k}: {keep}")
            else:
                print(f"  {k}: {v}")

show("OVERALL", sb.get("overall"))
show("BY VERDICT TIER", sb.get("by_verdict"))
show("BY REVISION (the 'improving' axis)", sb.get("by_revision"))
show("BY COMPOUNDER BUCKET", sb.get("by_compounder_bucket"))
show("BY CAP BUCKET", sb.get("by_cap_bucket"))
