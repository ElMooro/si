"""Read live bottleneck-boom output to confirm freshness + current calls before building its page."""
import json, boto3
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")
d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/bottleneck-boom.json")["Body"].read())
print("generated_at:", d.get("generated_at"), "| version:", d.get("version"))
print("universe_n:", d.get("universe_n"), "scored_n:", d.get("scored_n"), "signals_logged:", d.get("signals_logged"))
print("top_calls:", d.get("top_calls"))
ip = d.get("industry_pressure") or {}
print("\nindustry_pressure type:", type(ip).__name__)
if isinstance(ip, dict):
    items = sorted(ip.items(), key=lambda kv: -(kv[1] if isinstance(kv[1],(int,float)) else (kv[1].get('z',0) if isinstance(kv[1],dict) else 0)))[:8]
    for k,v in items: print(f"  {k}: {v}")
elif isinstance(ip, list):
    for x in ip[:8]: print(" ", x)
print("\ntop 8 ranks:")
for r in (d.get("ranks") or [])[:8]:
    print(f"  {r.get('ticker'):>6} {str(r.get('industry'))[:26]:>26} | accel {r.get('rev_accel_pp')}pp growth {r.get('rev_growth_yoy')}% rev/mcap {r.get('rev_to_mcap_pct')}% boom {r.get('boom_score')}")
print("\nfield keys on a rank row:", list((d.get("ranks") or [{}])[0].keys()))
