import json, boto3
from datetime import datetime, timezone
s3=boto3.client("s3",region_name="us-east-1"); ddb=boto3.client("dynamodb",region_name="us-east-1")
B="justhodl-dashboard-live"
# 1) existing track-record snapshots: do they include bottleneck calls?
try:
    snaps=s3.list_objects_v2(Bucket=B,Prefix="data/track-record/snapshots/").get("Contents",[])
    print(f"track-record snapshots: {len(snaps)} files, latest: {snaps[-1]['Key'] if snaps else None}")
except Exception as e: print("snap list err",str(e)[:80])
# 2) signal-backtest engine breakdown — any per-engine/source split?
try:
    sb=json.loads(s3.get_object(Bucket=B,Key="data/signal-backtest.json")["Body"].read())
    print("signal-backtest top keys:", list(sb.keys())[:15])
    for k in ("by_engine","by_source","by_verdict","engines"):
        if k in sb: print(f"  has {k}:", list(sb[k].keys())[:8] if isinstance(sb[k],dict) else sb[k])
except Exception as e: print("signal-backtest err",str(e)[:80])
# 3) justhodl-signals DDB: any bottleneck-source entries?
try:
    r=ddb.scan(TableName="justhodl-signals", Limit=5)
    print("justhodl-signals sample item keys:", [list(it.keys()) for it in r.get("Items",[])[:2]])
    # look for a source/engine attr
    if r.get("Items"):
        it=r["Items"][0]; print("  sample item:", json.dumps({k:list(v.values())[0] for k,v in it.items()})[:300])
except Exception as e: print("signals scan err",str(e)[:80])
# 4) is there a bottleneck-specific track file already?
for k in ("data/bottleneck-track-record.json","data/bottleneck-boom-outcomes.json"):
    try: s3.head_object(Bucket=B,Key=k); print(f"{k}: EXISTS")
    except Exception: print(f"{k}: none")
