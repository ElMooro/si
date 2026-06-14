"""Verify peer-valuation axis is live: trigger opportunity-engine, confirm picks
now carry pv (cheap/fair/rich) + cyc, and show the distribution."""
import json, time, boto3, collections
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
today = datetime.now(timezone.utc).date().isoformat()
key = f"data/track-record/snapshots/{today}.json"

try:
    lam.invoke(FunctionName="justhodl-opportunity-engine", InvocationType="Event")
    print("invoked opportunity-engine (async)")
except Exception as e:
    print("invoke err:", str(e)[:160])

prev_mtime = None
for i in range(12):
    time.sleep(20)
    try:
        head = s3.head_object(Bucket=B, Key=key)
        snap = json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
        picks = snap.get("picks") or {}
        pv = collections.Counter(str((p or {}).get("pv")) for p in picks.values())
        cyc = collections.Counter(str((p or {}).get("cyc")) for p in picks.values())
        # only report once pv is actually populated (engine finished new run)
        if any(k in ("cheap","fair","rich") for k in pv):
            print(f"\nsnapshot {today}: {len(picks)} picks")
            print("  pv (peer valuation) distribution:", dict(pv))
            print("  cyc (cycle stage) distribution:", dict(cyc))
            # sample a few cheap names
            cheap = [tk for tk, p in picks.items() if (p or {}).get("pv") == "cheap"][:12]
            print("  sample CHEAP-vs-peer names:", cheap)
            break
    except Exception as e:
        pass
    print(f"  ...waiting ({(i+1)*20}s)")
else:
    print("pv not populated yet — engine still running; scheduled run will populate it")
