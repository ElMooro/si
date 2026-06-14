"""Verify revision-momentum v2 is live: trigger opportunity-engine, confirm a
dated baseline (data/estimate-revisions/{today}.json) is now being written."""
import json, time, boto3
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
B = "justhodl-dashboard-live"
today = datetime.now(timezone.utc).date().isoformat()
key = f"data/estimate-revisions/{today}.json"

# already there?
def exists(k):
    try: s3.head_object(Bucket=B, Key=k); return True
    except Exception: return False

print("dated baseline before:", exists(key))
try:
    lam.invoke(FunctionName="justhodl-opportunity-engine", InvocationType="Event")
    print("invoked opportunity-engine (async)")
except Exception as e:
    print("invoke err:", str(e)[:160])

for i in range(10):
    time.sleep(20)
    if exists(key):
        b = json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
        frg = b.get("fwd_rev_growth", {})
        print(f"dated baseline WRITTEN after ~{(i+1)*20}s: {len(frg)} tickers, date={b.get('date')}")
        break
else:
    print("dated baseline not yet present (engine may still be running; scheduled run will create it)")

# list how many dated baselines exist so far (accumulation progress)
objs = s3.list_objects_v2(Bucket=B, Prefix="data/estimate-revisions/").get("Contents", [])
days = sorted(o["Key"].split("/")[-1].replace(".json","") for o in objs if o["Key"].endswith(".json") and "/" in o["Key"][len("data/estimate-revisions/"):]==False or o["Key"]!="data/estimate-revisions/")
dd = sorted([o["Key"].split("/")[-1].replace(".json","") for o in objs if o["Key"]!=f"data/estimate-revisions/"])
print("dated baselines accumulated:", len(dd), "->", dd[:3], "...", dd[-2:] if len(dd)>2 else "")
print("NOTE: UP/DOWN signal needs ~15 of these; ETA ~3 weeks of daily runs.")
