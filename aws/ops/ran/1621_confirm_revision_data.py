"""Confirm root cause of empty by_revision: is fwd_rev_growth data populated,
and what 'rev' values actually land in a real (weekday) snapshot?"""
import json, boto3, collections
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
def load(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:120]}

# 1) the prior-snapshot comparison file
er = load("data/estimate-revisions-latest.json")
if "_err" in er:
    print("estimate-revisions-latest.json:", er["_err"])
else:
    frg = er.get("fwd_rev_growth", {})
    nonnull = {k: v for k, v in frg.items() if v is not None}
    print(f"estimate-revisions-latest.json: date={er.get('date')} | tickers={len(frg)} | non-null fwd_rev_growth={len(nonnull)}")
    print("  sample:", dict(list(nonnull.items())[:5]))

# 2) find a real weekday snapshot with picks and inspect rev distribution
r = s3.list_objects_v2(Bucket=B, Prefix="data/track-record/snapshots/", MaxKeys=400)
keys = sorted((o["Key"] for o in r.get("Contents", [])), reverse=True)
for k in keys:
    snap = load(k)
    picks = (snap or {}).get("picks") or {}
    if len(picks) > 0:
        revs = collections.Counter(str((p or {}).get("rev")) for p in picks.values())
        print(f"\nsnapshot {k.split('/')[-1]}: {len(picks)} picks")
        print("  rev value distribution:", dict(revs))
        break
else:
    print("\nNo snapshot with picks>0 found in last 400 keys")
