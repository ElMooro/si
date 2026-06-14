"""What do snapshot rows actually carry for revisions? Diagnose the empty by_revision."""
import json, boto3, collections
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
r = s3.list_objects_v2(Bucket=B, Prefix="data/track-record/snapshots/", MaxKeys=400)
keys = sorted(o["Key"] for o in r.get("Contents", []))
snap = json.loads(s3.get_object(Bucket=B, Key=keys[-1])["Body"].read())
rows = snap if isinstance(snap, list) else (snap.get("items") or snap.get("rows") or snap.get("data") or [])
print("latest snapshot:", keys[-1].split("/")[-1], "| rows:", len(rows))
if rows and isinstance(rows[0], dict):
    print("\nrow[0] ALL fields:", list(rows[0].keys()))
    print("\nsample row[0]:", json.dumps({k: rows[0][k] for k in list(rows[0])[:14]})[:400])
    # any field mentioning revision?
    revish = [k for k in rows[0] if "rev" in k.lower() or "estim" in k.lower() or "upgrad" in k.lower()]
    print("\nrevision-ish fields present:", revish)
    for f in revish:
        dist = collections.Counter(str(x.get(f)) for x in rows if isinstance(x, dict))
        print(f"  value distribution of '{f}':", dict(list(dist.items())[:8]))
    # does 'rev' specifically exist and what values?
    dist = collections.Counter(str(x.get("rev")) for x in rows if isinstance(x, dict))
    print("\n'rev' field value distribution:", dict(list(dist.items())[:8]))
