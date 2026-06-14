"""Quantify small-cap coverage gap: pv / rev / cyc coverage by cap bucket,
straight from today's opportunity snapshot picks."""
import json, boto3, collections
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
today = datetime.now(timezone.utc).date().isoformat()
snap = json.loads(s3.get_object(Bucket=B, Key=f"data/track-record/snapshots/{today}.json")["Body"].read())
picks = snap.get("picks") or {}
print(f"snapshot {today}: {len(picks)} picks\n")

buckets = collections.defaultdict(lambda: {"n":0, "pv":0, "rev":0, "cyc":0, "tail_proxy":0})
order = ["mega","large","mid","small","micro","nano","None"]
for tk, p in picks.items():
    cap = str(p.get("cap"))
    b = buckets[cap]
    b["n"] += 1
    if p.get("pv") in ("cheap","fair","rich"): b["pv"] += 1
    if p.get("rev") in ("UP","DOWN","FLAT"): b["rev"] += 1
    if p.get("cyc") and p.get("cyc") != "—": b["cyc"] += 1

print(f"{'cap':>7} | {'n':>4} | {'pv%':>5} | {'rev%':>5} | {'cyc%':>5}")
print("-"*42)
for cap in order:
    if cap not in buckets: continue
    b = buckets[cap]; n = b["n"]
    pct = lambda x: f"{round(x/n*100):>4}%" if n else "   -"
    print(f"{cap:>7} | {n:>4} | {pct(b['pv'])} | {pct(b['rev'])} | {pct(b['cyc'])}")

# headline: pv coverage large-cap vs small/micro/nano
big = sum(buckets[c]["pv"] for c in ("mega","large","mid"))
bign = sum(buckets[c]["n"] for c in ("mega","large","mid"))
sm = sum(buckets[c]["pv"] for c in ("small","micro","nano"))
smn = sum(buckets[c]["n"] for c in ("small","micro","nano"))
print(f"\npv coverage: large/mid {round(big/bign*100) if bign else '-'}% (n={bign})  "
      f"vs small/micro/nano {round(sm/smn*100) if smn else '-'}% (n={smn})")
