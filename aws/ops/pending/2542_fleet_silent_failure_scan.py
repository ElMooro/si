"""ops 2542 — fleet silent-failure scan.

Theme of the session: silent failures going unnoticed. LLM-health fixed the
provider blind spot; this finds the engine-level version — feeds that are stale
because a schedule is broken/disabled or the engine errors silently.

(1) Read the freshness manifest (keys + thresholds) and head each in S3 -> which
    are STALE now (age > threshold). (2) Does the existing freshness monitor
    already flag them? (3) Which EventBridge rules are DISABLED. (4) Spot-check
    the memory-flagged engines' last CloudWatch error.
"""
import boto3, json, time
from datetime import datetime, timezone

s3 = boto3.client("s3", "us-east-1")
events = boto3.client("events", "us-east-1")
logs = boto3.client("logs", "us-east-1")
BUCKET = "justhodl-dashboard-live"
now = datetime.now(timezone.utc)


def rd(k):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return {}


# 1. manifest keys + thresholds
man = rd("data/_freshness-manifest.json")
# manifest may be {key: hours} or {entries:[...]} — handle both
thresholds = {}
if isinstance(man, dict):
    for k, v in man.items():
        if isinstance(v, (int, float)):
            thresholds[k] = v
        elif isinstance(v, dict) and "max_age_h" in v:
            thresholds[k] = v["max_age_h"]
print("manifest keys watched:", len(thresholds))

stale = []
for k, thr in thresholds.items():
    try:
        h = s3.head_object(Bucket=BUCKET, Key=k)
        age = (now - h["LastModified"].astimezone(timezone.utc)).total_seconds() / 3600
        if age > thr:
            stale.append((k, round(age, 1), thr))
    except Exception:
        stale.append((k, "MISSING", thr))
stale.sort(key=lambda x: -(x[1] if isinstance(x[1], (int, float)) else 1e9))
print(f"\nSTALE feeds (age_h > threshold): {len(stale)}")
for k, age, thr in stale[:30]:
    print(f"  {k:<46} age={age}h  thr={thr}h")

# 2. does the existing monitor report them?
mon = rd("data/_freshness-monitor.json")
mon_stale = mon.get("stale") or mon.get("stale_keys") or mon.get("alerts") or mon.get("flagged")
print("\nfreshness-monitor last run:", mon.get("generated_at") or mon.get("last_run") or "?")
print("  monitor-reported stale count:", len(mon_stale) if isinstance(mon_stale, list) else mon_stale)

# 3. disabled EventBridge rules
disabled = []
paginator = events.get_paginator("list_rules")
allrules = 0
for page in paginator.paginate():
    for r in page["Rules"]:
        allrules += 1
        if r.get("State") == "DISABLED" and ("justhodl" in r["Name"] or "intel" in r["Name"] or "scanner" in r["Name"]):
            disabled.append(r["Name"])
print(f"\nEventBridge rules total: {allrules} | DISABLED (justhodl-ish): {len(disabled)}")
for d in disabled[:30]:
    print("  DISABLED:", d)

# 4. spot-check flagged engines' last error
for fn in ["justhodl-future-intelligence", "justhodl-pairs-scanner", "justhodl-divergence-v2",
           "justhodl-divergence-interpreter"]:
    try:
        lg = f"/aws/lambda/{fn}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1).get("logStreams", [])
        if not streams:
            print(f"\n{fn}: no log streams (maybe never invoked)"); continue
        last = streams[0]
        last_ts = datetime.fromtimestamp(last.get("lastEventTimestamp", 0) / 1000, timezone.utc)
        age_d = (now - last_ts).total_seconds() / 86400
        ev = logs.get_log_events(logGroupName=lg, logStreamName=last["logStreamName"], limit=15, startFromHead=False).get("events", [])
        errs = [e["message"].rstrip()[:160] for e in ev if any(x in e["message"] for x in ("Error", "Traceback", "Exception", "Task timed", "errorMessage", "Unable"))]
        print(f"\n{fn}: last log {round(age_d,1)}d ago", "| recent errors:" if errs else "| no recent errors in tail")
        for e in errs[:4]:
            print("    " + e)
    except logs.exceptions.ResourceNotFoundException:
        print(f"\n{fn}: NO log group (never deployed/invoked)")
    except Exception as e:
        print(f"\n{fn}: log check err {str(e)[:80]}")
print("\nDONE 2542")
