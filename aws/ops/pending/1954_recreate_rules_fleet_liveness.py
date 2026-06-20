"""
1954 — (1) cleanly RECREATE the 3 silently-dead rules so delivery resumes.
       (2) FLEET-WIDE liveness audit: every scheduled rule -> its output feed
           -> flag any ENABLED rule whose feed is stale beyond 2x its cadence
           (the full silently-dead-schedule picture, not just the 3 known ones).
"""
import boto3, json, re, time, datetime

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"; ACCT = "857687956942"
now = datetime.datetime.now(datetime.timezone.utc)

# ---------- (1) recreate the 3 known-dead rules ----------
recreate = [
    ("justhodl-pairs-scanner",        "pairs-scanner-6hourly",   "cron(42 14 * * ? *)"),
    ("justhodl-future-intelligence",  "future-intel-2x-daily",   "cron(40 16 * * ? *)"),
    ("justhodl-divergence-engine-v2", "divergence-v2-2hourly",   "cron(0 17 * * ? *)"),
]
print("="*64); print("(1) RECREATE DEAD RULES"); print("="*64)
for fn, rule, cron in recreate:
    arn = f"arn:aws:lambda:us-east-1:{ACCT}:function:{fn}"
    try:
        events.put_rule(Name=rule, ScheduleExpression=cron, State="ENABLED")
        # clean duplicate targets then re-add single target
        existing = events.list_targets_by_rule(Rule=rule).get("Targets", [])
        if len(existing) > 1:
            events.remove_targets(Rule=rule, Ids=[t["Id"] for t in existing])
        events.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
        rule_arn = f"arn:aws:events:us-east-1:{ACCT}:rule/{rule}"
        sid = f"{rule}-invoke"
        try:
            lam.remove_permission(FunctionName=fn, StatementId=sid)
        except Exception:
            pass
        lam.add_permission(FunctionName=fn, StatementId=sid, Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
        d = events.describe_rule(Name=rule)
        print(f"  {rule}: state={d['State']} sched={d['ScheduleExpression']} -> rebuilt OK")
    except Exception as e:
        print(f"  {rule}: FAILED {type(e).__name__}: {e}")

# ---------- (2) fleet-wide liveness audit ----------
print("\n" + "="*64); print("(2) FLEET LIVENESS AUDIT (enabled rule, stale feed)"); print("="*64)

def cadence_hours(expr):
    if not expr: return None
    m = re.match(r"rate\((\d+)\s+(\w+)\)", expr)
    if m:
        n = int(m.group(1)); u = m.group(2)
        if "minute" in u: return n/60
        if "hour" in u:   return n
        if "day" in u:    return n*24
    if expr.startswith("cron"):
        # crude: if minute/hour fields contain "/" or "," treat as intraday, else daily
        body = expr[5:-1]
        fields = body.split()
        if len(fields) >= 2:
            mins, hrs = fields[0], fields[1]
            if "/" in hrs or "," in hrs or hrs == "*": return 6   # multiple times/day -> ~6h
            if "/" in mins or "," in mins: return 2
        return 24  # single daily fire
    return None

# map rule -> target lambda
rows = []
p = events.get_paginator("list_rules")
for pg in p.paginate():
    for r in pg["Rules"]:
        expr = r.get("ScheduleExpression")
        if not expr or r.get("State") != "ENABLED":
            continue
        tgts = events.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
        fns = [t["Arn"].split(":function:")[-1] for t in tgts if ":function:" in t.get("Arn","")]
        if not fns:
            continue
        fn = fns[0]
        feed = "data/" + fn.replace("justhodl-", "") + ".json"
        cad = cadence_hours(expr)
        try:
            lm = s3.head_object(Bucket=BUCKET, Key=feed)["LastModified"]
            age = (now - lm).total_seconds()/3600
        except Exception:
            age = None  # no matching feed (engine may write a different key) -> skip noise
        if age is None or cad is None:
            continue
        stale = age > 2.5 * cad
        rows.append((stale, round(age,1), cad, fn, feed))

rows.sort(key=lambda x: -(x[1] or 0))
dead = [r for r in rows if r[0]]
print(f"scheduled rules with a resolvable feed: {len(rows)} | SILENTLY-DEAD (age > 2.5x cadence): {len(dead)}\n")
print("  STALE  age_h  cad_h  function -> feed")
for stale, age, cad, fn, feed in rows:
    if stale:
        print(f"   DEAD  {age:>6}  {cad:>4}   {fn}")
print("\n  (healthy feeds, oldest 8 for context):")
for stale, age, cad, fn, feed in [r for r in rows if not r[0]][:8]:
    print(f"   ok    {age:>6}  {cad:>4}   {fn}")

print("\nDONE 1954")
