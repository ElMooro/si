"""
1953 — diagnose & REPAIR the silently-dead feeds feeding master-ranker.
For pairs-scanner, future-intelligence, divergence-v2:
  1. resolve producing Lambda (search 'diverg' for the unknown one)
  2. inspect resource policy for events.amazonaws.com invoke permission
  3. inspect the EventBridge rule target Arn correctness
  4. REPAIR: add missing lambda:InvokeFunction permission for the rule
  5. test-invoke and confirm the data/<feed>.json key refreshes (mtime moves)
Idempotent: add_permission wrapped in try (skips if SID exists).
"""
import boto3, json, time, datetime

lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
ACCT = "857687956942"

def find_diverg():
    names = []
    p = lam.get_paginator("list_functions")
    for pg in p.paginate():
        for f in pg["Functions"]:
            n = f["FunctionName"]
            if "diverg" in n.lower():
                names.append(n)
    return names

# resolve the 3 producers
targets = {
    "pairs-scanner": ("justhodl-pairs-scanner", "data/pairs-scanner.json"),
    "future-intelligence": ("justhodl-future-intelligence", "data/future-intelligence.json"),
}
print("diverg* functions:", find_diverg())
# pick the most likely divergence-v2 producer
dv = find_diverg()
if dv:
    # prefer one whose name suggests v2 / scanner / engine
    pick = sorted(dv, key=lambda n: (("v2" not in n), ("scan" not in n and "radar" not in n), len(n)))[0]
    targets["divergence-v2"] = (pick, "data/divergence-v2.json")

def rule_targets_for(fn):
    out = []
    p = events.get_paginator("list_rules")
    for pg in p.paginate():
        for r in pg["Rules"]:
            for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets", []):
                if fn in t.get("Arn", ""):
                    out.append((r["Name"], r.get("State"), r.get("ScheduleExpression"), t.get("Id"), t.get("Arn")))
    return out

def has_events_perm(fn):
    try:
        pol = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
    except lam.exceptions.ResourceNotFoundException:
        return False, []
    sids = []
    ok = False
    for st in pol.get("Statement", []):
        svc = st.get("Principal", {}).get("Service", "")
        sids.append(st.get("Sid"))
        if svc == "events.amazonaws.com":
            ok = True
    return ok, sids

for feed, (fn, key) in targets.items():
    print("\n" + "="*64)
    print(f"FEED {feed}  ->  {fn}")
    print("="*64)
    try:
        lam.get_function(FunctionName=fn)
    except Exception as e:
        print(f"  lambda missing: {e}"); continue

    rts = rule_targets_for(fn)
    print("  rule targets:", rts if rts else "NONE")
    ok, sids = has_events_perm(fn)
    print(f"  events invoke permission present: {ok}  (policy SIDs: {sids})")

    # mtime before
    try:
        before = s3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
    except Exception:
        before = None
    print(f"  {key} LastModified BEFORE: {before}")

    # REPAIR permission if a rule exists but perm missing
    if rts and not ok:
        rule_arn = f"arn:aws:events:us-east-1:{ACCT}:rule/{rts[0][0]}"
        sid = f"evt-{fn[:40]}".replace("-", "")[:64]
        try:
            lam.add_permission(FunctionName=fn, StatementId=sid,
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=rule_arn)
            print(f"  REPAIRED: added invoke permission SID={sid} for {rule_arn}")
        except lam.exceptions.ResourceConflictException:
            print(f"  permission SID exists already ({sid}) — not the cause")
        except Exception as e:
            print(f"  add_permission failed: {type(e).__name__}: {e}")
    elif not rts:
        print("  NO rule targets this fn — schedule orphaned (needs rule creation, not perm)")

    # test invoke
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
        sc = r.get("StatusCode")
        payload = r["Payload"].read()[:300]
        ferr = r.get("FunctionError")
        print(f"  test invoke: StatusCode={sc} FunctionError={ferr}")
        print(f"  payload head: {payload[:240]!r}")
    except Exception as e:
        print(f"  invoke failed: {type(e).__name__}: {e}")
        continue

    time.sleep(3)
    try:
        after = s3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
    except Exception:
        after = None
    moved = (before is None and after is not None) or (before and after and after > before)
    print(f"  {key} LastModified AFTER : {after}   -> refreshed: {moved}")

print("\nDONE 1953")
