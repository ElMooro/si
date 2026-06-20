"""
1955 — revive all 18 silently-dead scheduled engines found in 1954.
For each: rebuild every scheduled rule->target->permission binding using the
rule's OWN existing ScheduleExpression (preserve intended cadence), then
force-invoke once so the feed is fresh now. Report per-engine.
"""
import boto3, json, time, datetime
lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
BUCKET="justhodl-dashboard-live"; ACCT="857687956942"
now = datetime.datetime.now(datetime.timezone.utc)

DEAD = ["justhodl-forward-orders","justhodl-forensic-screen","justhodl-crisis-plumbing",
 "justhodl-engine-conflicts","justhodl-buzz-velocity","justhodl-cross-asset-confirm",
 "justhodl-liquidity-pulse","justhodl-crypto-cycle-risk","justhodl-earnings-tracker",
 "justhodl-yield-curve","justhodl-onchain-ratios","justhodl-theme-cascade",
 "justhodl-forward-returns","justhodl-bagger-engine","justhodl-hiring-velocity",
 "justhodl-signal-halflife","justhodl-buyback-scanner","justhodl-index-inclusion"]

# index rules by target function
fn_rules = {f: [] for f in DEAD}
p = events.get_paginator("list_rules")
for pg in p.paginate():
    for r in pg["Rules"]:
        if not r.get("ScheduleExpression"): continue
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets", []):
            fn = t["Arn"].split(":function:")[-1] if ":function:" in t.get("Arn","") else None
            if fn in fn_rules:
                fn_rules[fn].append((r["Name"], r["ScheduleExpression"]))

ok=0; fail=0
for fn in DEAD:
    arn=f"arn:aws:lambda:us-east-1:{ACCT}:function:{fn}"
    feed="data/"+fn.replace("justhodl-","")+".json"
    rules=fn_rules.get(fn,[])
    rebuilt=[]
    for rule,cron in rules:
        try:
            events.put_rule(Name=rule, ScheduleExpression=cron, State="ENABLED")
            ex=events.list_targets_by_rule(Rule=rule).get("Targets",[])
            if len(ex)>1: events.remove_targets(Rule=rule, Ids=[t["Id"] for t in ex])
            events.put_targets(Rule=rule, Targets=[{"Id":"1","Arn":arn}])
            sid=f"{rule}-invoke"
            try: lam.remove_permission(FunctionName=fn, StatementId=sid)
            except Exception: pass
            lam.add_permission(FunctionName=fn, StatementId=sid, Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:us-east-1:{ACCT}:rule/{rule}")
            rebuilt.append(f"{rule}[{cron}]")
        except Exception as e:
            rebuilt.append(f"{rule}=ERR:{type(e).__name__}")
    # force invoke now
    try:
        r=lam.invoke(FunctionName=fn, InvocationType="RequestResponse")
        sc=r.get("StatusCode"); ferr=r.get("FunctionError")
    except Exception as e:
        sc=None; ferr=f"{type(e).__name__}:{e}"
    time.sleep(2)
    try:
        lm=s3.head_object(Bucket=BUCKET, Key=feed)["LastModified"]
        age=round((now-lm).total_seconds()/3600,1)
        fresh = age < 1
    except Exception:
        age="?"; fresh=False
    status = "OK" if (sc==200 and not ferr) else f"INVOKE_ERR({ferr})"
    if status=="OK": ok+=1
    else: fail+=1
    print(f"{fn:<34} rules={len(rebuilt)} inv={status:<18} feed_age_h={age} {'<fresh>' if fresh else ''}")
    if not rules: print(f"    NOTE: no scheduled rule found targeting {fn} (orphaned)")

print(f"\nrevived OK={ok} FAIL={fail} of {len(DEAD)}")
print("DONE 1955")
