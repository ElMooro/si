"""1956 — revive 18 dead engines: rebuild rule bindings (idempotent) + ASYNC
invoke (Event) so we never block on slow engines. Sleep then sample-verify feeds."""
import boto3, time, datetime
lam=boto3.client("lambda","us-east-1"); events=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1"); BUCKET="justhodl-dashboard-live"; ACCT="857687956942"
now=datetime.datetime.now(datetime.timezone.utc)
DEAD=["justhodl-forward-orders","justhodl-forensic-screen","justhodl-crisis-plumbing",
 "justhodl-engine-conflicts","justhodl-buzz-velocity","justhodl-cross-asset-confirm",
 "justhodl-liquidity-pulse","justhodl-crypto-cycle-risk","justhodl-earnings-tracker",
 "justhodl-yield-curve","justhodl-onchain-ratios","justhodl-theme-cascade",
 "justhodl-forward-returns","justhodl-bagger-engine","justhodl-hiring-velocity",
 "justhodl-signal-halflife","justhodl-buyback-scanner","justhodl-index-inclusion"]
fn_rules={f:[] for f in DEAD}
for pg in events.get_paginator("list_rules").paginate():
    for r in pg["Rules"]:
        if not r.get("ScheduleExpression"): continue
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
            fn=t["Arn"].split(":function:")[-1] if ":function:" in t.get("Arn","") else None
            if fn in fn_rules: fn_rules[fn].append((r["Name"],r["ScheduleExpression"]))
for fn in DEAD:
    arn=f"arn:aws:lambda:us-east-1:{ACCT}:function:{fn}"; n=0
    for rule,cron in fn_rules.get(fn,[]):
        try:
            events.put_rule(Name=rule,ScheduleExpression=cron,State="ENABLED")
            ex=events.list_targets_by_rule(Rule=rule).get("Targets",[])
            if len(ex)>1: events.remove_targets(Rule=rule,Ids=[t["Id"] for t in ex])
            events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":arn}])
            sid=f"{rule}-invoke"
            try: lam.remove_permission(FunctionName=fn,StatementId=sid)
            except Exception: pass
            lam.add_permission(FunctionName=fn,StatementId=sid,Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACCT}:rule/{rule}")
            n+=1
        except Exception as e: print(f"  {fn} rule {rule} ERR {e}")
    try: lam.invoke(FunctionName=fn,InvocationType="Event")  # async fire
    except Exception as e: print(f"  {fn} invoke ERR {e}")
    print(f"{fn:<34} rules_rebuilt={n} async_fired")
print("\nsleeping 75s for async engines to write...")
time.sleep(75)
fresh=0
for fn in DEAD:
    feed="data/"+fn.replace("justhodl-","")+".json"
    try:
        age=(now-s3.head_object(Bucket=BUCKET,Key=feed)["LastModified"]).total_seconds()/3600
        ok=age<0.5; fresh+=ok
        print(f"  {feed:<42} age_h={age:5.1f} {'FRESH' if ok else 'still-stale'}")
    except Exception as e: print(f"  {feed:<42} head ERR {type(e).__name__}")
print(f"\nfresh after revive: {fresh}/{len(DEAD)}  (rules rebuilt regardless; slow engines refresh on next scheduled tick)")
print("DONE 1956")
