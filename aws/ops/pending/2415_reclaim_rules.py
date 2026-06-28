import boto3
ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1")
# 1) all existing function names
fns=set(); tok=None
while True:
    kw={"MaxItems":1000}
    if tok: kw["Marker"]=tok
    r=lam.list_functions(**kw)
    for f in r["Functions"]: fns.add(f["FunctionName"])
    tok=r.get("NextMarker")
    if not tok: break
print("live functions:",len(fns))
# 2) all rules + targets, find orphans (lambda target(s), none exist)
rules=[]; tok=None
while True:
    kw={"Limit":100}
    if tok: kw["NextToken"]=tok
    r=ev.list_rules(**kw); rules.extend(r["Rules"]); tok=r.get("NextToken")
    if not tok: break
print("total rules:",len(rules))
orphans=[]
for ru in rules:
    name=ru["Name"]
    tg=ev.list_targets_by_rule(Rule=name).get("Targets",[])
    lam_targets=[t for t in tg if ":function:" in t.get("Arn","")]
    if not lam_targets: continue
    missing=[t for t in lam_targets if t["Arn"].split(":function:")[-1].split(":")[0] not in fns]
    if missing and len(missing)==len(lam_targets):  # ALL lambda targets dead
        orphans.append((name,[t["Arn"].split(":function:")[-1] for t in missing],[t["Id"] for t in tg]))
print("\n=== ORPHANED rules (target function deleted) — %d ==="%len(orphans))
for n,fnmiss,ids in orphans:
    print("  %s -> dead target: %s"%(n,fnmiss[0]))
# 3) delete orphans (free slots)
deleted=0
for n,fnmiss,ids in orphans:
    try:
        if ids: ev.remove_targets(Rule=n, Ids=ids)
        ev.delete_rule(Name=n)
        deleted+=1
    except Exception as e:
        print("  del fail %s: %s"%(n,str(e)[:60]))
print("\ndeleted %d orphaned rules -> rules now ~%d/300"%(deleted,len(rules)-deleted))
# 4) now create the watchdog rule that hit the limit
if deleted>0 or len(rules)<300:
    try:
        ACCT="857687956942"; rule="fleet-error-monitor-5min"; fn="justhodl-fleet-error-monitor"
        farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
        rr=ev.put_rule(Name=rule,ScheduleExpression="rate(5 minutes)",State="ENABLED",Description="AUDIT - fleet error rates + DLQ depth")
        ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":farn}])
        try:
            lam.add_permission(FunctionName=fn,StatementId="eb-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=rr["RuleArn"])
        except lam.exceptions.ResourceConflictException: pass
        print("CREATED fleet-error-monitor-5min ->",ev.describe_rule(Name=rule)["State"])
    except Exception as e: print("create err:",str(e)[:80])
print("DONE 2415")
