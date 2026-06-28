import boto3, json, os
ev=boto3.client("events","us-east-1"); sch=boto3.client("scheduler","us-east-1")
lam=boto3.client("lambda","us-east-1"); iam=boto3.client("iam")
ACCT="857687956942"; role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
REPO=os.environ.get("GITHUB_WORKSPACE",".")
ruled=set(); tok=None
while True:
    kw={"Limit":100}
    if tok: kw["NextToken"]=tok
    r=ev.list_rules(**kw)
    for ru in r["Rules"]:
        for t in ev.list_targets_by_rule(Rule=ru["Name"]).get("Targets",[]):
            if ":function:" in t.get("Arn",""): ruled.add(t["Arn"].split(":function:")[-1].split(":")[0])
    tok=r.get("NextToken")
    if not tok: break
schednames=set(); tok=None
while True:
    kw={"MaxResults":100}
    if tok: kw["NextToken"]=tok
    r=sch.list_schedules(**kw); schednames.update(s["Name"] for s in r["Schedules"]); tok=r.get("NextToken")
    if not tok: break
fns=set(); tok=None
while True:
    kw={"MaxItems":1000}
    if tok: kw["Marker"]=tok
    r=lam.list_functions(**kw); fns.update(f["FunctionName"] for f in r["Functions"]); tok=r.get("NextMarker")
    if not tok: break
def is_scheduled(fn): return fn in ruled or (fn.replace("justhodl-","")+"-sched") in schednames
base=os.path.join(REPO,"aws/lambdas"); orphans=[]
for eng in sorted(os.listdir(base)):
    cfg=os.path.join(base,eng,"config.json")
    if not os.path.exists(cfg): continue
    try: c=json.load(open(cfg))
    except Exception: continue
    fn=c.get("function_name") or eng
    sc=c.get("schedule")
    cron=(sc.get("cron") if isinstance(sc,dict) else sc if isinstance(sc,str) else None)
    if cron and isinstance(cron,str) and cron.startswith(("cron(","rate(")) and fn in fns and not is_scheduled(fn):
        orphans.append((fn,cron))
print("engines with config-cron but NOT scheduled in AWS: %d"%len(orphans))
created=0
for fn,cron in orphans:
    name=fn.replace("justhodl-","")+"-sched"
    farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
    try:
        args=dict(Name=name,ScheduleExpression=cron,FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED")
        (sch.update_schedule if name in schednames else sch.create_schedule)(**args); created+=1
    except Exception as e: print("  FAIL %s: %s"%(fn,str(e)[:50]))
print("scheduled %d orphan engines on EventBridge Scheduler"%created)
for fn,cron in orphans: print("  %s  %s"%(fn,cron))
print("DONE 2422")
