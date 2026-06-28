import boto3, json, time, subprocess, os
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1"); ev=boto3.client("events","us-east-1")
sch=boto3.client("scheduler","us-east-1"); lam=boto3.client("lambda","us-east-1"); iam=boto3.client("iam")
ACCT="857687956942"; now=datetime.now(timezone.utc)
role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
REPO=os.environ.get("GITHUB_WORKSPACE",".")
# scheduled-function index: from EventBridge rule targets + Scheduler schedule names
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
def is_scheduled(fn): return fn in ruled or (fn.replace("justhodl-","")+"-sched") in schednames
# stale live outputs
objs=[]; tok=None
while True:
    kw={"Bucket":"justhodl-dashboard-live","MaxKeys":1000}
    if tok: kw["ContinuationToken"]=tok
    r=s3.list_objects_v2(**kw)
    for o in r.get("Contents",[]):
        k=o["Key"]
        if k.endswith(".json") and (("/" not in k) or (k.startswith("data/") and k.count("/")==1)):
            objs.append((k,(now-o["LastModified"]).total_seconds()/3600))
    tok=r.get("NextContinuationToken")
    if not tok: break
skip=("history","archive","snapshot","backup","2024","2025","2026","config","user-","watchlist","feedback","manifest.json","data.json","peek","-data.json","predictions","khalid-config","ka-config")
stale=[(k,a) for k,a in objs if a>=49 and not any(w in k.lower() for w in skip)]
print("triage candidates (stale, non-excluded): %d"%len(stale))
scheduled_now=[]; runtime_issue=[]; orphan_file=[]
for k,a in sorted(stale,key=lambda x:-x[1]):
    fname=k.split("/")[-1]
    try:
        g=subprocess.run(["grep","-rl",fname,"aws/lambdas"],cwd=REPO,capture_output=True,text=True,timeout=30)
        writers=[p.split("/")[2] for p in g.stdout.strip().split("\n") if "/source/" in p]
    except Exception: writers=[]
    writers=sorted(set(writers))
    if not writers:
        orphan_file.append((k,a)); continue
    eng=writers[0]
    cfgp=os.path.join(REPO,"aws/lambdas",eng,"config.json")
    cron=None
    if os.path.exists(cfgp):
        try:
            c=json.load(open(cfgp)); sc=c.get("schedule") or {}; cron=sc.get("cron")
        except Exception: pass
    if is_scheduled(eng):
        runtime_issue.append((k,a,eng)); continue
    if cron:
        name=eng.replace("justhodl-","")+"-sched"
        farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,eng)
        try:
            args=dict(Name=name,ScheduleExpression=cron,FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED")
            (sch.update_schedule if name in schednames else sch.create_schedule)(**args)
            lam.invoke(FunctionName=eng,InvocationType="Event",Payload=b"{}")
            scheduled_now.append((eng,cron,k)); schednames.add(name)
        except Exception as e:
            runtime_issue.append((k,a,eng+" sched-err:"+str(e)[:40]))
    else:
        orphan_file.append((k,a,eng+" (no config cron)"))
print("\n=== SCHEDULED on Scheduler + triggered (%d) ==="%len(scheduled_now))
for eng,cron,k in scheduled_now: print("  %s  %s  <- %s"%(eng,cron,k.split('/')[-1]))
print("\n=== ALREADY-SCHEDULED but stale = runtime issue, needs code look (%d) ==="%len(runtime_issue))
for x in runtime_issue[:25]: print("  %.0fh  %s  [%s]"%(x[1],x[0].split('/')[-1],x[2]))
print("\n=== no writer found = retire candidate (%d) ==="%len(orphan_file))
for x in orphan_file[:25]: print("  %.0fh  %s"%(x[1],x[0]))
print("DONE 2420")
