import boto3, json
iam=boto3.client("iam"); sch=boto3.client("scheduler","us-east-1"); lam=boto3.client("lambda","us-east-1")
ACCT="857687956942"
role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
print("scheduler role:",role)
# existing schedule names
existing=set(); tok=None
while True:
    kw={"MaxResults":100}
    if tok: kw["NextToken"]=tok
    r=sch.list_schedules(**kw); existing.update(s["Name"] for s in r["Schedules"]); tok=r.get("NextToken")
    if not tok: break
print("existing scheduler schedules:",len(existing))
def upsert(fn, expr):
    name=fn.replace("justhodl-","")+"-sched"
    farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
    args=dict(Name=name, ScheduleExpression=expr, FlexibleTimeWindow={"Mode":"OFF"},
              Target={"Arn":farn,"RoleArn":role}, State="ENABLED")
    if name in existing:
        sch.update_schedule(**args); act="updated"
    else:
        sch.create_schedule(**args); act="created"
    d=sch.get_schedule(Name=name)
    print("  %s: %s %s (%s)"%(name, act, d["ScheduleExpression"], d["State"]))
print("=== watchdogs onto EventBridge Scheduler ===")
upsert("justhodl-fleet-freshness-monitor","rate(30 minutes)")
upsert("justhodl-fleet-error-monitor","rate(15 minutes)")
upsert("justhodl-fleet-monitor","rate(3 hours)")
print("DONE 2418")
