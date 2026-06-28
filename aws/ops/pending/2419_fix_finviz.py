import boto3, json, time
from datetime import datetime, timezone
iam=boto3.client("iam"); sch=boto3.client("scheduler","us-east-1"); lam=boto3.client("lambda","us-east-1")
s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
ACCT="857687956942"; role=iam.get_role(RoleName="justhodl-scheduler-role")["Role"]["Arn"]
existing=set(); tok=None
while True:
    kw={"MaxResults":100}
    if tok: kw["NextToken"]=tok
    r=sch.list_schedules(**kw); existing.update(s["Name"] for s in r["Schedules"]); tok=r.get("NextToken")
    if not tok: break
def upsert(fn, expr):
    name=fn.replace("justhodl-","")+"-sched"
    farn="arn:aws:lambda:us-east-1:%s:function:%s"%(ACCT,fn)
    args=dict(Name=name,ScheduleExpression=expr,FlexibleTimeWindow={"Mode":"OFF"},Target={"Arn":farn,"RoleArn":role},State="ENABLED")
    (sch.update_schedule if name in existing else sch.create_schedule)(**args)
    print("  scheduled %s -> %s"%(name,expr))
print("=== schedule finviz engines on Scheduler ===")
upsert("justhodl-finviz-universe","cron(0 14,22 * * ? *)")
upsert("justhodl-finviz-signals","cron(0 14,18,21 * * ? *)")
# trigger now (async)
for fn in ["justhodl-finviz-universe","justhodl-finviz-signals"]:
    lam.invoke(FunctionName=fn,InvocationType="Event",Payload=b"{}")
print("triggered both async; waiting 100s...")
time.sleep(100)
now=datetime.now(timezone.utc)
for k in ["data/finviz-universe.json","data/finviz-short.json","data/finviz-heatmap.json","data/finviz-earnings-calendar.json","data/finviz-etf-flows.json","data/finviz-signals.json"]:
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key=k)
        print("  %5.1fh  %s"%((now-h["LastModified"]).total_seconds()/3600,k))
    except Exception as e: print("  ???  %s (%s)"%(k,str(e)[:30]))
# finviz-signals errors?
try:
    grp="/aws/lambda/justhodl-finviz-signals"
    st=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
    ev=logs.get_log_events(logGroupName=grp,logStreamName=st[0]["logStreamName"],limit=25,startFromHead=False)["events"]
    errs=[e["message"].strip()[:150] for e in ev if any(w in e["message"] for w in ("Error","Traceback","timed out","Exception","[ERROR]"))]
    print("  finviz-signals recent errors:",errs[-3:] if errs else "none (clean run)")
except Exception as e: print("  logs err:",str(e)[:60])
print("DONE 2419")
