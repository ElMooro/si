import boto3, json, datetime
logs=boto3.client("logs","us-east-1"); ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
B="justhodl-dashboard-live"
def tail(fn,n=6):
    try:
        lg="/aws/lambda/"+fn
        st=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        if not st: return "no log streams"
        e=logs.get_log_events(logGroupName=lg,logStreamName=st[0]["logStreamName"],limit=15,startFromHead=False)["events"]
        return " || ".join(x["message"].strip().replace("\n"," ")[:130] for x in e if x["message"].strip())[-700:]
    except Exception as ex: return "log err "+str(ex)[:60]
def sched_of(fn):
    try:
        arn=lam.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
        rs=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
        out=[]
        for rn in rs:
            r=ev.describe_rule(Name=rn); out.append("%s[%s]=%s"%(rn,r.get("State"),r.get("ScheduleExpression")))
        return "; ".join(out) or "NO RULE TARGETING IT"
    except Exception as e: return "err "+str(e)[:50]
for fn in ["justhodl-polygon-fx-regime","justhodl-polygon-futures-curves"]:
    print("=== %s ==="%fn)
    print("  schedule:",sched_of(fn))
    print("  last log:",tail(fn))
    print()
# manual invoke to capture current error
from botocore.config import Config
lam2=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
for fn in ["justhodl-polygon-fx-regime","justhodl-polygon-futures-curves"]:
    try:
        r=lam2.invoke(FunctionName=fn,InvocationType="RequestResponse")
        print("INVOKE %s -> %s"%(fn,r["Payload"].read().decode()[:200]))
    except Exception as e: print("INVOKE %s ERR %s"%(fn,str(e)[:80]))
print("\n=== etf-flows/daily.json universe + rotation structure ===")
try:
    d=json.loads(s3.get_object(Bucket=B,Key="etf-flows/daily.json")["Body"].read())
    ms=d.get("metrics",[])
    print("daily.json: %d ETFs tracked"%len(ms))
    print("  sample tickers:", [m.get("ticker") for m in ms[:20]])
    ok=[m for m in ms if not m.get("error")]
    print("  with data:",len(ok))
    for m in ok[:5]:
        print("   %-6s label=%-14s z90=%s flow5d=%s"%(m.get("ticker"),m.get("signal_label"),m.get("flow_zscore_90d"),m.get("flow_5d_usd")))
except Exception as e: print("daily err:",str(e)[:80])
try:
    r=json.loads(s3.get_object(Bucket=B,Key="etf-flows/rotation.json")["Body"].read())
    print("rotation.json keys:",list(r.keys()))
    bc=r.get("by_category",{})
    print("  categories:",list(bc.keys())[:12])
except Exception as e: print("rotation err:",str(e)[:80])
