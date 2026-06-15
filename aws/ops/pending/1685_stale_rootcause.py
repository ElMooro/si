import boto3
from datetime import datetime, timezone, timedelta
lam=boto3.client("lambda",region_name="us-east-1"); ev=boto3.client("events",region_name="us-east-1"); cw=boto3.client("cloudwatch",region_name="us-east-1")
producers={
 "justhodl-page-ai-commentary":"ai-commentary/* (10 feeds)",
 "justhodl-smart-money-cluster":"13f-positions.json",
 "justhodl-allocator":"allocator.json",
 "justhodl-redflag-alerter":"8k-filings.json",
 "justhodl-sec-10kq":"10kq-filings.json",
 "justhodl-13f-price-divergence":"13f-price-divergence.json",
 "justhodl-activist-13d":"activist-13d-names.json",
 "justhodl-alert-router":"alert-history.json",
}
now=datetime.now(timezone.utc); start=now-timedelta(days=14)
def daily(name,metric):
    r=cw.get_metric_statistics(Namespace="AWS/Lambda",MetricName=metric,
        Dimensions=[{"Name":"FunctionName","Value":name}],StartTime=start,EndTime=now,Period=86400,Statistics=["Sum"])
    pts=sorted(r.get("Datapoints",[]),key=lambda x:x["Timestamp"])
    return [(p["Timestamp"].strftime("%m-%d"),int(p["Sum"])) for p in pts]
for fn,feed in producers.items():
    print("="*70)
    print(f"{fn}  ->  {feed}")
    try:
        cfg=lam.get_function_configuration(FunctionName=fn); arn=cfg["FunctionArn"]
        print(f"  last_modified={cfg['LastModified']}")
    except Exception as e:
        print(f"  LAMBDA MISSING/ERR: {str(e)[:80]}"); continue
    # rules targeting this lambda
    try:
        rules=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
        if not rules: print("  rules: NONE target this function")
        for rn in rules:
            r=ev.describe_rule(Name=rn)
            print(f"  rule {rn}: State={r.get('State')} sched={r.get('ScheduleExpression')}")
    except Exception as e:
        print(f"  rules err: {str(e)[:80]}")
    inv=daily(fn,"Invocations"); err=daily(fn,"Errors")
    print(f"  invocations(14d): {inv}")
    if err: print(f"  errors(14d): {err}")
    tot_i=sum(v for _,v in inv); tot_e=sum(v for _,v in err)
    # classify
    if tot_i==0: cls="NO INVOCATIONS in 14d -> rule disabled or removed"
    elif tot_e>=tot_i*0.5 and tot_e>0: cls="INVOKED BUT ERRORING"
    else: cls="invoked OK (stale feed may be downstream/write issue)"
    print(f"  >>> {tot_i} inv / {tot_e} err in 14d  =>  {cls}")
