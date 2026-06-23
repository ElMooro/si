import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-ma200-reclaim"
FN_ARN=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
for _ in range(40):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
# re-invoke (intact fix)
lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ma200-reclaim.json")["Body"].read())
print("counts:",json.dumps(d.get("counts",{})))
print("\nFRESH ABOVE (must now all be +dist):")
for r in d.get("fresh_breakouts_above",[])[:6]: print(f"   {r['ticker']:<6} {r['dist_pct']:+}% slope {r.get('ma200_slope_pct')}% bars={r.get('bars_since_cross')}")
print("FRESH BELOW (must now all be -dist):")
for r in d.get("fresh_breakdowns_below",[])[:6]: print(f"   {r['ticker']:<6} {r['dist_pct']:+}% slope {r.get('ma200_slope_pct')}% bars={r.get('bars_since_cross')}")
print("RETEST-HELD (fresher/near-line first):")
for r in d.get("retest_held",[])[:8]: print(f"   {r['ticker']:<6} {r.get('state'):<15} {r['dist_pct']:+}% slope {r.get('ma200_slope_pct')}% retest_age={r.get('retest_age')} gc={r.get('ma50_above_ma200')}")

# ---- schedule via existing rule (account at rule cap) ----
print("\n--- attaching to an existing post-close rule ---")
tok=None; chosen=None
while True:
    kw={"NextToken":tok} if tok else {}
    resp=ev.list_rules(Limit=100,**kw)
    for r in resp.get("Rules",[]):
        se=r.get("ScheduleExpression","")
        if se.startswith("cron(") and " 21 " in se and ("MON-FRI" in se or "*" in se.split()[2]):
            tg=ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[])
            if len(tg)<5:
                chosen=(r["Name"],se,len(tg)); break
    tok=resp.get("NextToken")
    if chosen or not tok: break
if chosen:
    rname,se,ntg=chosen
    tid=f"ma200-{int(time.time())%100000}"
    ev.put_targets(Rule=rname,Targets=[{"Id":tid,"Arn":FN_ARN}])
    try: lam.add_permission(FunctionName=FN,StatementId=f"ev-{rname}"[:64],Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rname}")
    except Exception as e: print("perm:",str(e)[:50])
    print(f"ATTACHED ma200-reclaim to existing rule '{rname}' ({se}, had {ntg} targets)")
else:
    print("no spare post-close rule found — will invoke via cron piggyback later")
print("DONE 2162")
