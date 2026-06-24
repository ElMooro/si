import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
ACC="857687956942"

# ===== Fix #2 completion: give ma200-reclaim its own rule, retire the orphan =====
MA="justhodl-ma200-reclaim"; MA_ARN=lam.get_function(FunctionName=MA)["Configuration"]["FunctionArn"]
rule="justhodl-ma200-reclaim-daily"
try:
    ev.put_rule(Name=rule,ScheduleExpression="cron(30 21 ? * MON-FRI *)",State="ENABLED")
    try: lam.add_permission(FunctionName=MA,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except Exception: pass
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":MA_ARN}])
    print(f"created proper rule {rule} -> ma200-reclaim")
    # remove ma200 from the orphan + delete it
    orph="aiapi-hourly-collection"
    tg=ev.list_targets_by_rule(Rule=orph).get("Targets",[])
    ids=[t["Id"] for t in tg if MA in t["Arn"]]
    if ids: ev.remove_targets(Rule=orph,Ids=ids,Force=True)
    leftover=ev.list_targets_by_rule(Rule=orph).get("Targets",[])
    if not leftover:
        ev.delete_rule(Name=orph,Force=True); print(f"retired orphan rule {orph} (+1 slot freed)")
    else: print(f"orphan {orph} still has {len(leftover)} targets — left intact")
except Exception as e: print("rehome:",str(e)[:80])

# ===== crypto-ma200: ensure scheduled, then bootstrap =====
CR="justhodl-crypto-ma200"
try:
    CR_ARN=lam.get_function(FunctionName=CR)["Configuration"]["FunctionArn"]
    try: ev.describe_rule(Name="justhodl-crypto-ma200-daily"); print("crypto rule present (deploy made it)")
    except Exception:
        cr="justhodl-crypto-ma200-daily"
        ev.put_rule(Name=cr,ScheduleExpression="cron(45 0 * * ? *)",State="ENABLED")
        try: lam.add_permission(FunctionName=CR,StatementId="ev-"+cr,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{cr}")
        except Exception: pass
        ev.put_targets(Rule=cr,Targets=[{"Id":"1","Arn":CR_ARN}]); print("created crypto rule")
    for _ in range(40):
        c=lam.get_function(FunctionName=CR)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
        time.sleep(3)
    sess=0
    for k in range(3):
        lam.invoke(FunctionName=CR,InvocationType="RequestResponse")
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-ma200.json")["Body"].read())
        sess=d.get("buffer_sessions",0)
        print(f"invoke {k+1}: days={sess} status={d.get('status','READY')} fetched={d.get('fetched_this_run')} dur={d.get('duration_s')}s")
        if sess>=205: break
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-ma200.json")["Body"].read())
    print("\ncrypto universe:",d.get("universe"),"| counts:",json.dumps(d.get("counts",{})))
    for bk,lbl in [("retest_held","RETEST-HELD"),("fresh_breakouts_above","FRESH ABOVE"),("fresh_breakdowns_below","FRESH BELOW")]:
        arr=d.get(bk,[])
        if arr:
            print(f"\n{lbl}: {len(arr)}")
            for r in arr[:7]:
                print(f"   {r['ticker']:<7} {r.get('state',''):<15} px {r['price']} vs ma200 {r['ma200']} ({r['dist_pct']:+}%) slope {r.get('ma200_slope_pct')}% gc={r.get('ma50_above_ma200')}")
except Exception as e:
    import traceback; print("crypto:",str(e)[:100])
print("DONE 2167")
