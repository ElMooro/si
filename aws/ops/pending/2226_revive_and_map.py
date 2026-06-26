import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1")
FN="justhodl-supply-inflection-scanner"
# 1) REVIVE: create the missing daily schedule
RULE="justhodl-supply-inflection-daily"
try:
    ev.put_rule(Name=RULE,ScheduleExpression="cron(0 7 * * ? *)",State="ENABLED",Description="Daily supply-input inflection scan (memory/lithium/power/etc.)")
    arn=lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
    ev.put_targets(Rule=RULE,Targets=[{"Id":"1","Arn":arn}])
    try: lam.add_permission(FunctionName=FN,StatementId="evt-supplyinflect",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
    except Exception as e: print("perm:",str(e)[:30])
    print("KEYSTONE REVIVED: schedule created ->", RULE)
except Exception as e: print("schedule FAIL:", str(e)[:80])
# 2) read ALL input signals (so the beneficiary map is comprehensive)
lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
print("\nsupply-inflection.json keys:", list(d.keys())[:12])
for L in ("signals","all_signals","inflections","scored"):
    arr=d.get(L)
    if isinstance(arr,list) and arr:
        print(f"\nALL INPUT SIGNALS ('{L}', n={len(arr)}):")
        for s in sorted(arr,key=lambda x:-(x.get('score') or x.get('inflection_score') or 0)):
            nm=s.get("name") or s.get("signal") or s.get("id")
            print(f"   {str(nm)[:24]:<24} score={s.get('score') or s.get('inflection_score')} dir={s.get('direction') or s.get('tightening')} chg90={s.get('chg_90d') or s.get('change_90d') or s.get('pct_90d')} themes={s.get('themes') or s.get('beneficiary_etfs') or s.get('etfs')}")
        break
# 3) bottleneck-boom candidate list key
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("\nbottleneck-boom keys:", list(b.keys()))
for k,v in b.items():
    if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0]):
        print(f"  candidate list '{k}' n={len(v)} keys={list(v[0].keys())[:8]}")
        for it in v[:4]: print("    ", it.get("ticker") or it.get("symbol"), {kk:it.get(kk) for kk in list(it.keys())[1:5]})
print("DONE 2226")
