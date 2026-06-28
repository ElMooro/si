import boto3, json
ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1")
ACCT="857687956942"
# 1) fleet-freshness-monitor: ensure EventBridge can invoke (rule already exists)
for fn,rule in [("justhodl-fleet-freshness-monitor","fleet-freshness-monitor-30min")]:
    # the deployed rule name may differ; find rules targeting this fn
    rarn="arn:aws:events:us-east-1:%s:rule/"%ACCT
    # check current policy
    try:
        pol=json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        has_eb=any("events.amazonaws.com" in json.dumps(s) for s in pol.get("Statement",[]))
    except Exception:
        has_eb=False
    print("%s: eventbridge-invoke-permission currently = %s"%(fn,has_eb))
    if not has_eb:
        # find the actual rule(s) targeting it
        targeting=[]
        tok=None
        while True:
            kw={"Limit":100}
            if tok: kw["NextToken"]=tok
            r=ev.list_rules(**kw)
            for ru in r["Rules"]:
                tg=ev.list_targets_by_rule(Rule=ru["Name"]).get("Targets",[])
                if any(fn in t.get("Arn","") for t in tg): targeting.append(ru["Name"])
            tok=r.get("NextToken")
            if not tok: break
        print("  rules targeting it:",targeting)
        for rn in targeting:
            try:
                lam.add_permission(FunctionName=fn,StatementId="eb-"+rn[:60],Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",SourceArn="arn:aws:events:us-east-1:%s:rule/%s"%(ACCT,rn))
                print("  + permission added for rule",rn)
            except lam.exceptions.ResourceConflictException: print("  permission already present for",rn)
            except Exception as e: print("  perm err:",str(e)[:60])
# 2) confirm fleet-monitor (3h) is the working baseline watchdog
try:
    d=ev.describe_rule(Name="fleet-monitor-3h"); print("fleet-monitor-3h:",d.get("State"),d.get("ScheduleExpression"))
except Exception as e: print("fleet-monitor-3h:",str(e)[:50])
# 3) request EventBridge rules-per-bus quota increase
sq=boto3.client("service-quotas","us-east-1")
try:
    quotas=sq.list_service_quotas(ServiceCode="events")["Quotas"]
    rq=[q for q in quotas if "ule" in q["QuotaName"] and ("bus" in q["QuotaName"].lower() or "rule" in q["QuotaName"].lower())]
    for q in rq[:5]: print("QUOTA:",q["QuotaName"],"code",q["QuotaCode"],"value",q["Value"],"adjustable",q["Adjustable"])
    target=[q for q in rq if q["Adjustable"] and q["Value"]<=300]
    if target:
        q=target[0]
        rr=sq.request_service_quota_increase(ServiceCode="events",QuotaCode=q["QuotaCode"],DesiredValue=600.0)
        print("REQUESTED increase ->",q["QuotaName"],"to 600 | status:",rr["RequestedQuota"]["Status"])
except Exception as e: print("quota err:",str(e)[:120])
print("DONE 2416")
