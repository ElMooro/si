import boto3, json
lam=boto3.client("lambda","us-east-1")
sq=boto3.client("service-quotas","us-east-1")
eb=boto3.client("events","us-east-1")

print("=== (1) deploy-create fix: no-env self-test created? ===")
created=False
try:
    c=lam.get_function_configuration(FunctionName="justhodl-deploy-selftest")
    created=True
    print("  CREATED OK -> fix works. state:",c.get("State"),"env:",c.get("Environment"))
except Exception as e:
    print("  STILL MISSING -> fix did NOT work:",str(e)[:90])

print("\n=== (2) EventBridge rules quota status (300 -> 1000 request) ===")
# current applied quota
try:
    q=sq.get_service_quota(ServiceCode="events",QuotaCode="L-244521F2")["Quota"]
    print("  APPLIED quota now:",q.get("Value"))
except Exception as e:
    d=sq.get_aws_default_service_quota(ServiceCode="events",QuotaCode="L-244521F2")["Quota"]
    print("  (no override yet) default:",d.get("Value"),"|",str(e)[:50])
# pending/just-resolved requests
try:
    hist=sq.list_requested_service_quota_change_history_by_quota(ServiceCode="events",QuotaCode="L-244521F2")["RequestedQuotas"]
    for r in sorted(hist,key=lambda x:x.get("Created",0),reverse=True)[:3]:
        print("  request",r.get("Id","")[:18],"| desired",r.get("DesiredValue"),"| status",r.get("Status"),"| created",str(r.get("Created"))[:19])
except Exception as e:
    print("  history err:",str(e)[:80])
# actual rules in use
try:
    rules=eb.list_rules(); 
    cnt=0; tok=None
    while True:
        r=eb.list_rules(NextToken=tok) if tok else eb.list_rules()
        cnt+=len(r.get("Rules",[])); tok=r.get("NextToken")
        if not tok: break
    print("  rules currently in account:",cnt,"/ (cap)")
except Exception as e:
    print("  rule count err:",str(e)[:60])

print("\n=== (3) cleanup self-test function ===")
if created:
    try: lam.delete_function(FunctionName="justhodl-deploy-selftest"); print("  deleted justhodl-deploy-selftest")
    except Exception as e: print("  delete err:",str(e)[:80])
print("DONE 2442")
