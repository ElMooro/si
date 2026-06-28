import boto3
lam=boto3.client("lambda","us-east-1"); sq=boto3.client("service-quotas","us-east-1")
print("=== (1) no-env self-test created? (proves create-fix) ===")
created=False
try:
    c=lam.get_function_configuration(FunctionName="justhodl-deploy-selftest")
    created=True; print("  CREATED OK -> FIX WORKS. state:",c.get("State"),"| env:",c.get("Environment"))
except Exception as e:
    print("  STILL MISSING -> fix failed:",str(e)[:90])
print("=== (2) EventBridge rules quota (300 -> 1000 request) ===")
try:
    q=sq.get_service_quota(ServiceCode="events",QuotaCode="L-244521F2")["Quota"]
    print("  APPLIED quota now:",q.get("Value"))
except Exception:
    d=sq.get_aws_default_service_quota(ServiceCode="events",QuotaCode="L-244521F2")["Quota"]
    print("  no override applied; default still:",d.get("Value"))
try:
    hist=sq.list_requested_service_quota_change_history_by_quota(ServiceCode="events",QuotaCode="L-244521F2")["RequestedQuotas"]
    if not hist: print("  no quota-change requests on record")
    for r in sorted(hist,key=lambda x:x.get("Created",0),reverse=True)[:3]:
        print("  req",r.get("Id","")[:20],"| desired",r.get("DesiredValue"),"| STATUS",r.get("Status"),"| created",str(r.get("Created"))[:19])
except Exception as e:
    print("  history err:",str(e)[:80])
print("=== (3) cleanup self-test fn ===")
if created:
    try: lam.delete_function(FunctionName="justhodl-deploy-selftest"); print("  deleted justhodl-deploy-selftest")
    except Exception as e: print("  delete err:",str(e)[:60])
else:
    print("  nothing to delete")
print("DONE 2443")
