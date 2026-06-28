import boto3, json, time
iam=boto3.client("iam"); ACCT="857687956942"; USER="github-actions-justhodl"
GROUP="justhodl-quota-admins"; parn="arn:aws:iam::%s:policy/justhodl-servicequotas-events"%ACCT
step="create-group"
try:
    try:
        iam.create_group(GroupName=GROUP); print("created group",GROUP)
    except iam.exceptions.EntityAlreadyExistsException:
        print("group exists",GROUP)
    step="attach-group-policy"; iam.attach_group_policy(GroupName=GROUP,PolicyArn=parn); print("attached policy to group")
    step="add-user"; iam.add_user_to_group(GroupName=GROUP,UserName=USER); print("added",USER,"to group")
    print("waiting 20s for propagation..."); time.sleep(20)
except Exception as e:
    print("IAM step '%s' FAILED:"%step,str(e)[:170]); print("DONE 2425"); raise SystemExit
sq=boto3.client("service-quotas","us-east-1")
try:
    quotas=[]; tok=None
    while True:
        kw={"ServiceCode":"events","MaxResults":100}
        if tok: kw["NextToken"]=tok
        r=sq.list_service_quotas(**kw); quotas.extend(r["Quotas"]); tok=r.get("NextToken")
        if not tok: break
    cand=[q for q in quotas if "rule" in q["QuotaName"].lower()]
    for q in cand: print("  QUOTA:",q["QuotaName"],"| code",q["QuotaCode"],"| value",q["Value"],"| adjustable",q["Adjustable"])
    target=[q for q in cand if q["Adjustable"] and "bus" in q["QuotaName"].lower()] or [q for q in cand if q["Adjustable"] and "Rules" in q["QuotaName"]]
    if target:
        q=target[0]
        rr=sq.request_service_quota_increase(ServiceCode="events",QuotaCode=q["QuotaCode"],DesiredValue=1000.0)
        ch=rr["RequestedQuota"]
        print("REQUESTED: %s  %s -> 1000 | status:%s | id:%s"%(q["QuotaName"],q["Value"],ch["Status"],ch.get("Id")))
    else:
        print("no adjustable rules-per-bus quota found")
except Exception as e:
    print("quota request err:",str(e)[:180])
print("DONE 2425")
