import boto3, json, time
iam=boto3.client("iam"); ACCT="857687956942"; USER="github-actions-justhodl"
POLICY={"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[
    "servicequotas:GetServiceQuota","servicequotas:ListServiceQuotas",
    "servicequotas:RequestServiceQuotaIncrease","servicequotas:GetRequestedServiceQuotaChange",
    "servicequotas:ListRequestedServiceQuotaChangeHistoryByQuota"],"Resource":"*"}]}
parn="arn:aws:iam::%s:policy/justhodl-servicequotas-events"%ACCT
step="create-policy"
try:
    try:
        r=iam.create_policy(PolicyName="justhodl-servicequotas-events",PolicyDocument=json.dumps(POLICY))
        parn=r["Policy"]["Arn"]; print("created managed policy:",parn)
    except iam.exceptions.EntityAlreadyExistsException:
        print("managed policy already exists:",parn)
    step="attach"
    iam.attach_user_policy(UserName=USER,PolicyArn=parn); print("attached to",USER)
    print("waiting 18s for propagation..."); time.sleep(18)
except Exception as e:
    print("IAM step '%s' FAILED:"%step,str(e)[:160]); print("DONE 2424"); raise SystemExit
# request quota
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
        # check existing pending request first
        rr=sq.request_service_quota_increase(ServiceCode="events",QuotaCode=q["QuotaCode"],DesiredValue=1000.0)
        ch=rr["RequestedQuota"]
        print("REQUESTED INCREASE: %s  %s -> 1000 | status:%s | case:%s"%(q["QuotaName"],q["Value"],ch["Status"],ch.get("Id")))
    else:
        print("no adjustable rules-per-bus quota found")
except Exception as e:
    print("quota request err:",str(e)[:180])
print("DONE 2424")
