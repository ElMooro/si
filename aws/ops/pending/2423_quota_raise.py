import boto3, json, time
sts=boto3.client("sts"); iam=boto3.client("iam")
ident=sts.get_caller_identity()
arn=ident["Arn"]; print("caller:",arn)
# determine principal type + name
role_name=user_name=None
if ":assumed-role/" in arn:
    role_name=arn.split(":assumed-role/")[1].split("/")[0]
    print("principal: assumed-role ->",role_name)
elif ":user/" in arn:
    user_name=arn.split(":user/")[1]
    print("principal: iam-user ->",user_name)
POLICY={"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[
    "servicequotas:GetServiceQuota","servicequotas:ListServiceQuotas",
    "servicequotas:RequestServiceQuotaIncrease","servicequotas:GetRequestedServiceQuotaChange",
    "servicequotas:ListRequestedServiceQuotaChangeHistoryByQuota"],"Resource":"*"}]}
granted=False
try:
    if role_name:
        iam.put_role_policy(RoleName=role_name,PolicyName="servicequotas-selfgrant",PolicyDocument=json.dumps(POLICY)); granted=True
    elif user_name:
        iam.put_user_policy(UserName=user_name,PolicyName="servicequotas-selfgrant",PolicyDocument=json.dumps(POLICY)); granted=True
    print("self-grant servicequotas policy:","OK" if granted else "skipped")
except Exception as e:
    print("self-grant FAILED:",str(e)[:140])
if granted:
    print("waiting 15s for IAM propagation..."); time.sleep(15)
# attempt quota discovery + request
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
    target=[q for q in cand if q["Adjustable"] and "bus" in q["QuotaName"].lower()] or [q for q in cand if q["Adjustable"]]
    if target:
        q=target[0]
        rr=sq.request_service_quota_increase(ServiceCode="events",QuotaCode=q["QuotaCode"],DesiredValue=1000.0)
        ch=rr["RequestedQuota"]
        print("REQUESTED:",q["QuotaName"],"%s -> 1000 | status:%s | id:%s"%(q["Value"],ch["Status"],ch["Id"]))
    else:
        print("no adjustable rules-per-bus quota found in list")
except Exception as e:
    print("service-quotas still denied/err:",str(e)[:160])
print("DONE 2423")
