import boto3, json
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1"); iam=boto3.client("iam"); sch=boto3.client("scheduler","us-east-1")
now=datetime.now(timezone.utc)
# 1) confirm freshness manifest fresh now
try:
    h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/_freshness-manifest.json")
    print("freshness-manifest age now: %.1fh (was 168h)"%((now-h["LastModified"]).total_seconds()/3600))
except Exception as e: print("manifest:",str(e)[:60])
# 2) any role trusting scheduler.amazonaws.com?
sched_roles=[]
try:
    paginator=iam.get_paginator("list_roles")
    for pg in paginator.paginate():
        for r in pg["Roles"]:
            adp=r.get("AssumeRolePolicyDocument",{})
            if "scheduler.amazonaws.com" in json.dumps(adp): sched_roles.append(r["RoleName"])
    print("roles trusting scheduler.amazonaws.com:",sched_roles or "NONE")
    # lambda-execution-role trust
    try:
        lr=iam.get_role(RoleName="lambda-execution-role")["Role"]
        td=json.dumps(lr["AssumeRolePolicyDocument"])
        print("lambda-execution-role trusts: lambda=%s scheduler=%s | can_update_trust=?"%("lambda.amazonaws.com" in td,"scheduler.amazonaws.com" in td))
    except Exception as e: print("lambda-execution-role:",str(e)[:60])
except Exception as e: print("iam list err:",str(e)[:80])
# 3) existing EventBridge Scheduler schedules?
try:
    sl=sch.list_schedules(MaxResults=20).get("Schedules",[])
    print("existing EventBridge Scheduler schedules:",len(sl),[s["Name"] for s in sl[:8]])
except Exception as e: print("scheduler list err:",str(e)[:80])
# 4) can we create an IAM role for scheduler? probe permission cheaply via simulate-less: try get_account_summary
try:
    iam.create_role  # attribute exists; try a dry create with invalid to see permission? skip—just report
    print("note: will attempt role creation in next step if needed")
except Exception as e: print(e)
print("DONE 2417")
