import boto3, json, time
iam=boto3.client("iam"); ROLE="lambda-execution-role"; ACCT="857687956942"
r=iam.get_role(RoleName=ROLE)["Role"]
pb=r.get("PermissionsBoundary")
print("PermissionsBoundary:", pb.get("PermissionsBoundaryArn") if pb else "NONE")
print("inline policies:", iam.list_role_policies(RoleName=ROLE)["PolicyNames"])
# show our policy doc is present
try:
    d=iam.get_role_policy(RoleName=ROLE, PolicyName="justhodl-schedule-selfheal")
    print("selfheal policy present, actions:", [s["Action"] for s in d["PolicyDocument"]["Statement"]])
except Exception as e:
    print("get_role_policy err:", e)
print("\nre-simulate after propagation:")
time.sleep(20)
res=iam.simulate_principal_policy(PolicySourceArn=f"arn:aws:iam::{ACCT}:role/{ROLE}",
    ActionNames=["events:PutRule","lambda:AddPermission"],
    ResourceArns=[f"arn:aws:events:us-east-1:{ACCT}:rule/pairs-scanner-6hourly"])
for x in res["EvaluationResults"]:
    print(f"  {x['EvalActionName']:<22} {x['EvalDecision']}")
# also simulate without resource constraint
res2=iam.simulate_principal_policy(PolicySourceArn=f"arn:aws:iam::{ACCT}:role/{ROLE}",
    ActionNames=["events:PutRule","lambda:AddPermission"])
print(" no-resource:")
for x in res2["EvaluationResults"]:
    print(f"  {x['EvalActionName']:<22} {x['EvalDecision']}")
print("DONE 1970")
