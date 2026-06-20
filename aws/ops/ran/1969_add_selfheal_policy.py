"""1969 — grant lambda-execution-role a scoped self-heal policy so the watchdog
can rebuild dead schedule bindings. Least-privilege: only 7 mutate actions,
account-scoped to EventBridge rules + Lambda functions in this account."""
import boto3, json
iam=boto3.client("iam")
ROLE="lambda-execution-role"; POLICY="justhodl-schedule-selfheal"
ACCT="857687956942"; R="us-east-1"
doc={
  "Version":"2012-10-17",
  "Statement":[
    {"Sid":"SchedSelfHealEvents","Effect":"Allow",
     "Action":["events:PutRule","events:PutTargets","events:EnableRule","events:RemoveTargets"],
     "Resource":f"arn:aws:events:{R}:{ACCT}:rule/*"},
    {"Sid":"SchedSelfHealLambdaPerm","Effect":"Allow",
     "Action":["lambda:AddPermission","lambda:RemovePermission","lambda:GetPolicy"],
     "Resource":f"arn:aws:lambda:{R}:{ACCT}:function:*"}
  ]
}
try:
    iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY, PolicyDocument=json.dumps(doc))
    print("attached inline policy", POLICY, "to", ROLE)
except Exception as e:
    print("put_role_policy ERR:", type(e).__name__, e)
# verify via simulate
import time; time.sleep(5)
res=iam.simulate_principal_policy(PolicySourceArn=f"arn:aws:iam::{ACCT}:role/{ROLE}",
    ActionNames=["events:PutRule","events:PutTargets","lambda:AddPermission","lambda:RemovePermission","lambda:GetPolicy"])
for r in res["EvaluationResults"]:
    print(f"  {r['EvalActionName']:<28} {r['EvalDecision']}")
print("DONE 1969")
