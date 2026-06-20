"""1968 — can lambda-execution-role self-heal schedules? Simulate key actions."""
import boto3, json
iam=boto3.client("iam")
ROLE_ARN="arn:aws:iam::857687956942:role/lambda-execution-role"
actions=["events:PutRule","events:PutTargets","events:ListRules","events:ListTargetsByRule",
         "events:DescribeRule","lambda:AddPermission","lambda:RemovePermission",
         "lambda:InvokeFunction","lambda:GetFunctionConfiguration","lambda:GetPolicy",
         "ssm:GetParameter"]
try:
    res=iam.simulate_principal_policy(PolicySourceArn=ROLE_ARN, ActionNames=actions)
    for r in res["EvaluationResults"]:
        print(f"  {r['EvalActionName']:<34} {r['EvalDecision']}")
except Exception as e:
    print("simulate err:", e)
    # fallback: dump policies
    print("\nattached managed:")
    for p in iam.list_attached_role_policies(RoleName="lambda-execution-role")["AttachedPolicies"]:
        print("  ", p["PolicyName"], p["PolicyArn"])
    print("inline:")
    for n in iam.list_role_policies(RoleName="lambda-execution-role")["PolicyNames"]:
        print("  ", n)
print("DONE 1968")
