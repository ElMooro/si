import json, boto3
lam=boto3.client("lambda",region_name="us-east-1")
import json as j
fn=j.load(open("aws/lambdas/justhodl-sovereign-stress/config.json")).get("function_name","justhodl-sovereign-stress")
print("real function_name:",fn)
try:
    r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:160])
except Exception as e: print("err:",str(e)[:160])
