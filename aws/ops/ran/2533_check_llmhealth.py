import boto3
lam=boto3.client("lambda","us-east-1")
try:
    c=lam.get_function_configuration(FunctionName="justhodl-llm-health")
    print("EXISTS — state:",c.get("State"),"| last update:",c.get("LastUpdateStatus"))
except lam.exceptions.ResourceNotFoundException:
    print("NOT DEPLOYED — needs standalone create")
print("DONE 2533")
