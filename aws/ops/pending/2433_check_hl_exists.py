import boto3
lam=boto3.client("lambda","us-east-1")
try:
    c=lam.get_function_configuration(FunctionName="justhodl-hyperliquid-perps")
    print("EXISTS | LastModified:",c["LastModified"],"| state:",c.get("State"))
except Exception as e:
    print("NOT FOUND:",str(e)[:80])
print("DONE 2433")
