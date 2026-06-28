import boto3
lam=boto3.client("lambda","us-east-1")
try:
    c=lam.get_function(FunctionName="justhodl-crypto-options")["Configuration"]
    print("EXISTS now | LastModified:",c["LastModified"])
except Exception as e:
    print("STILL MISSING:",str(e)[:70])
print("DONE 2381")
