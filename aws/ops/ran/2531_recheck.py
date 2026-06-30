import boto3, json, io, zipfile, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# 1) is the DEPLOYED code free of mentioned_tickers?
loc=lam.get_function(FunctionName="justhodl-brain-sync")["Code"]["Location"]
src=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc).read())).read("lambda_function.py").decode()
print("deployed code still emits mentioned_tickers:", '"mentioned_tickers"' in src)
print("deployed code has knowledge-only note:", "KNOWLEDGE layer, not a watchlist" in src)
# 2) re-invoke + re-read
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(3)
br=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
print("brain.json has 'mentioned_tickers':", "mentioned_tickers" in br, "(want False)")
d=br.get("directive") or {}
print("directive knowledge intact:", bool(d), "| hard_rules:", len(d.get("hard_rules") or []), "| themes:", len(d.get("themes") or []))
print("DONE 2531")
