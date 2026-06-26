import boto3, json
from datetime import datetime, timezone
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
try:
    c=lam.get_function(FunctionName="justhodl-scarcity-radar")["Configuration"]
    print("Lambda EXISTS: state",c.get("State"),"| last update",c.get("LastUpdateStatus"))
except Exception as e: print("Lambda MISSING:",str(e)[:50])
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/scarcity-radar.json")["Body"].read())
    ga=d.get("generated_at")
    print("output EXISTS, generated_at:",ga,"| counts:",json.dumps(d.get("counts")))
    print("top stealth shortages:",[(r.get("ticker"),r.get("scarcity"),r.get("stealth"),r.get("vertical")) for r in (d.get("stealth_shortage_board") or [])[:5]])
except Exception as e: print("output MISSING:",str(e)[:50])
print("DONE 2230")
