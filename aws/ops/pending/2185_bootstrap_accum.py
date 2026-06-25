import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=900,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
print("bootstrap invoke #1 (backfilling grouped-daily, may take minutes)...")
t=time.time()
try:
    r=lam.invoke(FunctionName="justhodl-accumulation-radar",InvocationType="RequestResponse")
    print("invoke1:",r["StatusCode"],"in",round(time.time()-t),"s |",r["Payload"].read().decode()[:160])
except Exception as e: print("invoke1 err:",str(e)[:100])
b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_cycle/pv.json")["Body"].read())
print("buffer days:",len(b.get("dates",[])),"universe:",len(b.get("universe",[])))
# second pass if buffer not full
if len(b.get("dates",[]))<150:
    print("buffer not full, invoke #2...")
    t=time.time()
    try:
        r=lam.invoke(FunctionName="justhodl-accumulation-radar",InvocationType="RequestResponse")
        print("invoke2:",r["StatusCode"],"in",round(time.time()-t),"s |",r["Payload"].read().decode()[:160])
    except Exception as e: print("invoke2 err:",str(e)[:100])
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/_cycle/pv.json")["Body"].read())
    print("buffer days now:",len(b.get("dates",[])))
print("DONE 2185")
