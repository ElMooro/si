import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=330,retries={"max_attempts":0}))
logs=boto3.client("logs","us-east-1"); FN="justhodl-hot-stocks-digest"
t=time.time(); r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
body=json.loads(r["Payload"].read().decode())
try: body=json.loads(body["body"]) if isinstance(body.get("body"),str) else body
except Exception: pass
emailed=body.get("emailed")
print("invoke ok (%.0fs); emailed ->"%(time.time()-t), emailed)
if emailed=="raafouis@gmail.com":
    print("\n*** VERIFIED *** — your gmail IS verified in SES; the brief just went to raafouis@gmail.com")
elif emailed=="reports@justhodl.ai":
    print("\n*** NOT YET VERIFIED *** — gmail send was rejected, fell back to reports@justhodl.ai")
else:
    print("\nemailed value:",emailed)
# pull the exact email log line(s) for proof
time.sleep(6)
try:
    GRP="/aws/lambda/"+FN
    st=logs.describe_log_streams(logGroupName=GRP,orderBy="LastEventTime",descending=True,limit=1)["logStreams"][0]["logStreamName"]
    ev=logs.get_log_events(logGroupName=GRP,logStreamName=st,limit=40,startFromHead=False)["events"]
    for e in ev:
        m=e["message"].rstrip()
        if "[email]" in m or "hot-stocks-digest" in m: print("  LOG:",m[:200])
except Exception as e: print("log read:",str(e)[:60])
print("DONE 2143")
