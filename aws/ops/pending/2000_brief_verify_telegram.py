"""ops 2000: verify-only (no force redeploy). Wait active, diagnose Telegram, invoke brief, show built sections via logs."""
import boto3, json, time, urllib.request, urllib.error
REGION="us-east-1"; FN="justhodl-morning-brief-tg"
lam=boto3.client("lambda",REGION); ssm=boto3.client("ssm",REGION); logs=boto3.client("logs",REGION)

# wait for any in-progress deploy to settle
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(5)
print("morning-brief state:",c.get("State"),c.get("LastUpdateStatus"),c.get("LastModified"))
env=c.get("Environment",{}).get("Variables",{}); tok=env.get("TELEGRAM_TOKEN","")
try: chat=ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
except Exception as e: chat=None; print("chat_id err",e)
print("token present:",bool(tok),"chat_id:",chat)

def tg(method,payload=None):
    url=f"https://api.telegram.org/bot{tok}/{method}"
    try:
        if payload is None: r=urllib.request.urlopen(url,timeout=15)
        else: r=urllib.request.urlopen(urllib.request.Request(url,data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"},method="POST"),timeout=15)
        return r.getcode(), r.read().decode()[:400]
    except urllib.error.HTTPError as e: return e.code,(e.read().decode()[:400] if hasattr(e,'read') else str(e))
    except Exception as e: return None,str(e)[:200]

print("\n=== TELEGRAM ===")
if tok:
    print("getMe:",*tg("getMe"))
    if chat: print("sendMessage:",*tg("sendMessage",{"chat_id":chat,"text":"JustHodl boom-watch test ✅"}))
else: print("no token")

print("\n=== INVOKE BRIEF ===")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("status",r["StatusCode"],"payload",r["Payload"].read().decode()[:400])
time.sleep(6)
lg=f"/aws/lambda/{FN}"
try:
    st=logs.describe_log_streams(logGroupName=lg,orderBy="LastEventTime",descending=True,limit=1)["logStreams"][0]["logStreamName"]
    ev=logs.get_log_events(logGroupName=lg,logStreamName=st,limit=40,startFromHead=False)["events"]
    for e in ev[-25:]:
        m=e["message"].rstrip()
        if any(k in m for k in ("Boom","Guidance","Earnings","send","Telegram","Markdown","error","Error","🚀","📊","📅")): print(" ",m[:200])
except Exception as e: print("logs err",e)
print("DONE 2000")
