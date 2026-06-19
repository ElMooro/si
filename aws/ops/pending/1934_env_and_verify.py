import boto3, json, time, urllib.request
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def env(fn):
    try: return lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
    except Exception as e: return {"_err":str(e)[:50]}
e13=env("justhodl-13f-positions")
print("13f-positions has TELEGRAM_TOKEN:", "TELEGRAM_TOKEN" in e13, "| CHAT:", "TELEGRAM_CHAT_ID" in e13)
rad=env("justhodl-capital-flow-radar")
print("radar current env keys:", list(rad.keys()))
# set telegram env on radar (merge, don't clobber)
TG="8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"; CHAT="8678089260"
merged=dict(rad); merged["TELEGRAM_TOKEN"]=TG; merged["TELEGRAM_CHAT_ID"]=CHAT
for _ in range(20):
    try:
        lam.update_function_configuration(FunctionName="justhodl-capital-flow-radar",Environment={"Variables":merged}); break
    except Exception as ex:
        if "ResourceConflict" in str(ex): time.sleep(4); continue
        print("env set err",str(ex)[:80]); break
for _ in range(30):
    c=lam.get_function_configuration(FunctionName="justhodl-capital-flow-radar")
    if c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("radar env now has telegram:", "TELEGRAM_TOKEN" in env("justhodl-capital-flow-radar"))
# invoke radar (first run seeds state silently)
try: print("invoke:",lam.invoke(FunctionName="justhodl-capital-flow-radar",InvocationType="RequestResponse")["Payload"].read().decode()[:130])
except Exception as ex: print("invoke err",str(ex)[:100])
time.sleep(2)
# state file created?
try:
    st=json.loads(s3.get_object(Bucket=B,Key="data/capital-flow-radar-state.json")["Body"].read())
    print("state seeded: pump=%d over=%d"%(len(st.get("pump",[])),len(st.get("over",[]))))
except Exception as ex: print("state err",str(ex)[:60])
# verify page live (GitHub Pages / CF)
for url in ["https://justhodl.ai/capital-flow-radar.html"]:
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0"})
        html=urllib.request.urlopen(req,timeout=15).read().decode("utf-8","ignore")
        ok="Capital Flow Radar" in html and "leveraged" in html.lower()
        print("PAGE %s -> %d bytes, markers present: %s"%(url,len(html),ok))
    except Exception as ex: print("page fetch %s err: %s"%(url,str(ex)[:80]))
