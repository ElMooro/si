import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
try: print("invoke:",lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse")["Payload"].read().decode()[:120])
except Exception as e: print("err",str(e)[:120])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/master-ranker.json")["Body"].read())
tt=d.get("top_tickers",[])
print("top_tickers:",len(tt))
print("\nTOP 12 with capital-flow overlay:")
for t in tt[:12]:
    mult=t.get("capital_flow_mult",1.0)
    flag="📈BOOST" if mult>1 else "📉PENALTY" if mult<1 else "—"
    print("  %-6s score=%-6s cf_mult=%-5s %s"%(t["ticker"],t["score"],mult,flag))
# show ones actually moved
moved=[t for t in tt if t.get("capital_flow_mult",1.0)!=1.0]
print("\n%d of top-25 had a capital-flow adjustment"%len(moved))
for t in moved[:6]:
    print("  %-6s cf_mult=%s | %s"%(t["ticker"],t.get("capital_flow_mult"),(t.get("rationale") or "")[-90:]))
