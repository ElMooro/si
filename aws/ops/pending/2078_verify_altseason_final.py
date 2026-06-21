import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-altseason")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
try:
    r=lam.invoke(FunctionName="justhodl-altseason",InvocationType="RequestResponse")
    print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:120])
except Exception as e:
    print("invoke note:",str(e)[:80])
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/altseason.json")["Body"].read())
votes={v["metric"]:v for v in d.get("votes",[])}
nv=votes.get("Alt 200DMA reclaim breadth")
st=d.get("breadth_200dma_study",{})
print(f"\nphase {d['composite']['phase']} score {d['composite']['score']}")
print(f"\n200DMA vote: value {nv['value']} | vote {nv['vote']} | WEIGHT {nv['weight']} (0=diagnostic, excluded from score)")
print(f"study verdict: {st.get('verdict')} | edge {st.get('edge_pp')}pp")
print(f"score-affecting weight total: {sum(v['weight'] for v in d['votes'])} | 200dma contributes: {nv['weight']}")
ai=d.get("ai_brief",{})
print(f"\nAI tribunal error: {ai.get('error')} | verdict: {str(ai.get('verdict'))[:120]}")
print("DONE 2078")
