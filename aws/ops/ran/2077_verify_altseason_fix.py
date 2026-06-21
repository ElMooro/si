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
    print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:140])
except Exception as e:
    print("invoke note:",str(e)[:90])
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/altseason.json")["Body"].read())
comp=d.get("composite",{})
print("\nphase:",comp.get("phase"),"| score",comp.get("score"),"| generated",d.get("generated_at","")[:19])
votes={v["metric"]:v for v in d.get("votes",[])}
nv=votes.get("Alt 200DMA reclaim breadth")
print("\nNEW VOTE — Alt 200DMA reclaim breadth:")
print(f"  value {nv['value']} | vote {nv['vote']} | weight {nv['weight']}\n  note: {nv['note']}" if nv else "  MISSING")
st=d.get("breadth_200dma_study",{})
print(f"\nBACKTEST: fwd90 {st.get('fwd90_high_med')}% (≥50) vs {st.get('fwd90_low_med')}% (≤20) | edge {st.get('edge_pp')}pp | n {st.get('n_high')}/{st.get('n_low')}")
ai=d.get("ai_brief",{})
print("\nAI TRIBUNAL: error =",ai.get("error"))
if not ai.get("error"):
    print("  verdict:",str(ai.get("verdict"))[:180])
    print("  watch_next:",ai.get("watch_next"))
b2=d.get("histories",{}).get("breadth_200dma",[])
print(f"\nbreadth_200dma history points: {len(b2)} | latest: {b2[-1] if b2 else '—'}")
print("DONE 2077")
