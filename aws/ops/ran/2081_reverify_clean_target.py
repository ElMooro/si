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
print("\nphase",d["composite"]["phase"],"score",d["composite"]["score"])
b=d.get("breadth_200dma_study",{})
print(f"\n[200DMA breadth] CLEAN target: fwd90 EW-basket {b.get('fwd90_high_med')}% when ≥50 vs {b.get('fwd90_low_med')}% when ≤20 | edge {b.get('edge_pp')}pp | n {b.get('n_high')}/{b.get('n_low')} | verdict {b.get('verdict')} | weight {b.get('weight_assigned')}")
s=d.get("stablecoin_study",{})
print(f"[stablecoin]     CLEAN target: fwd90 EW-basket {s.get('fwd90_expand_med')}% expanding vs {s.get('fwd90_contract_med')}% contracting | edge {s.get('edge_pp')}pp | n {s.get('n_expand')}/{s.get('n_contract')} | verdict {s.get('verdict')} | weight {s.get('weight_assigned')}")
votes={v["metric"]:v for v in d.get("votes",[])}
for m in ("Alt 200DMA reclaim breadth","Stablecoin supply trend (dry powder)"):
    nv=votes.get(m)
    if nv: print(f"\nvote '{m}': {nv['vote']} weight {nv['weight']} | {nv['value']}")
print("\nscore-weight total:",sum(v['weight'] for v in d['votes']))
print("AI error:",d.get("ai_brief",{}).get("error"))
print("DONE 2081")
