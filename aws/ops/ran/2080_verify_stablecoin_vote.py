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
print("\nphase",d["composite"]["phase"],"score",d["composite"]["score"],"| gen",d.get("generated_at","")[:19])
sup=d.get("stablecoin_supply",{}); st=d.get("stablecoin_study",{})
print(f"\nstablecoin supply: ${(sup.get('total_usd') or 0)/1e9:.0f}B | 30d {sup.get('chg_30d_pct')}% | 90d {sup.get('chg_90d_pct')}% | >90DMA {sup.get('above_90dma')}")
print(f"BACKTEST: fwd90 {st.get('fwd90_expand_med')}% expanding vs {st.get('fwd90_contract_med')}% contracting | edge {st.get('edge_pp')}pp | n {st.get('n_expand')}/{st.get('n_contract')} | verdict {st.get('verdict')} | weight {st.get('weight_assigned')}")
votes={v["metric"]:v for v in d.get("votes",[])}
nv=votes.get("Stablecoin supply trend (dry powder)")
print(f"\nNEW VOTE: value {nv['value']} | vote {nv['vote']} | weight {nv['weight']}" if nv else "VOTE MISSING")
if nv: print(f"  note: {nv['note'][:200]}")
ai=d.get("ai_brief",{})
print(f"\nAI tribunal error: {ai.get('error')}")
diag=[x for x in d.get("diagnostics",[]) if "stablecoin" in x.lower()]
print("diag:",diag)
print("DONE 2080")
