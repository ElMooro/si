import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
def get(k):
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
# 1) universe -> heatmap
print("invoking finviz-universe..."); lam.invoke(FunctionName="justhodl-finviz-universe",InvocationType="RequestResponse")
h=get("data/finviz-heatmap.json"); secs=h.get("sectors",[])
print(f"heatmap sectors={len(secs)}")
for x in secs[:3]+secs[-2:]:
    print(f"  {x['sector']:24} n={x['n']:4} 1M={x['avg_perf_m']:+.1f}% top={[t['ticker'] for t in x['top'][:3]]}")
# 2) signals -> confluence
print("\ninvoking finviz-signals (slow)..."); lam.invoke(FunctionName="justhodl-finviz-signals",InvocationType="RequestResponse")
sg=get("data/finviz-signals.json"); cf=sg.get("confluence",{})
for k,v in cf.items(): print(f"  confluence {k}: {len(v)} {v[:8]}")
# 3) insider -> finviz overlay
print("\ninvoking insider-radar..."); lam.invoke(FunctionName="justhodl-insider-radar",InvocationType="RequestResponse")
ir=get("data/insider-radar.json")
print(f"  finviz_buys={len(ir.get('finviz_buys',[]))} finviz_sells={len(ir.get('finviz_sells',[]))} confirm={ir.get('finviz_buy_confirm')}")
for ln in ir.get("diagnostics",[]):
    if "finviz" in ln.lower(): print("  DIAG:",ln)
