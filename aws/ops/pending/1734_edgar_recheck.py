import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/edgar-authority.json")["Body"].read()).get("generated_at")
lam.invoke(FunctionName="justhodl-edgar-authority",InvocationType="Event")
d=None
for i in range(13):
    time.sleep(20)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/edgar-authority.json")["Body"].read())
    if d.get("generated_at")!=before: break
print(f"elapsed={d.get('elapsed_s')}s")
print(f"NET-NETS credible={d.get('n_net_nets')} (raw={d.get('n_net_nets_raw')}) classic={d.get('n_classic_net_nets')}")
for x in d.get("net_nets",[])[:10]:
    print(f"  {x['ticker']:6} {(x['name'] or '')[:24]:24} disc={x['discount_pct']:5}% mc=${x['market_cap_m']:.0f}M ncav=${x['ncav_m']:.0f}M classic={x['classic_net_net']} {x['sector']}")
cc=d.get("crosscheck",{})
print(f"\nCROSS-CHECK checked={cc.get('n_checked')} clean={cc.get('n_clean')} flagged={cc.get('n_flagged')}")
for c in cc.get("flagged",[])[:10]:
    print(f"  {c['ticker']:6} fy{c['fy']} {c['flags']}")
print("clean sample:", cc.get("sample_clean",[])[:14])
