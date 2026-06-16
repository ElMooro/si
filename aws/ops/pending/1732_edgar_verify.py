import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
print("invoking justhodl-edgar-authority (async)...")
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/edgar-authority.json")["Body"].read()).get("generated_at")
except: before=None
lam.invoke(FunctionName="justhodl-edgar-authority",InvocationType="Event")
d=None
for i in range(13):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/edgar-authority.json")["Body"].read())
        if d.get("generated_at")!=before: break
    except: pass
if not d or d.get("generated_at")==before:
    print("no refresh yet — check logs"); raise SystemExit
print(f"elapsed={d.get('elapsed_s')}s | ncav_coverage={d.get('ncav_coverage')} (companies with EDGAR current-assets)")
print(f"\nNET-NETS: total={d.get('n_net_nets')} classic(<2/3 NCAV)={d.get('n_classic_net_nets')}")
for x in d.get("net_nets",[])[:10]:
    print(f"  {x['ticker']:6} {(x['name'] or '')[:22]:22} disc={x['discount_pct']:5}%  mc=${x['market_cap_m']:.0f}M ncav=${x['ncav_m']:.0f}M classic={x['classic_net_net']} sector={x['sector']}")
cc=d.get("crosscheck",{})
print(f"\nCROSS-CHECK: checked={cc.get('n_checked')} clean={cc.get('n_clean')} flagged={cc.get('n_flagged')}")
for c in cc.get("flagged",[])[:8]:
    print(f"  {c['ticker']:6} fy{c['fy']} flags={c['flags']}  filing_rev=${(c['filing']['revenue'] or 0)/1e9:.2f}B fmp_rev=${(c['fmp']['revenue'] or 0)/1e9:.2f}B")
print("clean sample:", cc.get("sample_clean",[])[:12])
