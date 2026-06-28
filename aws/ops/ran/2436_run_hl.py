import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=90,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
fn="justhodl-hyperliquid-perps"
# wait for Active
for _ in range(20):
    st=lam.get_function_configuration(FunctionName=fn).get("State")
    if st=="Active": break
    time.sleep(3)
print("state:",st)
r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:220])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hyperliquid-perps.json")["Body"].read())
print("total OI: $%.2fB | BTC OI $%.2fB funding %s%%/yr premium %sbps | ETH $%.2fB | SOL $%.2fB"%(
    (d.get("total_oi_usd") or 0)/1e9,(d.get("btc",{}).get("oi_usd") or 0)/1e9,d.get("btc",{}).get("funding_ann_pct"),
    d.get("btc",{}).get("premium_bps"),(d.get("eth",{}).get("oi_usd") or 0)/1e9,(d.get("sol",{}).get("oi_usd") or 0)/1e9))
print("leverage_regime:",d.get("leverage_regime"),"| liq_proxy:",d.get("liq_pressure_proxy"),"| hist_n:",d.get("history_n"))
print("top OI:",[(x["coin"],"$%.1fB"%(x["oi_usd"]/1e9)) for x in (d.get("top_oi") or [])[:5]])
print("funding most_long:",d.get("funding_extremes",{}).get("most_long",[])[:3])
print("interp:",d.get("interpretation"))
# confirm schedule exists
sch=boto3.client("scheduler","us-east-1")
try: print("schedule state:",sch.get_schedule(Name="hyperliquid-perps-sched")["State"])
except Exception as e: print("sched err:",str(e)[:60])
print("DONE 2436")
