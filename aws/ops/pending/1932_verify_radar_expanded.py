import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
try: print("invoke:",lam.invoke(FunctionName="justhodl-capital-flow-radar",InvocationType="RequestResponse")["Payload"].read().decode()[:160])
except Exception as e: print("err",str(e)[:120])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/capital-flow-radar.json")["Body"].read())
print("n_complexes:",d.get("n_complexes"))
ls=d.get("leveraged_sentiment",{})
print("\nLEVERAGED RISK APPETITE:",ls.get("risk_appetite"),
      "| agg bull-lev inflow $%sM"%round((ls.get("aggregate_bull_lev_inflow_5d") or 0)/1e6,0),
      "| agg bear-lev inflow $%sM"%round((ls.get("aggregate_bear_lev_inflow_5d") or 0)/1e6,0))
print("\nMOST BULLISH leveraged positioning:")
for r in ls.get("most_bullish_positioning",[])[:6]:
    print("  %-16s net $%sM (bull $%sM / bear $%sM) %s"%(r["theme"],round(r["net_leveraged_flow_5d"]/1e6,1),
        round(r["bull_lev_flow_5d"]/1e6,1),round(r["bear_lev_flow_5d"]/1e6,1),r["etfs"]))
print("\nMOST BEARISH leveraged positioning:")
for r in ls.get("most_bearish_positioning",[])[:6]:
    print("  %-16s net $%sM (bull $%sM / bear $%sM) %s"%(r["theme"],round(r["net_leveraged_flow_5d"]/1e6,1),
        round(r["bull_lev_flow_5d"]/1e6,1),round(r["bear_lev_flow_5d"]/1e6,1),r["etfs"]))
print("\nPUMP SETUPS:",[(c["complex"],c["pump_probability"]) for c in d.get("pump_setups",[])])
print("PARTY OVER:",[c["complex"] for c in d.get("party_over_alerts",[])])
print("CASCADE:",[(x["symbol"],x["complex"]) for x in d.get("top_pick_cascade",[])][:8])
