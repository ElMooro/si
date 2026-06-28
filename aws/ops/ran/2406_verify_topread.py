import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# cycle-clock
print("=== cycle-clock ===")
r=lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
time.sleep(3)
for k in ["data/cycle-clock.json","cycle-clock.json"]:
    try: d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read()); break
    except Exception: d=None
cr=(d or {}).get("crypto") or {}
print("crypto block new fields:")
print("  exchange_flow_regime:",cr.get("exchange_flow_regime"),"| 30d_pctile:",cr.get("exchange_flow_30d_pctile"),"| study:",cr.get("exchange_flow_study"))
print("  cot_asset_mgr:",cr.get("cot_asset_mgr"),"("+str(cr.get("cot_asset_mgr_pctile"))+"th) | lev_funds:",cr.get("cot_lev_funds"))
print("  realized_price:",cr.get("realized_price"),"| price_vs_realized:",cr.get("price_vs_realized_pct"),"| NUPL:",cr.get("nupl"),cr.get("nupl_zone"))
syn=(d or {}).get("synthesis") or {}
print("synthesis:",syn.get("posture"),syn.get("score"),"| conviction",syn.get("conviction"))
contribs=[c.get("label") for c in (syn.get("contributors") or [])]
print("crypto contributors firing:",[c for c in contribs if "Crypto" in c])
divs=(d or {}).get("divergences") or []
print("COT divergence present:",any("COT SPLIT" in str(x) for x in divs))
# confluence
print("=== crypto-confluence ===")
r=lam.invoke(FunctionName="justhodl-crypto-confluence",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
time.sleep(2)
for k in ["data/crypto-confluence.json","crypto-confluence.json"]:
    try: c=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read()); break
    except Exception: c=None
mc=(c or {}).get("market_context") or {}
print("  regime:",mc.get("regime"),"tilt:",mc.get("tilt"))
print("  exchange_flow_regime:",mc.get("exchange_flow_regime"),"| cot_asset_mgr:",mc.get("cot_asset_mgr"))
print("  realized_price:",mc.get("realized_price"),"| price_vs_realized:",mc.get("price_vs_realized_pct"),"| nupl_zone:",mc.get("nupl_zone"))
print("DONE 2406")
