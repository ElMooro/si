import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=150,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
# cycle-clock async (heavy)
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("cycle-clock async kicked")
# crypto-confluence sync
r=lam.invoke(FunctionName="justhodl-crypto-confluence",InvocationType="RequestResponse",Payload=b"{}")
print("confluence FunctionError:",r.get("FunctionError"))
time.sleep(2)
cf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-confluence.json")["Body"].read())
mc=cf.get("market_context") or {}
print("CONFLUENCE market_context: regime",mc.get("regime"),"tilt",mc.get("tilt"))
print("  vol_regime",mc.get("vol_regime"),"| skew:",mc.get("options_skew"),"| vol_term:",mc.get("vol_term"))
print("  puell",mc.get("puell"),mc.get("puell_zone"),"| hash_ribbon",mc.get("hash_ribbon"),"| carry_3m",mc.get("cash_carry_3m"),mc.get("carry_regime"))
# wait for cycle-clock
print("waiting for cycle-clock...")
time.sleep(95)
cc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
cr=cc.get("crypto") or {}
print("\nCYCLE-CLOCK crypto block new fields:")
print("  rr_25d",cr.get("rr_25d"),"| skew_read:",cr.get("skew_read"),"| vol_term_regime:",cr.get("vol_term_regime"))
print("  hash_ribbon",cr.get("hash_ribbon"),"| puell",cr.get("puell"),cr.get("puell_zone"))
print("  cash_carry_3m",cr.get("cash_carry_3m"),"| carry_regime",cr.get("carry_regime"),"| eth_funding_ann",cr.get("eth_funding_ann"))
syn=cc.get("synthesis") or {}
crypto_contribs=[c for c in (syn.get("contributors") or syn.get("drivers") or []) if "rypto" in str(c)]
print("\nsynthesis posture:",syn.get("posture"),syn.get("score"))
# find crypto contributors / divergences
divs=[d for d in (cc.get("divergences") or []) if "CRYPTO" in str(d).upper()]
print("crypto divergences:",len(divs))
for d in divs: print("   -",d[:140])
print("DONE 2393")
