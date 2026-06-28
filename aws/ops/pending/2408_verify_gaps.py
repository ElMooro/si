import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def rd(k):
    for kk in ([k,"data/"+k] if not k.startswith("data/") else [k]):
        try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=kk)["Body"].read())
        except Exception: pass
    return {}
# cycle-clock
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}");time.sleep(3)
cc=rd("cycle-clock.json").get("crypto") or {}
print("cycle-clock crypto: stablecoin_status:",cc.get("stablecoin_peg_status"),"| gauge:",cc.get("stablecoin_peg_gauge"),"| coinbase_premium:",cc.get("coinbase_premium_pct"))
# confluence
lam.invoke(FunctionName="justhodl-crypto-confluence",InvocationType="RequestResponse",Payload=b"{}");time.sleep(2)
mc=rd("crypto-confluence.json").get("market_context") or {}
print("confluence: regime",mc.get("regime"),"tilt",mc.get("tilt"),"| stablecoin:",mc.get("stablecoin_peg_status"),"| cb_prem:",mc.get("coinbase_premium_pct"))
# morning-intelligence (heavy; just confirm no error + crypto flow line present)
print("invoking morning-intelligence (may take ~60-120s)...")
r=lam.invoke(FunctionName="justhodl-morning-intelligence",InvocationType="RequestResponse",Payload=b"{}")
print("MI FunctionError:",r.get("FunctionError"))
if r.get("FunctionError"):
    print("  payload:",r["Payload"].read().decode()[:300])
else:
    time.sleep(3)
    mi=rd("morning-intelligence.json")
    blob=json.dumps(mi)
    print("  MI ran OK | CRYPTO_FLOWS line present:","CRYPTO_FLOWS" in blob,"| exchange_flow field present:","crypto_exchange_flow_regime" in blob)
print("DONE 2408")
