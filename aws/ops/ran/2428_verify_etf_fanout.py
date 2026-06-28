import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); logs=boto3.client("logs","us-east-1")
def rd(k):
    for kk in ([k,"data/"+k] if not k.startswith("data/") else [k]):
        try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=kk)["Body"].read())
        except Exception: pass
    return {}
# crypto-intel
lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="Event",Payload=b"{}")
print("crypto-intel async; waiting 75s..."); time.sleep(75)
ci=rd("crypto-intel.json"); ef=ci.get("etf_flows") or {}
print("crypto-intel v%s etf_flows: BTC %s ($%.1fB/30d, %sth) ES=%s | ETH %s | AUM $%.0fB"%(
    ci.get("version"),ef.get("btc_regime"),(ef.get("btc_flow_30d_usd") or 0)/1e9,ef.get("btc_30d_pctile"),
    ef.get("btc_event_study"),ef.get("eth_regime"),(ef.get("btc_aum_usd") or 0)/1e9))
# cycle-clock
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}");time.sleep(3)
cc=rd("cycle-clock.json"); cr=cc.get("crypto") or {}; syn=cc.get("synthesis") or {}
print("cycle-clock crypto: etf_btc_regime",cr.get("etf_flow_btc_regime"),"30d $%.1fB"%((cr.get("etf_flow_btc_30d_usd") or 0)/1e9),"| etf_eth",cr.get("etf_flow_eth_regime"))
print("  synthesis:",syn.get("posture"),syn.get("score"),"| ETF contributors:",[c.get("label") for c in (syn.get("contributors") or []) if "ETF" in c.get("label","")])
# confluence
lam.invoke(FunctionName="justhodl-crypto-confluence",InvocationType="RequestResponse",Payload=b"{}");time.sleep(2)
mc=rd("crypto-confluence.json").get("market_context") or {}
print("confluence: regime",mc.get("regime"),"tilt",mc.get("tilt"),"| etf_btc",mc.get("etf_flow_btc_regime"))
# signal-logger
lam.invoke(FunctionName="justhodl-signal-logger",InvocationType="RequestResponse",Payload=b"{}");time.sleep(4)
try:
    grp="/aws/lambda/justhodl-signal-logger"
    st=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
    ev=logs.get_log_events(logGroupName=grp,logStreamName=st[0]["logStreamName"],limit=60,startFromHead=False)["events"]
    hits=[e["message"].strip() for e in ev if "[LOG]" in e["message"] and "crypto_etf_flow" in e["message"]]
    print("ledger:",hits[-1] if hits else "crypto_etf_flow not emitted (check pctile/threshold)")
except Exception as e: print("logs err:",str(e)[:60])
print("DONE 2428")
