import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# does the function exist (workflow create branch)?
try:
    c=lam.get_function(FunctionName="justhodl-crypto-liquidity")["Configuration"]
    print("function EXISTS | state",c.get("State"),"| timeout",c.get("Timeout"),"| mem",c.get("MemorySize"))
    for _ in range(20):
        c=lam.get_function(FunctionName="justhodl-crypto-liquidity")["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
        time.sleep(3)
    r=lam.invoke(FunctionName="justhodl-crypto-liquidity",InvocationType="RequestResponse")
    print("invoke:",r["StatusCode"],r["Payload"].read().decode()[:200])
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/crypto-liquidity.json")["Body"].read())
    print(f"\nREGIME: {d['regime']} · score {d['liquidity_score']} · {d['regime_read'][:90]}")
    ssr=d['ssr'];print(f"SSR {ssr['value']} | pctile2y {ssr['percentile_2y']} | {ssr['interpretation']}")
    print(f"F&G {d['fear_greed']['value']} ({d['fear_greed']['classification']}) | stbl dom {d['stablecoin_dominance']['live_coingecko_pct']}%")
    ss=d['event_study_ssr']['fwd90d']; sv=d['event_study_ssr']
    print(f"\nSSR BACKTEST 90d: low(≤20%ile)→{ss['low_median']}% (hit {ss['low_hit_pct']}%) vs high(≥80%ile)→{ss['high_median']}% | EDGE {ss['edge_low_minus_high_pp']}pp | n{ss['n_low']}/{ss['n_high']} | {sv['verdict']} (w{sv['weight']})")
    fs=d['event_study_fear_greed']['fwd90d']; fv=d['event_study_fear_greed']
    print(f"F&G BACKTEST 90d: fear(≤25)→{fs['low_median']}% (hit {fs['low_hit_pct']}%) vs greed(≥75)→{fs['high_median']}% | EDGE {fs['edge_low_minus_high_pp']}pp | n{fs['n_low']}/{fs['n_high']} | {fv['verdict']} (w{fv['weight']})")
    print("directional_read:",d.get("directional_read"),"| top_picks:",d.get("top_picks"))
    print("CREATED_OK")
except lam.exceptions.ResourceNotFoundException:
    print("FUNCTION_NOT_CREATED — needs boto3 create")
print("DONE 2083")
