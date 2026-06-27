import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def exists(fn):
    try: lam.get_function(FunctionName=fn); return True
    except Exception: return False
print("crypto-dvol exists:", exists("justhodl-crypto-dvol"))
if exists("justhodl-crypto-dvol"):
    r=lam.invoke(FunctionName="justhodl-crypto-dvol",InvocationType="RequestResponse",Payload=b"{}")
    print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:200])
    time.sleep(2)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-dvol.json")["Body"].read())
        print("BTC:",json.dumps(d.get("btc")))
        print("ETH:",json.dumps(d.get("eth")))
        print("composite regime:",d.get("crypto_vol_regime"),"| pctile:",d.get("crypto_vol_pctile"),"| spread:",d.get("btc_eth_spread"))
        print("interp:",d.get("interpretation"))
    except Exception as e: print("read err:",str(e)[:80])
print("DONE 2362")
