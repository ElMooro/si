import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
print("=== crypto-intel: implied_vol ===")
r=lam.invoke(FunctionName="justhodl-crypto-intel",InvocationType="RequestResponse",Payload=b"{}")
print("  FunctionError:",r.get("FunctionError"))
time.sleep(2)
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="crypto-intel.json")["Body"].read())
    iv=d.get("implied_vol") or {}
    print("  implied_vol:",json.dumps({k:iv.get(k) for k in ('btc_dvol','btc_dvol_pctile','btc_dvol_regime','btc_dvol_trend','eth_dvol','regime')}))
    es=(iv.get("event_study") or {})
    print("  event_study verdict:",es.get("verdict"),"standing:",es.get("standing"))
except Exception as e: print("  err:",str(e)[:80])
print("\n=== eurodollar-plumbing: stablecoin offshore-$ metric ===")
r=lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse",Payload=b"{}")
print("  FunctionError:",r.get("FunctionError"))
time.sleep(3)
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/eurodollar-plumbing.json")["Body"].read())
    # find the metric across layers
    found=None
    for lname,metrics in (d.get("layers") or {}).items():
        if isinstance(metrics,list):
            for m in metrics:
                if isinstance(m,dict) and m.get("id")=="stablecoin_offshore_usd": found=(lname,m)
    print("  plumbing_health:",d.get("plumbing_health"),"| verdict:",d.get("verdict") or d.get("regime"))
    if found: print("  stablecoin metric in layer '%s':"%found[0], json.dumps({k:found[1].get(k) for k in ('label','value','unit','status')}))
    else: print("  stablecoin_offshore_usd NOT FOUND in layers:",list((d.get('layers') or {}).keys()))
except Exception as e: print("  err:",str(e)[:80])
print("DONE 2371")
