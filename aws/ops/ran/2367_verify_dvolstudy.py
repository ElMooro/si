import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-crypto-dvol",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
time.sleep(2)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-dvol.json")["Body"].read())
es=d.get("event_study_dvol") or {}
print("standing:",es.get("standing"),"| verdict:",es.get("verdict"),"| n_days:",es.get("n_days"))
print("hypothesis:",es.get("hypothesis"))
for h in ("fwd30d","fwd90d","fwd180d"):
    v=es.get(h) or {}
    print(f"  {h}: LOW-pctile(complacency) mean {v.get('low_pctile_mean')}% (n{v.get('n_low')}, hit {v.get('low_hit_pct')}%) | HIGH-pctile(fear) mean {v.get('high_pctile_mean')}% (n{v.get('n_high')}, hit {v.get('high_hit_pct')}%) | edge H-L {v.get('edge_high_minus_low_pp')}pp")
print("DONE 2367")
