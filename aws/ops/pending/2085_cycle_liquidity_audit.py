import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
# list candidate feeds
paginator=s3.get_paginator("list_objects_v2")
keys=[]
for pg in paginator.paginate(Bucket=B,Prefix="data/"):
    for o in pg.get("Contents",[]):
        k=o["Key"]
        if any(w in k.lower() for w in ["regime","crisis","plumbing","liquidity","cycle","macro","risk","funding","stress","credit","squeeze"]):
            keys.append(k)
print("=== candidate cycle/liquidity feeds ===")
for k in sorted(keys): print(" ",k)

def grab(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}

def show(k, fields):
    d=grab(k)
    if "_err" in d: print(f"\n[{k}] MISSING/{d['_err']}"); return
    print(f"\n[{k}] gen={str(d.get('generated_at',''))[:16]}")
    for f in fields:
        cur=d
        for part in f.split("."):
            cur=cur.get(part) if isinstance(cur,dict) else None
        print(f"   {f} = {cur}")

show("data/regime-map.json", ["regime","regime_label","risk_on_score","classification","summary"])
show("data/risk-regime.json", ["risk_regime_score","regime","posture"])
show("data/crypto-liquidity.json", ["regime","liquidity_score","ssr.percentile_2y","fear_greed.value","forecast_supported"])
show("data/regime.json", ["regime","label","macro_regime","summary"])
show("data/eurodollar-plumbing.json", ["plumbing_health","regime","summary"])
show("data/crisis-composite.json", ["score","level","regime","summary"])
show("data/crisis.json", ["score","level","composite","regime"])
show("data/treasury-noise.json", ["treasury_stress","funding_stress","regime"])
print("DONE 2085")
