import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
def probe(k):
    d=g(k)
    if "_err" in d: print(f"\n[{k}] {d['_err']}"); return
    print(f"\n[{k}] top keys: {list(d.keys())[:18]}")
    for ak in ("top_picks","items","actions","names","results","data","upgrades","downgrades","revisions","tickers"):
        v=d.get(ak)
        if isinstance(v,list) and v:
            print(f"   .{ak}[0] keys: {list(v[0].keys()) if isinstance(v[0],dict) else type(v[0])}")
            print(f"   .{ak}[0] sample: {json.dumps(v[0],default=str)[:300]}")
            break
for k in ["data/analyst-actions.json","data/estimate-revisions.json","data/earnings-tracker.json"]:
    probe(k)
# also check sector ETF mapping availability — is there a ticker->sector map anywhere?
for k in ["data/sector-emergence.json","data/equity-universe.json","data/ticker-sectors.json"]:
    d=g(k)
    print(f"\n[{k}] {'MISSING' if '_err' in d else 'present keys: '+str(list(d.keys())[:12])}")
print("DONE 2094")
