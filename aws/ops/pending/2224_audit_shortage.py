import boto3, json, re
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1")
now=datetime.now(timezone.utc)
def age_h(ts):
    try:
        t=datetime.fromisoformat(str(ts).replace("Z","+00:00"))
        if not t.tzinfo: t=t.replace(tzinfo=timezone.utc)
        return round((now-t).total_seconds()/3600,1)
    except: return None
def probe(f):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        ga=d.get("generated_at") or d.get("as_of") or d.get("asof")
        print(f"\n{f}: age={age_h(ga)}h  keys={list(d.keys())[:11]}")
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                it=v[0]; idk=[x for x in it if x in ("ticker","symbol","theme","signal","name","company")]
                if idk: print(f"    '{k}' n={len(v)} id={idk} fields={[x for x in it.keys()][:8]}")
            if isinstance(v,dict) and v and all(isinstance(x,(int,float,dict)) for x in list(v.values())[:3]) and len(v)<=30:
                print(f"    dict '{k}' subkeys={list(v.keys())[:8]}")
    except Exception as e: print(f"\n{f}: ERR {str(e)[:50]}")
for f in ["bottleneck-boom","supply-inflection-scanner","chokepoint","commodity-curves",
          "supply-chain-graph","ai-infra-stack","narrative-vs-tape"]:
    probe(f)
print("\nDONE 2224")
