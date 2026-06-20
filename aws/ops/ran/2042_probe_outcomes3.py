"""ops 2042: engines-as-dict + outcomes schema (date field/excess) — final pre-build probe."""
import json, boto3
from collections import defaultdict
s3=boto3.client("s3","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1"); B="justhodl-dashboard-live"
a=json.loads(s3.get_object(Bucket=B,Key="data/engine-alpha.json")["Body"].read())
eng=a["engines"]
print("engines type:",type(eng).__name__,"n:",len(eng))
k0=list(eng.keys())[0]
print("sample engine key:",k0,"\n dict:",json.dumps(eng[k0],default=str)[:700])
print("\n proven dicts:")
for nm in a["alpha_proven_signals"]:
    e=eng.get(nm,{})
    print(" ",nm,"→ keys:",sorted(e.keys()))
    print("     net:",e.get("alpha_net") or e.get("astats_net") or {k:e.get(k) for k in e if "net" in k.lower()})
    print("     by_regime keys:",list((e.get("by_regime") or {}).keys()))
    break

print("\n"+"="*60);print("outcomes schema");print("="*60)
t=ddb.Table("justhodl-outcomes")
st_count=defaultdict(int); samples=[]; scanned=0; keyset=set()
kw={"Limit":500}
for _ in range(5):
    r=t.scan(**kw); items=r.get("Items",[])
    for o in items:
        st_count[str(o.get("signal_type"))]+=1; keyset|=set(o.keys())
        if len(samples)<4 and o.get("price_at_signal") not in (None,0,"0"): samples.append(o)
    scanned+=len(items)
    if "LastEvaluatedKey" not in r or scanned>2500: break
    kw["ExclusiveStartKey"]=r["LastEvaluatedKey"]
print("scanned:",scanned,"\nALL KEYS:",sorted(keyset))
print("\ntop signal_type:",sorted(st_count.items(),key=lambda x:-x[1])[:16])
for s in samples[:3]:
    print("\n SAMPLE:",json.dumps({k:str(v)[:32] for k,v in s.items()},default=str))
print("DONE 2042")
