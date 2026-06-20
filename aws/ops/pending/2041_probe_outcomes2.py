"""ops 2041: get real outcome-row schema (date field, signal_type values, excess) + proven names + engine dict keys."""
import json, boto3
from collections import defaultdict
s3=boto3.client("s3","us-east-1"); ddb=boto3.resource("dynamodb","us-east-1"); B="justhodl-dashboard-live"
a=json.loads(s3.get_object(Bucket=B,Key="data/engine-alpha.json")["Body"].read())
print("alpha_proven_signals:",json.dumps(a.get("alpha_proven_signals"))[:400])
print("\nengines[0] full:",json.dumps(a["engines"][0],default=str)[:600])
print("\nengines[0] keys:",sorted(a["engines"][0].keys()))
# find a proven engine dict
prov=[e for e in a["engines"] if e.get("alpha_status")=="ALPHA_PROVEN"]
if prov: print("\nproven engine dict:",json.dumps(prov[0],default=str)[:700])

print("\n"+"="*60);print("outcomes — real schema");print("="*60)
t=ddb.Table("justhodl-outcomes")
st_count=defaultdict(int); samples=[]; scanned=0; keyset=set()
kw={"Limit":500}
for _ in range(6):
    r=t.scan(**kw); items=r.get("Items",[])
    for o in items:
        st_count[str(o.get("signal_type"))]+=1
        keyset|=set(o.keys())
        if len(samples)<4 and o.get("price_at_signal") is not None: samples.append(o)
    scanned+=len(items)
    if "LastEvaluatedKey" not in r or scanned>2800: break
    kw["ExclusiveStartKey"]=r["LastEvaluatedKey"]
print("scanned:",scanned,"| union of all keys:",sorted(keyset))
print("\ntop signal_type values:",sorted(st_count.items(),key=lambda x:-x[1])[:18])
for s in samples[:3]:
    print("\n SAMPLE:",json.dumps({k:str(v)[:40] for k,v in s.items()},default=str)[:500])
print("DONE 2041")
